//! Model Switcher — coordinates wake/sleep transitions between models.
//!
//! The switcher tracks which model is active, manages in-flight request
//! counting, drains requests before switching, and delegates all lifecycle
//! operations to the [`HookRunner`].

use crate::config::OrchestrationConfig;
use crate::cost::SwitchCostTracker;
use crate::hooks::HookRunner;
use crate::policy::{PolicyContext, PolicyDecision, ScheduleContext, SwitchContext, SwitchPolicy};
use crate::types::{RequestPriority, SwitchError, SwitcherState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};

/// Signal sent through the oneshot channel when a model becomes active.
///
/// The receiver must call [`ReadySignal::settle`] after acquiring its
/// in-flight guard. `notify_pending` blocks on these settle signals so
/// that the switch lock is held until all notified requests are actively
/// being processed — preventing the next switch from draining the model
/// before any request has started.
pub(crate) struct ReadySignal {
    settle_tx: mpsc::Sender<()>,
}

impl ReadySignal {
    pub(crate) async fn settle(self) {
        let _ = self.settle_tx.send(()).await;
    }
}

/// A pending request waiting for a model to become active
struct PendingRequest {
    id: u64,
    priority: RequestPriority,
    queued_at: Instant,
    ready_tx: oneshot::Sender<Result<ReadySignal, SwitchError>>,
}

/// Per-model state tracking
struct ModelState {
    in_flight: AtomicUsize,
    pending: Mutex<Vec<PendingRequest>>,
    in_flight_changed: Arc<Notify>,
    /// Set to `true` while draining in-flight requests before sleep.
    /// When true, `acquire_in_flight` will refuse new guards so that
    /// no requests sneak in between drain completion and the actual sleep call.
    draining: AtomicBool,
}

impl Default for ModelState {
    fn default() -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            pending: Mutex::new(Vec::new()),
            in_flight_changed: Arc::new(Notify::new()),
            draining: AtomicBool::new(false),
        }
    }
}

struct SwitcherInner {
    hooks: Arc<HookRunner>,
    policy: Box<dyn SwitchPolicy>,
    state: RwLock<SwitcherState>,
    model_states: HashMap<String, Arc<ModelState>>,
    switch_lock: Mutex<()>,
    /// When the currently active model was activated (for cooldown enforcement)
    activated_at: RwLock<Option<Instant>>,
    /// When the last switch failure occurred (for backoff)
    last_switch_failure: RwLock<Option<Instant>>,
    /// Empirical switch cost tracking (EMA of observed durations)
    cost_tracker: SwitchCostTracker,
    orchestration: OrchestrationConfig,
    management: Arc<StdMutex<ManagementState>>,
    next_request_id: AtomicU64,
    scheduler_notify: Arc<Notify>,
    persist_lock: Mutex<()>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistentState {
    #[serde(default = "persistent_state_version")]
    version: u8,
    pinned_model: Option<String>,
}

fn persistent_state_version() -> u8 {
    1
}

#[derive(Debug, Clone, Default)]
struct ManagementState {
    pinned_model: Option<String>,
    lease_model: Option<String>,
    lease_expires_at: Option<SystemTime>,
    last_error: Option<String>,
    phase: Option<String>,
    last_switch: Option<LastSwitchStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStatus {
    pub interactive: usize,
    pub background: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicModelStatus {
    pub id: String,
    pub topology: Option<String>,
    pub context_length: Option<u64>,
    pub quantization: Option<String>,
    pub speculative: Option<String>,
    pub description: Option<String>,
    pub impact: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LastSwitchStatus {
    pub from: Option<String>,
    pub to: String,
    pub result: String,
    pub duration_seconds: f64,
    pub at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControllerStatus {
    pub manual: bool,
    pub phase: Option<String>,
    pub state: String,
    pub active_model: Option<String>,
    pub target_model: Option<String>,
    pub pinned: bool,
    pub pinned_model: Option<String>,
    pub lease_expires_at: Option<u64>,
    pub lease_remaining_seconds: u64,
    pub queues: HashMap<String, QueueStatus>,
    pub in_flight: HashMap<String, usize>,
    pub last_switch: Option<LastSwitchStatus>,
    pub models: Vec<PublicModelStatus>,
    pub last_error: Option<String>,
}

/// The model switcher coordinates wake/sleep transitions.
pub struct ModelSwitcher {
    inner: Arc<SwitcherInner>,
}

impl Clone for ModelSwitcher {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ModelSwitcher {
    pub fn new(hooks: Arc<HookRunner>, policy: Box<dyn SwitchPolicy>) -> Self {
        Self::new_with_config(hooks, policy, OrchestrationConfig::default())
    }

    pub fn new_with_config(
        hooks: Arc<HookRunner>,
        policy: Box<dyn SwitchPolicy>,
        orchestration: OrchestrationConfig,
    ) -> Self {
        let model_states: HashMap<String, Arc<ModelState>> = hooks
            .registered_models()
            .into_iter()
            .map(|model| (model, Arc::new(ModelState::default())))
            .collect();

        Self {
            inner: Arc::new(SwitcherInner {
                hooks,
                policy,
                state: RwLock::new(SwitcherState::Idle),
                model_states,
                switch_lock: Mutex::new(()),
                activated_at: RwLock::new(None),
                last_switch_failure: RwLock::new(None),
                cost_tracker: SwitchCostTracker::new(0.3),
                orchestration,
                management: Arc::new(StdMutex::new(ManagementState::default())),
                next_request_id: AtomicU64::new(1),
                scheduler_notify: Arc::new(Notify::new()),
                persist_lock: Mutex::new(()),
            }),
        }
    }

    /// Reconcile configured backends and persisted pin state at startup.
    /// More than one healthy backend is unsafe because llmux cannot know
    /// which service owns the shared accelerator resources.
    pub async fn reconcile_startup(&self) -> Result<(), SwitchError> {
        let mut alive = Vec::new();
        for model in self.registered_models() {
            match self.inner.hooks.run_alive(&model).await {
                Ok(true) => alive.push(model),
                Ok(false) => {}
                Err(error) => {
                    metrics::counter!("llmux_reconciliation_total", "result" => "hook_error")
                        .increment(1);
                    return Err(SwitchError::Internal(error.to_string()));
                }
            }
        }
        if alive.len() > 1 {
            metrics::counter!("llmux_reconciliation_total", "result" => "multiple_active")
                .increment(1);
            return Err(SwitchError::Internal(format!(
                "startup reconciliation found multiple active models: {}",
                alive.join(", ")
            )));
        }

        if let Some(model) = alive.first() {
            *self.inner.state.write().await = SwitcherState::Active {
                model: model.clone(),
            };
            *self.inner.activated_at.write().await = Some(Instant::now());
        }

        if let Some(path) = &self.inner.orchestration.state_path {
            match tokio::fs::read(path).await {
                Ok(bytes) => {
                    let persisted: PersistentState =
                        serde_json::from_slice(&bytes).map_err(|e| {
                            SwitchError::Internal(format!(
                                "invalid state file {}: {e}",
                                path.display()
                            ))
                        })?;
                    if persisted.version != 1 {
                        return Err(SwitchError::Internal(format!(
                            "unsupported state version {}",
                            persisted.version
                        )));
                    }
                    if let Some(pin) = &persisted.pinned_model
                        && !self.is_registered(pin)
                    {
                        return Err(SwitchError::Internal(format!(
                            "state file pins unknown model {pin}"
                        )));
                    }
                    self.inner
                        .management
                        .lock()
                        .expect("management lock")
                        .pinned_model = persisted.pinned_model;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(SwitchError::Internal(format!(
                        "failed reading state file {}: {e}",
                        path.display()
                    )));
                }
            }
        }

        self.update_state_metrics().await;
        let result = if alive.is_empty() { "idle" } else { "active" };
        metrics::counter!("llmux_reconciliation_total", "result" => result).increment(1);
        Ok(())
    }

    pub async fn restore_pinned_model(&self) -> Result<(), SwitchError> {
        let pin = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .pinned_model
            .clone();
        if let Some(model) = pin {
            self.force_switch(&model).await?;
        }
        Ok(())
    }

    pub async fn controller_status(&self) -> ControllerStatus {
        let switcher = self.state().await;
        let (state, active_model, target_model) = match &switcher {
            SwitcherState::Active { model } => ("active", Some(model.clone()), None),
            SwitcherState::Switching { from, to } => ("switching", from.clone(), Some(to.clone())),
            SwitcherState::Idle => ("idle", None, None),
        };
        let management = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .clone();
        let now = SystemTime::now();
        let remaining_seconds = management
            .lease_expires_at
            .and_then(|deadline| deadline.duration_since(now).ok())
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let expires_at_unix = management
            .lease_expires_at
            .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs());

        let mut models = Vec::new();
        let mut queues = HashMap::new();
        let mut in_flight = HashMap::new();
        for id in self.registered_models() {
            let model_state = &self.inner.model_states[&id];
            let queue = model_state.pending.lock().await;
            queues.insert(
                id.clone(),
                QueueStatus {
                    interactive: queue
                        .iter()
                        .filter(|p| p.priority == RequestPriority::Interactive)
                        .count(),
                    background: queue
                        .iter()
                        .filter(|p| p.priority == RequestPriority::Background)
                        .count(),
                },
            );
            in_flight.insert(id.clone(), self.in_flight_count(&id));
            let metadata = &self
                .inner
                .hooks
                .model_config(&id)
                .expect("registered model config")
                .metadata;
            models.push(PublicModelStatus {
                topology: metadata.topology.clone(),
                context_length: metadata.context_length,
                quantization: metadata.quantization.clone(),
                speculative: metadata.speculative_decoding.clone(),
                description: metadata.description.clone(),
                impact: metadata.impact.clone(),
                id,
            });
        }
        models.sort_by(|a, b| a.id.cmp(&b.id));

        ControllerStatus {
            manual: self.inner.orchestration.manual,
            phase: management.phase,
            state: state.to_string(),
            active_model,
            target_model,
            pinned: management.pinned_model.is_some(),
            pinned_model: management.pinned_model,
            lease_expires_at: expires_at_unix,
            lease_remaining_seconds: remaining_seconds,
            queues,
            in_flight,
            last_switch: management.last_switch,
            models,
            last_error: management.last_error,
        }
    }

    /// Explicit control-plane switch. If a pin exists, the pin follows the
    /// explicit selection; otherwise the switch remains unpinned.
    pub async fn control_switch(&self, model: &str) -> Result<(), SwitchError> {
        if self.inner.orchestration.manual {
            return self.manual_transition(Some(model)).await;
        }
        self.require_registered(model)?;
        let previous_pin = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .pinned_model
            .clone();
        if previous_pin.is_some() {
            self.set_pin(Some(model.to_string())).await?;
        }
        if let Err(error) = self.force_switch(model).await {
            if previous_pin.is_some() {
                let _ = self.set_pin(previous_pin).await;
            }
            return Err(error);
        }
        Ok(())
    }

    pub async fn pin_model(&self, model: &str) -> Result<(), SwitchError> {
        if self.inner.orchestration.manual {
            return self.manual_transition(Some(model)).await;
        }
        self.require_registered(model)?;
        let previous = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .pinned_model
            .clone();
        self.set_pin(Some(model.to_string())).await?;
        if let Err(error) = self.force_switch(model).await {
            let _ = self.set_pin(previous).await;
            return Err(error);
        }
        Ok(())
    }

    pub async fn unpin_model(&self) -> Result<(), SwitchError> {
        if self.inner.orchestration.manual {
            return Err(SwitchError::NotReady(
                "Manual mode has no unpin action; use Load or Stop".into(),
            ));
        }
        self.set_pin(None).await?;
        self.inner.scheduler_notify.notify_one();
        Ok(())
    }

    fn require_registered(&self, model: &str) -> Result<(), SwitchError> {
        if self.is_registered(model) {
            Ok(())
        } else {
            Err(SwitchError::ModelNotFound(model.to_string()))
        }
    }

    async fn set_pin(&self, pin: Option<String>) -> Result<(), SwitchError> {
        let previous = {
            let mut state = self.inner.management.lock().expect("management lock");
            std::mem::replace(&mut state.pinned_model, pin.clone())
        };
        if let Err(error) = self.persist_state().await {
            self.inner
                .management
                .lock()
                .expect("management lock")
                .pinned_model = previous;
            return Err(error);
        }
        self.update_state_metrics().await;
        Ok(())
    }

    async fn persist_state(&self) -> Result<(), SwitchError> {
        let Some(path) = self.inner.orchestration.state_path.as_ref() else {
            return Ok(());
        };
        let _guard = self.inner.persist_lock.lock().await;
        let state = PersistentState {
            version: 1,
            pinned_model: self
                .inner
                .management
                .lock()
                .expect("management lock")
                .pinned_model
                .clone(),
        };
        let bytes = serde_json::to_vec_pretty(&state)
            .map_err(|e| SwitchError::Internal(format!("failed serializing state: {e}")))?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                SwitchError::Internal(format!("failed creating {}: {e}", parent.display()))
            })?;
        }
        let temporary = temporary_state_path(path);
        tokio::fs::write(&temporary, bytes).await.map_err(|e| {
            SwitchError::Internal(format!("failed writing {}: {e}", temporary.display()))
        })?;
        tokio::fs::rename(&temporary, path).await.map_err(|e| {
            SwitchError::Internal(format!("failed replacing {}: {e}", path.display()))
        })?;
        Ok(())
    }

    fn set_last_error(&self, error: String) {
        self.inner
            .management
            .lock()
            .expect("management lock")
            .last_error = Some(error);
    }

    fn clear_last_error(&self) {
        self.inner
            .management
            .lock()
            .expect("management lock")
            .last_error = None;
    }

    fn record_last_switch(
        &self,
        from: Option<String>,
        to: &str,
        result: &str,
        duration: Duration,
        error: Option<String>,
    ) {
        self.inner
            .management
            .lock()
            .expect("management lock")
            .last_switch = Some(LastSwitchStatus {
            from,
            to: to.to_string(),
            result: result.to_string(),
            duration_seconds: duration.as_secs_f64(),
            at: unix_now(),
            error,
        });
    }

    fn set_active_metrics(&self, active: Option<&str>) {
        for model in self.registered_models() {
            metrics::gauge!("llmux_active_model_info", "model" => model.clone())
                .set(f64::from(active == Some(model.as_str())));
        }
    }

    fn set_lifecycle_metric(&self, current: &str) {
        for state in ["idle", "active", "switching"] {
            metrics::gauge!("llmux_lifecycle_state_info", "state" => state)
                .set(f64::from(state == current));
        }
    }

    async fn update_state_metrics(&self) {
        let status = self.controller_status().await;
        self.set_active_metrics(status.active_model.as_deref());
        for model in self.registered_models() {
            metrics::gauge!("llmux_pin_info", "model" => model.clone()).set(f64::from(
                status.pinned_model.as_deref() == Some(model.as_str()),
            ));
        }
        metrics::gauge!("llmux_lease_remaining_seconds").set(status.lease_remaining_seconds as f64);
        self.set_lifecycle_metric(&status.state);
    }

    /// Runs lease accounting and wakes queued requests when a pin or lease no
    /// longer blocks them. The notification avoids waiting for the next tick
    /// after an explicit unpin or an interactive response completion.
    pub fn spawn_orchestration_scheduler(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    _ = self.inner.scheduler_notify.notified() => {},
                }
                self.update_state_metrics().await;
                if let Some(target) = self.next_eligible_target().await {
                    self.do_switch(&target, false).await;
                }
            }
        })
    }

    async fn next_eligible_target(&self) -> Option<String> {
        let management = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .clone();
        let active = self.active_model().await;
        if let Some(pin) = management.pinned_model {
            return (active.as_deref() != Some(pin.as_str())).then_some(pin);
        }

        let mut interactive: Option<(Instant, String)> = None;
        let mut background: Option<(Instant, String)> = None;
        for (model, state) in &self.inner.model_states {
            let mut queue = state.pending.lock().await;
            queue.retain(|pending| !pending.ready_tx.is_closed());
            self.record_queue_metrics(model, &queue);
            for pending in queue.iter() {
                let slot = match pending.priority {
                    RequestPriority::Interactive => &mut interactive,
                    RequestPriority::Background => &mut background,
                };
                if slot
                    .as_ref()
                    .is_none_or(|(oldest, _)| pending.queued_at < *oldest)
                {
                    *slot = Some((pending.queued_at, model.clone()));
                }
            }
        }
        if let Some((_, model)) = interactive {
            return Some(model);
        }

        let lease_active = management
            .lease_expires_at
            .is_some_and(|deadline| deadline > SystemTime::now())
            && management.lease_model.as_deref() == active.as_deref();
        if lease_active {
            None
        } else {
            background.map(|(_, model)| model)
        }
    }

    pub async fn state(&self) -> SwitcherState {
        self.inner.state.read().await.clone()
    }

    pub async fn active_model(&self) -> Option<String> {
        match &*self.inner.state.read().await {
            SwitcherState::Active { model } => Some(model.clone()),
            _ => None,
        }
    }

    pub fn is_active_alias(&self, name: &str) -> bool {
        self.inner.orchestration.active_alias.as_deref() == Some(name)
    }

    fn set_phase(&self, phase: &str) {
        self.inner.management.lock().expect("management lock").phase = Some(phase.into());
    }

    /// Explicit, serialized lifecycle for manual mode. Failure never starts
    /// another model automatically. A failed stop retains ownership so the
    /// next operator action must retry cleanup before allocating anything.
    pub async fn manual_transition(&self, target: Option<&str>) -> Result<(), SwitchError> {
        if !self.inner.orchestration.manual {
            return Err(SwitchError::NotReady("Stop requires manual mode".into()));
        }
        if let Some(model) = target {
            self.require_registered(model)?;
        }
        let _lock = self
            .inner
            .switch_lock
            .try_lock()
            .map_err(|_| SwitchError::NotReady("A model change is already running".into()))?;
        let started = Instant::now();
        let previous = self.active_model().await;
        if let Some(model) = target {
            if previous.as_deref() == Some(model)
                && !self.inner.model_states[model]
                    .draining
                    .load(Ordering::SeqCst)
                && self.inner.hooks.run_alive(model).await.unwrap_or(false)
            {
                self.set_phase("ready");
                self.clear_last_error();
                return Ok(());
            }
        }
        // Persist intent before doing anything destructive. Manual startup
        // only observes an existing model; it never wakes this saved choice.
        self.set_pin(target.map(str::to_owned)).await?;
        if let Some(model) = &previous {
            self.inner.model_states[model]
                .draining
                .store(true, Ordering::SeqCst);
        }
        *self.inner.state.write().await = SwitcherState::Switching {
            from: previous.clone(),
            to: target.unwrap_or("").into(),
        };
        self.set_phase("draining");
        if let Some(model) = &previous {
            let deadline =
                Instant::now() + Duration::from_secs(self.inner.orchestration.drain_timeout_secs);
            while self.in_flight_count(model) > 0 {
                if Instant::now() >= deadline {
                    self.inner.model_states[model]
                        .draining
                        .store(false, Ordering::SeqCst);
                    *self.inner.state.write().await = SwitcherState::Active {
                        model: model.clone(),
                    };
                    self.set_phase("ready");
                    self.set_last_error(
                        "Requests did not finish before the drain deadline; model left running"
                            .into(),
                    );
                    return Err(SwitchError::Timeout);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            self.set_phase("stopping");
            if let Err(error) = self.inner.hooks.run_sleep(model).await {
                *self.inner.state.write().await = SwitcherState::Active {
                    model: model.clone(),
                };
                self.set_phase("failed");
                let detail = format!("Stop failed; no new model was started: {error}");
                self.set_last_error(detail.clone());
                self.record_last_switch(
                    previous.clone(),
                    target.unwrap_or(""),
                    "stop_failure",
                    started.elapsed(),
                    Some(detail.clone()),
                );
                return Err(SwitchError::HookFailed {
                    model: model.clone(),
                    detail,
                });
            }
        }
        if let Some(model) = target {
            self.set_phase("loading");
            if let Err(error) = self.inner.hooks.run_wake(model).await {
                let cleanup = self.inner.hooks.run_sleep(model).await;
                // A failed cleanup must remain a barrier to the next load.
                *self.inner.state.write().await = if cleanup.is_err() {
                    self.inner.model_states[model]
                        .draining
                        .store(true, Ordering::SeqCst);
                    SwitcherState::Active {
                        model: model.into(),
                    }
                } else {
                    SwitcherState::Idle
                };
                self.set_phase("failed");
                let detail =
                    format!("Load failed: {error}; cleanup={cleanup:?}. No fallback was started.");
                self.set_last_error(detail.clone());
                self.record_last_switch(
                    previous,
                    model,
                    "wake_failure",
                    started.elapsed(),
                    Some(detail.clone()),
                );
                return Err(SwitchError::HookFailed {
                    model: model.into(),
                    detail,
                });
            }
            self.inner.model_states[model]
                .draining
                .store(false, Ordering::SeqCst);
            *self.inner.state.write().await = SwitcherState::Active {
                model: model.into(),
            };
            *self.inner.activated_at.write().await = Some(Instant::now());
            self.set_phase("ready");
            self.set_active_metrics(Some(model));
        } else {
            *self.inner.state.write().await = SwitcherState::Idle;
            self.set_phase("stopped");
            self.set_active_metrics(None);
        }
        self.clear_last_error();
        self.record_last_switch(
            previous,
            target.unwrap_or(""),
            "success",
            started.elapsed(),
            None,
        );
        self.update_state_metrics().await;
        Ok(())
    }

    pub fn registered_models(&self) -> Vec<String> {
        self.inner.model_states.keys().cloned().collect()
    }

    pub fn hooks(&self) -> &Arc<HookRunner> {
        &self.inner.hooks
    }

    pub fn is_registered(&self, model: &str) -> bool {
        self.inner.model_states.contains_key(model)
    }

    pub fn model_port(&self, model: &str) -> Option<u16> {
        self.inner.hooks.model_port(model)
    }

    pub fn in_flight_count(&self, model: &str) -> usize {
        self.inner
            .model_states
            .get(model)
            .map(|s| s.in_flight.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Estimated cost of switching between two models, based on observed durations.
    /// `from` is `None` for cold starts from Idle.
    pub fn estimated_switch_cost(&self, from: Option<&str>, to: &str) -> Option<Duration> {
        self.inner.cost_tracker.estimate(from, to)
    }

    /// Force a switch to the given model, bypassing policy.
    pub async fn force_switch(&self, model: &str) -> Result<(), SwitchError> {
        if !self.is_registered(model) {
            return Err(SwitchError::ModelNotFound(model.to_string()));
        }

        // If already active, nothing to do
        {
            let state = self.inner.state.read().await;
            if let SwitcherState::Active { model: active } = &*state
                && active == model
            {
                return Ok(());
            }
        }

        self.do_switch(model, true).await;

        let state = self.inner.state.read().await;
        match &*state {
            SwitcherState::Active { model: active } if active == model => Ok(()),
            _ => Err(SwitchError::NotReady(model.to_string())),
        }
    }

    /// Ensure a model is ready for requests.
    ///
    /// Returns `Ok(None)` immediately if the model is already active (fast
    /// path). Otherwise queues the request, triggers a switch if needed, and
    /// waits up to the policy timeout. Returns `Ok(Some(signal))` — the
    /// caller **must** call [`ReadySignal::settle`] after acquiring its
    /// in-flight guard so that `notify_pending` knows the request is actively
    /// being processed.
    pub(crate) async fn ensure_model_ready(
        &self,
        model: &str,
        priority: RequestPriority,
    ) -> Result<Option<ReadySignal>, SwitchError> {
        let model_state = self
            .inner
            .model_states
            .get(model)
            .ok_or_else(|| SwitchError::ModelNotFound(model.to_string()))?;

        // Fast path: model is already active
        {
            let state = self.inner.state.read().await;
            if let SwitcherState::Active { model: active } = &*state
                && active == model
            {
                if self.inner.orchestration.manual && model_state.draining.load(Ordering::SeqCst) {
                    return Err(SwitchError::NotReady(format!(
                        "{model}; stopping or failed"
                    )));
                }
                trace!(model = %model, "Model already active");
                return Ok(None);
            }
        }

        if self.inner.orchestration.manual {
            return Err(SwitchError::NotReady(format!(
                "{model}; use Spark Controls to load it"
            )));
        }

        // Queue the request
        let (ready_tx, ready_rx) = oneshot::channel();
        let id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let pending = PendingRequest {
            id,
            priority,
            queued_at: Instant::now(),
            ready_tx,
        };

        {
            let mut queue = model_state.pending.lock().await;
            queue.push(pending);
            let depth = queue.len();
            debug!(model = %model, queue_depth = depth, "Request queued");
            self.record_queue_metrics(model, &queue);
        }

        let mut ticket = QueueTicket::new(self.clone(), model.to_string(), id, priority);
        self.maybe_trigger_switch(model, priority).await;

        let timeout = match priority {
            RequestPriority::Interactive => self.inner.policy.request_timeout(),
            RequestPriority::Background => Some(Duration::from_secs(
                self.inner.orchestration.background_max_wait_secs,
            )),
        };

        let result = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, ready_rx).await {
                Ok(Ok(result)) => result.map(Some),
                Ok(Err(_)) => Err(SwitchError::Internal("channel closed".to_string())),
                Err(_) => {
                    warn!(
                        event = "request_timeout",
                        model = %model,
                        timeout_secs = timeout.as_secs_f64(),
                        "Request timed out waiting for model"
                    );
                    if priority == RequestPriority::Background {
                        metrics::counter!(
                            "llmux_background_wait_timeouts_total",
                            "model" => model.to_string()
                        )
                        .increment(1);
                    }
                    Err(match priority {
                        RequestPriority::Interactive => SwitchError::Timeout,
                        RequestPriority::Background => SwitchError::BackgroundTimeout,
                    })
                }
            },
            None => match ready_rx.await {
                Ok(result) => result.map(Some),
                Err(_) => Err(SwitchError::Internal("channel closed".to_string())),
            },
        };
        self.remove_pending(model, id).await;
        ticket.disarm();
        result
    }

    /// Acquire an in-flight guard.
    ///
    /// Returns `None` if the model is not registered or if it is currently
    /// draining. Uses increment-then-check to close the TOCTOU window.
    pub fn acquire_in_flight(
        &self,
        model: &str,
        priority: RequestPriority,
    ) -> Option<InFlightGuard> {
        let model_state = self.inner.model_states.get(model)?;

        let new_count = model_state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;

        if model_state.draining.load(Ordering::SeqCst) {
            model_state.in_flight.fetch_sub(1, Ordering::SeqCst);
            model_state.in_flight_changed.notify_waiters();
            return None;
        }

        metrics::gauge!("llmux_model_in_flight", "model" => model.to_string())
            .set(new_count as f64);

        Some(InFlightGuard {
            model_state: Arc::clone(model_state),
            model: model.to_string(),
            priority,
            management: Arc::clone(&self.inner.management),
            lease_duration: Duration::from_secs(self.inner.orchestration.interactive_lease_secs),
            scheduler_notify: Arc::clone(&self.inner.scheduler_notify),
        })
    }

    async fn remove_pending(&self, model: &str, id: u64) {
        let Some(state) = self.inner.model_states.get(model) else {
            return;
        };
        let mut queue = state.pending.lock().await;
        queue.retain(|pending| pending.id != id);
        self.record_queue_metrics(model, &queue);
    }

    fn record_queue_metrics(&self, model: &str, queue: &[PendingRequest]) {
        for priority in [RequestPriority::Interactive, RequestPriority::Background] {
            metrics::gauge!(
                "llmux_request_queue_depth",
                "model" => model.to_string(),
                "priority" => priority.as_str()
            )
            .set(queue.iter().filter(|p| p.priority == priority).count() as f64);
        }
    }

    /// Get queue depths for every registered model.
    pub async fn queue_depths(&self) -> HashMap<String, usize> {
        let mut depths = HashMap::new();
        for (model, state) in &self.inner.model_states {
            let queue = state.pending.lock().await;
            depths.insert(model.clone(), queue.len());
        }
        depths
    }

    /// Spawn a background scheduler task if the policy requests one.
    pub fn spawn_scheduler(self) -> Option<tokio::task::JoinHandle<()>> {
        let interval = self.inner.policy.scheduler_interval()?;

        info!(
            interval_ms = interval.as_millis(),
            "Spawning background scheduler"
        );

        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await;
                let ctx = self.build_schedule_context().await;
                if let Some(target) = self.inner.policy.schedule_tick(&ctx) {
                    debug!(target = %target, "Scheduler: triggering switch");
                    self.do_switch(&target, false).await;
                }
            }
        }))
    }

    // -----------------------------------------------------------------------
    // Private
    // -----------------------------------------------------------------------

    async fn maybe_trigger_switch(&self, target_model: &str, priority: RequestPriority) {
        let model_state = match self.inner.model_states.get(target_model) {
            Some(s) => s,
            None => return,
        };

        let ctx = {
            let state = self.inner.state.read().await;
            let queue = model_state.pending.lock().await;

            let oldest_waiting = queue
                .first()
                .map(|p| p.queued_at.elapsed())
                .unwrap_or(Duration::ZERO);

            let (active_model, active_in_flight) = match &*state {
                SwitcherState::Active { model } => {
                    (Some(model.clone()), self.in_flight_count(model))
                }
                _ => (None, 0),
            };

            let active_duration = self
                .inner
                .activated_at
                .read()
                .await
                .map(|t| t.elapsed())
                .unwrap_or(Duration::ZERO);

            let estimated_switch_cost = self
                .inner
                .cost_tracker
                .estimate(active_model.as_deref(), target_model);

            PolicyContext {
                target_model: target_model.to_string(),
                active_model,
                target_queue_depth: queue.len(),
                oldest_waiting,
                active_in_flight,
                active_duration,
                estimated_switch_cost,
            }
        };

        // Already switching to this model?
        {
            let state = self.inner.state.read().await;
            if let SwitcherState::Switching { to, .. } = &*state
                && to == target_model
            {
                return;
            }
        }

        let management = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .clone();
        if let Some(pin) = management.pinned_model
            && pin != target_model
        {
            debug!(model = %target_model, pinned_model = %pin, "Request waits behind model pin");
            return;
        }

        if priority == RequestPriority::Background {
            let active = ctx.active_model.as_deref();
            let lease_blocks = management.lease_model.as_deref() == active
                && management
                    .lease_expires_at
                    .is_some_and(|deadline| deadline > SystemTime::now());
            if active.is_some() && active != Some(target_model) && lease_blocks {
                debug!(model = %target_model, "Background request waits behind interactive lease");
                self.inner.scheduler_notify.notify_one();
                return;
            }
        }

        let decision = self.inner.policy.on_pending_request(&ctx).await;

        match decision {
            PolicyDecision::SwitchNow => {
                debug!(model = %target_model, "Policy: switch now");
                let switcher = self.clone();
                let target = target_model.to_string();
                tokio::spawn(async move {
                    switcher.do_switch(&target, false).await;
                });
            }
            PolicyDecision::Defer(future) => {
                debug!(model = %target_model, "Policy: defer");
                let switcher = self.clone();
                let target = target_model.to_string();
                tokio::spawn(async move {
                    future.await;
                    switcher.do_switch(&target, false).await;
                });
            }
            PolicyDecision::Skip => {
                trace!(model = %target_model, "Policy: skip");
            }
        }
    }

    async fn do_switch(&self, target_model: &str, force: bool) {
        let _guard = self.inner.switch_lock.lock().await;
        let switch_start = Instant::now();

        if !force && !self.request_switch_allowed(target_model).await {
            debug!(model = %target_model, "Queued switch is no longer eligible");
            return;
        }

        // Backoff after recent failure
        {
            let last_failure = self.inner.last_switch_failure.read().await;
            if let Some(failed_at) = *last_failure {
                let backoff = Duration::from_secs(2);
                let elapsed = failed_at.elapsed();
                if elapsed < backoff {
                    let remaining = backoff - elapsed;
                    info!(remaining = ?remaining, "Backing off after recent switch failure");
                    drop(last_failure);
                    tokio::time::sleep(remaining).await;
                }
            }
        }

        // Double-check state
        {
            let state = self.inner.state.read().await;
            match &*state {
                SwitcherState::Active { model } if model == target_model => {
                    self.notify_pending(target_model, Ok(())).await;
                    return;
                }
                SwitcherState::Switching { to, .. } if to == target_model => {
                    return;
                }
                _ => {}
            }
        }

        // Skip if there are no pending requests for the target model.
        // This happens when a stale do_switch task grabs the lock after
        // the target model's requests were already served by an earlier
        // activation.
        if !force && let Some(target_state) = self.inner.model_states.get(target_model) {
            let queue = target_state.pending.lock().await;
            if queue.is_empty() {
                debug!(
                    model = %target_model,
                    "No pending requests, skipping stale switch"
                );
                return;
            }
        }

        let from_model = {
            let state = self.inner.state.read().await;
            match &*state {
                SwitcherState::Active { model } => Some(model.clone()),
                _ => None,
            }
        };

        let from_label = from_model.as_deref().unwrap_or("idle").to_string();

        // Record how long the outgoing model was active
        if from_model.is_some() {
            let active_dur = self
                .inner
                .activated_at
                .read()
                .await
                .map(|t| t.elapsed())
                .unwrap_or(Duration::ZERO);
            metrics::histogram!(
                "llmux_model_active_duration_seconds",
                "model" => from_label.clone()
            )
            .record(active_dur.as_secs_f64());
        }

        // Update state to Switching
        {
            let mut state = self.inner.state.write().await;
            *state = SwitcherState::Switching {
                from: from_model.clone(),
                to: target_model.to_string(),
            };
        }
        self.set_lifecycle_metric("switching");

        info!(
            event = "switch_started",
            from = %from_label,
            to = %target_model,
            "Starting model switch"
        );

        // Phase 1: Cooldown — ensure the old model has been active long enough
        if from_model.is_some() {
            let min_active = self.inner.policy.min_active_duration();
            let activated_at = *self.inner.activated_at.read().await;
            if let Some(activated) = activated_at {
                let elapsed = activated.elapsed();
                if elapsed < min_active {
                    let remaining = min_active - elapsed;
                    info!(remaining = ?remaining, "Waiting for cooldown");
                    tokio::time::sleep(remaining).await;
                }
            }
        }

        // Phase 2: Drain — set draining flag and wait for in-flight to complete
        let drain_start = Instant::now();

        if let Some(ref from) = from_model
            && let Some(from_state) = self.inner.model_states.get(from)
        {
            from_state.draining.store(true, Ordering::SeqCst);
        }

        if let Some(ref from) = from_model
            && let Some(from_state) = self.inner.model_states.get(from)
        {
            let in_flight_changed = Arc::clone(&from_state.in_flight_changed);
            let from_state_clone = Arc::clone(from_state);

            let mut switch_ctx = SwitchContext::new(
                from_model.clone(),
                target_model.to_string(),
                in_flight_changed,
                Box::new(move || from_state_clone.in_flight.load(Ordering::SeqCst)),
            );

            self.inner.policy.prepare_switch(&mut switch_ctx).await;
        }

        if from_model.is_some() {
            metrics::histogram!(
                "llmux_switch_drain_duration_seconds",
                "from" => from_label.clone(),
                "to" => target_model.to_string()
            )
            .record(drain_start.elapsed().as_secs_f64());
        }

        // Phase 3: Sleep old model via hook
        if let Some(ref from) = from_model {
            debug!(model = %from, "Running sleep hook");
            if let Err(e) = self.inner.hooks.run_sleep(from).await {
                error!(
                    event = "sleep_hook_failed",
                    model = %from,
                    to = %target_model,
                    error = %e,
                    "Sleep hook failed; refusing to wake target"
                );

                if let Some(from_state) = self.inner.model_states.get(from) {
                    from_state.draining.store(false, Ordering::SeqCst);
                }
                *self.inner.state.write().await = SwitcherState::Active {
                    model: from.clone(),
                };
                *self.inner.last_switch_failure.write().await = Some(Instant::now());
                let error = SwitchError::HookFailed {
                    model: from.clone(),
                    detail: format!("sleep failed; target {target_model} was not started: {e}"),
                };
                self.set_last_error(error.to_string());
                self.record_last_switch(
                    from_model.clone(),
                    target_model,
                    "sleep_failure",
                    switch_start.elapsed(),
                    Some(error.to_string()),
                );
                self.set_lifecycle_metric("active");
                metrics::counter!(
                    "llmux_switch_total",
                    "from" => from_label,
                    "to" => target_model.to_string(),
                    "result" => "sleep_failure"
                )
                .increment(1);
                self.notify_pending(target_model, Err(error)).await;
                return;
            }
        }

        // Clear draining flag
        if let Some(ref from) = from_model
            && let Some(from_state) = self.inner.model_states.get(from)
        {
            from_state.draining.store(false, Ordering::SeqCst);
        }

        // Phase 4: Wake new model via hook
        debug!(model = %target_model, "Running wake hook");
        match self.inner.hooks.run_wake(target_model).await {
            Ok(()) => {
                let total_dur = switch_start.elapsed();

                {
                    let mut state = self.inner.state.write().await;
                    *state = SwitcherState::Active {
                        model: target_model.to_string(),
                    };
                }
                *self.inner.activated_at.write().await = Some(Instant::now());
                *self.inner.last_switch_failure.write().await = None;
                self.clear_last_error();
                self.set_active_metrics(Some(target_model));
                self.set_lifecycle_metric("active");
                self.record_last_switch(
                    from_model.clone(),
                    target_model,
                    "success",
                    total_dur,
                    None,
                );

                self.inner
                    .cost_tracker
                    .record(from_model.as_deref(), target_model, total_dur);

                // Structured log event for timeline reconstruction
                info!(
                    event = "model_activated",
                    model = %target_model,
                    from = %from_label,
                    duration_secs = total_dur.as_secs_f64(),
                    "Model is now active"
                );

                // Metrics
                metrics::counter!(
                    "llmux_switch_total",
                    "from" => from_label.clone(),
                    "to" => target_model.to_string(),
                    "result" => "success"
                )
                .increment(1);
                metrics::histogram!(
                    "llmux_switch_duration_seconds",
                    "from" => from_label.clone(),
                    "to" => target_model.to_string()
                )
                .record(total_dur.as_secs_f64());

                if let Some(ema) = self
                    .inner
                    .cost_tracker
                    .estimate(from_model.as_deref(), target_model)
                {
                    metrics::gauge!(
                        "llmux_switch_cost_ema_seconds",
                        "from" => from_label,
                        "to" => target_model.to_string()
                    )
                    .set(ema.as_secs_f64());
                }

                let from_str = from_model.as_deref().unwrap_or("");
                self.inner
                    .policy
                    .on_switch_complete(from_str, target_model, total_dur);

                self.notify_pending(target_model, Ok(())).await;
            }
            Err(e) => {
                // Structured log event for timeline reconstruction
                info!(
                    event = "switch_failed",
                    model = %target_model,
                    from = %from_label,
                    error = %e,
                    "Switch failed, returning to idle"
                );

                metrics::counter!(
                    "llmux_switch_total",
                    "from" => from_label,
                    "to" => target_model.to_string(),
                    "result" => "failure"
                )
                .increment(1);

                // Clean up the partially-woken target, then attempt to restore
                // the previous model. Both hooks are required to be idempotent.
                let cleanup_error = self.inner.hooks.run_sleep(target_model).await.err();
                let rollback = match from_model.as_deref() {
                    Some(previous) => self.inner.hooks.run_wake(previous).await.map(|_| previous),
                    None => Err(crate::hooks::HookError::ModelNotFound("idle".into())),
                };

                *self.inner.last_switch_failure.write().await = Some(Instant::now());
                let detail = match rollback {
                    Ok(previous) => {
                        *self.inner.state.write().await = SwitcherState::Active {
                            model: previous.to_string(),
                        };
                        *self.inner.activated_at.write().await = Some(Instant::now());
                        self.set_active_metrics(Some(previous));
                        format!("wake failed: {e}; previous model restored")
                    }
                    Err(rollback_error) => {
                        *self.inner.state.write().await = SwitcherState::Idle;
                        self.set_active_metrics(None);
                        format!(
                            "wake failed: {e}; cleanup={cleanup_error:?}; rollback failed: {rollback_error}"
                        )
                    }
                };
                self.set_last_error(detail.clone());
                self.record_last_switch(
                    from_model.clone(),
                    target_model,
                    "wake_failure",
                    switch_start.elapsed(),
                    Some(detail.clone()),
                );
                self.set_lifecycle_metric(if self.active_model().await.is_some() {
                    "active"
                } else {
                    "idle"
                });

                self.notify_pending(
                    target_model,
                    Err(SwitchError::HookFailed {
                        model: target_model.to_string(),
                        detail,
                    }),
                )
                .await;
            }
        }
    }

    async fn build_schedule_context(&self) -> ScheduleContext {
        let (active_model, active_in_flight) = match &*self.inner.state.read().await {
            SwitcherState::Active { model } => (Some(model.clone()), self.in_flight_count(model)),
            _ => (None, 0),
        };

        let active_duration = self
            .inner
            .activated_at
            .read()
            .await
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO);

        let queue_depths = self.queue_depths().await;

        let switch_costs = self
            .inner
            .cost_tracker
            .estimates_from(active_model.as_deref());

        ScheduleContext {
            active_model,
            active_duration,
            queue_depths,
            active_in_flight,
            switch_costs,
        }
    }

    async fn request_switch_allowed(&self, target_model: &str) -> bool {
        let management = self
            .inner
            .management
            .lock()
            .expect("management lock")
            .clone();
        if let Some(pin) = management.pinned_model {
            return pin == target_model;
        }

        let Some(target) = self.inner.model_states.get(target_model) else {
            return false;
        };
        let queue = target.pending.lock().await;
        let has_interactive = queue
            .iter()
            .any(|pending| pending.priority == RequestPriority::Interactive);
        let is_empty = queue.is_empty();
        drop(queue);
        if has_interactive {
            return true;
        }
        if is_empty {
            return false;
        }
        let active = self.active_model().await;
        !(management.lease_model.as_deref() == active.as_deref()
            && management
                .lease_expires_at
                .is_some_and(|deadline| deadline > SystemTime::now()))
    }

    /// Notify pending requests and — on success — wait for them to settle.
    ///
    /// When the result is `Ok`, each notified request receives a
    /// [`ReadySignal`]. The request must call `settle()` after acquiring
    /// its in-flight guard. This method blocks until all delivered signals
    /// have settled (or their receivers are dropped), ensuring the switch
    /// lock is held while requests transition from "notified" to "in-flight."
    ///
    /// When the result is `Err`, requests are notified of the failure and
    /// no settle wait is needed.
    async fn notify_pending(&self, model: &str, result: Result<(), SwitchError>) {
        let Some(model_state) = self.inner.model_states.get(model) else {
            return;
        };

        let mut queue = model_state.pending.lock().await;
        let count = queue.len();
        if count == 0 {
            return;
        }

        let mut delivered = 0;

        // For successful activations, create a settle channel so we can
        // wait for notified requests to acquire their in-flight guards.
        let settle_tx = if result.is_ok() {
            // +1 capacity: one per request, non-blocking sends
            Some(mpsc::channel::<()>(count))
        } else {
            None
        };

        for pending in queue.drain(..) {
            let r = match (&result, &settle_tx) {
                (Ok(()), Some((tx, _))) => Ok(ReadySignal {
                    settle_tx: tx.clone(),
                }),
                (Err(e), _) => Err(e.clone()),
                _ => unreachable!(),
            };
            if pending.ready_tx.send(r).is_ok() {
                delivered += 1;
            }
        }

        self.record_queue_metrics(model, &queue);
        drop(queue); // release pending lock before the settle wait

        if count > 0 {
            let expired = count - delivered;
            if expired > 0 {
                warn!(model = %model, count, delivered, expired,
                    "Notified pending requests ({expired} already timed out)");
            } else {
                debug!(model = %model, count, "Notified pending requests");
            }
        }

        // Wait for all delivered requests to acquire in-flight guards.
        // Each request calls ReadySignal::settle() after acquiring its
        // guard, which sends () on the channel. If a request drops its
        // signal without settling (e.g. cancelled), its sender clone is
        // dropped; once all senders are gone the channel closes and recv
        // returns None.
        if let Some((tx, mut rx)) = settle_tx {
            // Drop the original sender — only the clones sent to requests
            // should keep the channel alive.
            drop(tx);

            if delivered > 0 {
                let settle_wait = async {
                    for _ in 0..delivered {
                        if rx.recv().await.is_none() {
                            break; // all senders dropped
                        }
                    }
                };

                if tokio::time::timeout(Duration::from_secs(5), settle_wait)
                    .await
                    .is_err()
                {
                    warn!(
                        model = %model,
                        delivered,
                        "Settle timeout — proceeding with switch lock release"
                    );
                }
            }
        }
    }
}

/// Guard that tracks in-flight requests. When dropped, decrements the count
/// and notifies the drain waiter.
pub struct InFlightGuard {
    model_state: Arc<ModelState>,
    model: String,
    priority: RequestPriority,
    management: Arc<StdMutex<ManagementState>>,
    lease_duration: Duration,
    scheduler_notify: Arc<Notify>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let prev = self.model_state.in_flight.fetch_sub(1, Ordering::SeqCst);
        metrics::gauge!("llmux_model_in_flight", "model" => self.model.clone())
            .set((prev - 1) as f64);
        if prev == 1 {
            self.model_state.in_flight_changed.notify_waiters();
        }
        if self.priority == RequestPriority::Interactive {
            let mut management = self.management.lock().expect("management lock");
            management.lease_model = Some(self.model.clone());
            management.lease_expires_at = Some(SystemTime::now() + self.lease_duration);
            metrics::gauge!("llmux_lease_remaining_seconds").set(self.lease_duration.as_secs_f64());
        }
        self.scheduler_notify.notify_one();
    }
}

struct QueueTicket {
    switcher: ModelSwitcher,
    model: String,
    id: u64,
    priority: RequestPriority,
    armed: bool,
}

impl QueueTicket {
    fn new(switcher: ModelSwitcher, model: String, id: u64, priority: RequestPriority) -> Self {
        Self {
            switcher,
            model,
            id,
            priority,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for QueueTicket {
    fn drop(&mut self) {
        if self.armed {
            metrics::counter!(
                "llmux_pending_request_cancellations_total",
                "model" => self.model.clone(),
                "priority" => self.priority.as_str()
            )
            .increment(1);
            let switcher = self.switcher.clone();
            let model = self.model.clone();
            let id = self.id;
            tokio::spawn(async move { switcher.remove_pending(&model, id).await });
        }
    }
}

fn temporary_state_path(path: &Path) -> PathBuf {
    let mut temporary = path.to_path_buf();
    let suffix = path.extension().and_then(|s| s.to_str()).unwrap_or("json");
    temporary.set_extension(format!("{suffix}.tmp"));
    temporary
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelConfig;
    use crate::policy::FifoPolicy;

    fn make_test_hooks() -> Arc<HookRunner> {
        let mut configs = HashMap::new();
        configs.insert(
            "model-a".to_string(),
            ModelConfig {
                host: "127.0.0.1".into(),
                port: 8001,
                wake: "true".to_string(),
                sleep: "true".to_string(),
                alive: "true".to_string(),
                metadata: Default::default(),
            },
        );
        configs.insert(
            "model-b".to_string(),
            ModelConfig {
                host: "127.0.0.1".into(),
                port: 8002,
                wake: "true".to_string(),
                sleep: "true".to_string(),
                alive: "true".to_string(),
                metadata: Default::default(),
            },
        );
        Arc::new(HookRunner::new(configs))
    }

    #[test]
    fn test_switcher_creation() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        assert!(switcher.is_registered("model-a"));
        assert!(switcher.is_registered("model-b"));
        assert!(!switcher.is_registered("model-c"));
    }

    #[tokio::test]
    async fn test_in_flight_tracking() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        assert_eq!(switcher.in_flight_count("model-a"), 0);

        {
            let _guard = switcher.acquire_in_flight("model-a", RequestPriority::Interactive);
            assert_eq!(switcher.in_flight_count("model-a"), 1);
        }

        assert_eq!(switcher.in_flight_count("model-a"), 0);
    }

    #[test]
    fn test_acquire_in_flight_rejected_while_draining() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        let guard = switcher.acquire_in_flight("model-a", RequestPriority::Interactive);
        assert!(guard.is_some());
        assert_eq!(switcher.in_flight_count("model-a"), 1);
        drop(guard);

        // Set draining flag
        let model_state = switcher.inner.model_states.get("model-a").unwrap();
        model_state.draining.store(true, Ordering::SeqCst);

        let guard = switcher.acquire_in_flight("model-a", RequestPriority::Interactive);
        assert!(guard.is_none());
        assert_eq!(switcher.in_flight_count("model-a"), 0);

        model_state.draining.store(false, Ordering::SeqCst);

        let guard = switcher.acquire_in_flight("model-a", RequestPriority::Interactive);
        assert!(guard.is_some());
        assert_eq!(switcher.in_flight_count("model-a"), 1);
        drop(guard);
    }

    #[tokio::test]
    async fn test_model_port() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        assert_eq!(switcher.model_port("model-a"), Some(8001));
        assert_eq!(switcher.model_port("model-b"), Some(8002));
        assert_eq!(switcher.model_port("model-c"), None);
    }

    #[tokio::test]
    async fn test_force_switch_unknown_model() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        let result = switcher.force_switch("nonexistent").await;
        assert!(matches!(result, Err(SwitchError::ModelNotFound(_))));
    }

    #[tokio::test]
    async fn test_force_switch_already_active() {
        let hooks = make_test_hooks();
        let policy = Box::new(FifoPolicy::default());
        let switcher = ModelSwitcher::new(hooks, policy);

        {
            let mut state = switcher.inner.state.write().await;
            *state = SwitcherState::Active {
                model: "model-a".to_string(),
            };
        }

        let result = switcher.force_switch("model-a").await;
        assert!(result.is_ok());
    }
}
