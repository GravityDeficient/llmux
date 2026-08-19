//! End-to-end tests for llmux.
//!
//! Spins up mock backends (simple axum echo servers), writes tiny hook scripts,
//! and drives requests through the full stack: middleware → switcher → hooks → proxy.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use http_body_util::BodyExt;
use llmux::{AuthConfig, Config, ModelConfig, OrchestrationConfig, PolicyConfig};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tower::ServiceExt;

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_state_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "llmux-{name}-{}-{}.json",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Inline hook scripts for testing. Hooks are executed via `sh -c`.
struct MockHooks {
    wake: String,
    sleep: String,
    alive: String,
}

impl MockHooks {
    fn new(wake_ms: u64, sleep_ms: u64) -> Self {
        Self {
            wake: format!("sleep {}", wake_ms as f64 / 1000.0),
            sleep: format!("sleep {}", sleep_ms as f64 / 1000.0),
            alive: "false".to_string(),
        }
    }
}

/// Spawn a mock backend that echoes the model name and a counter.
async fn spawn_mock_backend(port: u16) -> (SocketAddr, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |Json(body): Json<Value>| {
            let c = counter_clone.fetch_add(1, Ordering::SeqCst);
            let model = body
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            async move {
                Json(json!({
                    "model": model,
                    "request_number": c,
                    "choices": [{"message": {"content": "hello"}}]
                }))
            }
        }),
    );

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Wait for the server to be ready
    tokio::time::sleep(Duration::from_millis(10)).await;

    (addr, counter)
}

/// Build a test config with two models.
fn test_config(
    model_a_port: u16,
    model_b_port: u16,
    hooks_a: &MockHooks,
    hooks_b: &MockHooks,
) -> Config {
    let mut models = HashMap::new();
    models.insert(
        "model-a".to_string(),
        ModelConfig {
            port: model_a_port,
            wake: hooks_a.wake.clone(),
            sleep: hooks_a.sleep.clone(),
            alive: hooks_a.alive.clone(),
            metadata: Default::default(),
        },
    );
    models.insert(
        "model-b".to_string(),
        ModelConfig {
            port: model_b_port,
            wake: hooks_b.wake.clone(),
            sleep: hooks_b.sleep.clone(),
            alive: hooks_b.alive.clone(),
            metadata: Default::default(),
        },
    );

    Config {
        models,
        policy: PolicyConfig {
            request_timeout_secs: Some(30),
            drain_before_switch: true,
            min_active_secs: 0,
        },
        port: 0,
        bind_address: "127.0.0.1".to_string(),
        auth: AuthConfig::default(),
        orchestration: OrchestrationConfig::default(),
    }
}

/// Send a chat completion request through the app and return the response body.
async fn chat_request(app: &Router, model: &str) -> (StatusCode, Value) {
    chat_request_with(app, model, None, None).await
}

async fn chat_request_with(
    app: &Router,
    model: &str,
    priority: Option<&str>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let body = json!({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}]
    });

    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("Content-Type", "application/json");
    if let Some(priority) = priority {
        builder = builder.header("X-LLMux-Priority", priority);
    }
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    let req = builder
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body_bytes)
        .unwrap_or(json!({"raw": String::from_utf8_lossy(&body_bytes).to_string()}));

    (status, json)
}

async fn control_request(
    app: &Router,
    method: &str,
    path: &str,
    model: Option<&str>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    if model.is_some() {
        builder = builder.header("Content-Type", "application/json");
    }
    let body = model
        .map(|model| Body::from(json!({"model": model}).to_string()))
        .unwrap_or_else(Body::empty);
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
    (status, value)
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// Basic: request for model-a goes to model-a's backend.
#[tokio::test]
async fn test_single_model_request() {
    let hooks_a = MockHooks::new(0, 0); // instant wake/sleep
    let hooks_b = MockHooks::new(0, 0);

    let (addr_a, counter_a) = spawn_mock_backend(0).await;
    let (addr_b, _counter_b) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _switcher) = llmux::build_app(config).await.unwrap();

    let (status, body) = chat_request(&app, "model-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["model"], "model-a");
    assert_eq!(counter_a.load(Ordering::SeqCst), 1);
}

/// Two sequential requests to different models triggers a switch.
#[tokio::test]
async fn test_model_switch() {
    let hooks_a = MockHooks::new(10, 10); // 10ms wake/sleep
    let hooks_b = MockHooks::new(10, 10);

    let (addr_a, counter_a) = spawn_mock_backend(0).await;
    let (addr_b, counter_b) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _switcher) = llmux::build_app(config).await.unwrap();

    // First request: model-a (cold start)
    let (status, body) = chat_request(&app, "model-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["model"], "model-a");

    // Second request: model-b (switch from a → b)
    let (status, body) = chat_request(&app, "model-b").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["model"], "model-b");

    assert_eq!(counter_a.load(Ordering::SeqCst), 1);
    assert_eq!(counter_b.load(Ordering::SeqCst), 1);
}

/// Multiple requests to the same model don't trigger a switch.
#[tokio::test]
async fn test_same_model_no_switch() {
    let hooks_a = MockHooks::new(10, 10);
    let hooks_b = MockHooks::new(10, 10);

    let (addr_a, counter_a) = spawn_mock_backend(0).await;
    let (addr_b, counter_b) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _switcher) = llmux::build_app(config).await.unwrap();

    for _ in 0..5 {
        let (status, body) = chat_request(&app, "model-a").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["model"], "model-a");
    }

    assert_eq!(counter_a.load(Ordering::SeqCst), 5);
    assert_eq!(counter_b.load(Ordering::SeqCst), 0);
}

/// Unknown model returns 404.
#[tokio::test]
async fn test_unknown_model() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);

    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _) = llmux::build_app(config).await.unwrap();

    let (status, body) = chat_request(&app, "nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not found")
    );
}

/// Switch cost is real wall-clock time from the hook scripts.
#[tokio::test]
async fn test_switch_timing() {
    let hooks_a = MockHooks::new(100, 50); // 100ms wake, 50ms sleep
    let hooks_b = MockHooks::new(100, 50);

    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _switcher) = llmux::build_app(config).await.unwrap();

    // First request: cold start wake for model-a (~100ms)
    let t0 = Instant::now();
    let (status, _) = chat_request(&app, "model-a").await;
    let cold_start = t0.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(
        cold_start >= Duration::from_millis(80),
        "cold start took {cold_start:?}"
    );

    // Second request: switch a→b (sleep a ~50ms + wake b ~100ms = ~150ms)
    let t1 = Instant::now();
    let (status, _) = chat_request(&app, "model-b").await;
    let switch_time = t1.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(
        switch_time >= Duration::from_millis(120),
        "switch took {switch_time:?}, expected >= 120ms (sleep + wake)"
    );
}

/// Concurrent requests for the same model all get served.
#[tokio::test]
async fn test_concurrent_same_model() {
    let hooks_a = MockHooks::new(50, 10);
    let hooks_b = MockHooks::new(50, 10);

    let (addr_a, counter_a) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _) = llmux::build_app(config).await.unwrap();

    // Send 10 concurrent requests for model-a
    let mut handles = Vec::new();
    for _ in 0..10 {
        let app = app.clone();
        handles.push(tokio::spawn(
            async move { chat_request(&app, "model-a").await },
        ));
    }

    for handle in handles {
        let (status, body) = handle.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["model"], "model-a");
    }

    assert_eq!(counter_a.load(Ordering::SeqCst), 10);
}

/// Concurrent requests for different models: all eventually served via FIFO switching.
#[tokio::test]
async fn test_concurrent_different_models() {
    let hooks_a = MockHooks::new(50, 10);
    let hooks_b = MockHooks::new(50, 10);

    let (addr_a, counter_a) = spawn_mock_backend(0).await;
    let (addr_b, counter_b) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _) = llmux::build_app(config).await.unwrap();

    // Send requests for both models concurrently
    let mut handles = Vec::new();
    for i in 0..6 {
        let app = app.clone();
        let model = if i % 2 == 0 { "model-a" } else { "model-b" };
        handles.push(tokio::spawn(async move { chat_request(&app, model).await }));
    }

    let mut statuses = Vec::new();
    for handle in handles {
        let (status, _) = handle.await.unwrap();
        statuses.push(status);
    }

    // All should succeed (FIFO will switch back and forth)
    assert!(
        statuses.iter().all(|s| *s == StatusCode::OK),
        "Some requests failed: {statuses:?}"
    );

    let total = counter_a.load(Ordering::SeqCst) + counter_b.load(Ordering::SeqCst);
    assert_eq!(total, 6);
}

/// GET /v1/models returns the list of configured models.
#[tokio::test]
async fn test_list_models() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);

    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, _) = llmux::build_app(config).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["object"], "list");

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);

    // Models should be sorted by id
    let ids: Vec<&str> = data.iter().map(|m| m["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["model-a", "model-b"]);

    for model in data {
        assert_eq!(model["object"], "model");
        assert_eq!(model["owned_by"], "llmux");
    }
}

/// Switch cost tracker records empirical costs after switches.
#[tokio::test]
async fn test_switch_cost_tracking() {
    let hooks_a = MockHooks::new(50, 20); // 50ms wake, 20ms sleep
    let hooks_b = MockHooks::new(80, 20); // 80ms wake, 20ms sleep

    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;

    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, switcher) = llmux::build_app(config).await.unwrap();

    // No costs recorded yet
    assert!(switcher.estimated_switch_cost(None, "model-a").is_none());
    assert!(switcher.estimated_switch_cost(None, "model-b").is_none());

    // Cold start model-a
    let (status, _) = chat_request(&app, "model-a").await;
    assert_eq!(status, StatusCode::OK);

    // Cold start cost for model-a should be recorded
    let cold_a = switcher.estimated_switch_cost(None, "model-a");
    assert!(cold_a.is_some(), "cold start cost for model-a not recorded");
    assert!(
        cold_a.unwrap() >= Duration::from_millis(30),
        "cold start cost {cold_a:?} too low"
    );

    // Switch a → b
    let (status, _) = chat_request(&app, "model-b").await;
    assert_eq!(status, StatusCode::OK);

    // a→b cost should be recorded (sleep a + wake b ≈ 20+80 = 100ms)
    let a_to_b = switcher.estimated_switch_cost(Some("model-a"), "model-b");
    assert!(a_to_b.is_some(), "a→b switch cost not recorded");
    assert!(
        a_to_b.unwrap() >= Duration::from_millis(70),
        "a→b cost {a_to_b:?} too low"
    );

    // b→a not yet observed
    assert!(
        switcher
            .estimated_switch_cost(Some("model-b"), "model-a")
            .is_none()
    );

    // Switch b → a
    let (status, _) = chat_request(&app, "model-a").await;
    assert_eq!(status, StatusCode::OK);

    // Now b→a should be recorded (sleep b + wake a ≈ 20+50 = 70ms)
    let b_to_a = switcher.estimated_switch_cost(Some("model-b"), "model-a");
    assert!(b_to_a.is_some(), "b→a switch cost not recorded");

    // Directional: a→b should cost more than b→a (80ms wake vs 50ms wake)
    assert!(
        a_to_b.unwrap() > b_to_a.unwrap(),
        "expected a→b ({a_to_b:?}) > b→a ({b_to_a:?}) due to asymmetric wake times"
    );
}

#[tokio::test]
async fn inference_auth_runs_before_lifecycle_switching() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let mut config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    config.auth.inference_bearer_token = Some("inference-secret".into());
    let (app, switcher) = llmux::build_app(config).await.unwrap();

    let (status, _) = chat_request(&app, "model-a").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(switcher.active_model().await, None);

    let (status, _) = chat_request_with(&app, "model-a", None, Some("inference-secret")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(switcher.active_model().await.as_deref(), Some("model-a"));
}

#[tokio::test]
async fn control_auth_pin_switch_and_unpin() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let mut config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    config.auth.control_bearer_token = Some("control-secret".into());
    let (app, _) = llmux::build_app(config).await.unwrap();

    let (status, _) = control_request(&app, "POST", "/control/v1/pin", Some("model-a"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, body) = control_request(
        &app,
        "POST",
        "/control/v1/pin",
        Some("model-a"),
        Some("control-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pinned_model"], "model-a");

    let (status, body) = control_request(
        &app,
        "POST",
        "/control/v1/switch",
        Some("model-b"),
        Some("control-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["active_model"], "model-b");
    assert_eq!(body["pinned_model"], "model-b");

    let (status, body) = control_request(
        &app,
        "DELETE",
        "/control/v1/pin",
        None,
        Some("control-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["pinned_model"].is_null());
}

#[tokio::test]
async fn cancelled_control_request_does_not_cancel_lifecycle() {
    let hooks_a = MockHooks::new(250, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, switcher) = llmux::build_app(config).await.unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/control/v1/pin")
        .header("Content-Type", "application/json")
        .body(Body::from(json!({"model": "model-a"}).to_string()))
        .unwrap();
    let request_task = tokio::spawn(async move { app.oneshot(request).await });

    tokio::time::sleep(Duration::from_millis(40)).await;
    request_task.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let status = switcher.controller_status().await;
    assert_eq!(status.state, "active");
    assert_eq!(status.active_model.as_deref(), Some("model-a"));
    assert_eq!(status.pinned_model.as_deref(), Some("model-a"));
}

#[tokio::test]
async fn background_waits_for_interactive_lease_but_interactive_overrides() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let mut config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    config.orchestration.interactive_lease_secs = 1;
    config.orchestration.background_max_wait_secs = 5;
    let (app, _) = llmux::build_app(config).await.unwrap();

    assert_eq!(chat_request(&app, "model-a").await.0, StatusCode::OK);
    let background_app = app.clone();
    let background = tokio::spawn(async move {
        chat_request_with(&background_app, "model-b", Some("background"), None).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !background.is_finished(),
        "background request ignored active lease"
    );
    let (status, _) = background.await.unwrap();
    assert_eq!(status, StatusCode::OK);

    // A fresh interactive completion renews model-b's lease, but another
    // interactive request is allowed to preempt it immediately.
    let started = Instant::now();
    assert_eq!(chat_request(&app, "model-a").await.0, StatusCode::OK);
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[tokio::test]
async fn background_timeout_is_retryable_and_queue_is_cleaned() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let mut config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    config.orchestration.interactive_lease_secs = 60;
    config.orchestration.background_max_wait_secs = 0;
    let (app, switcher) = llmux::build_app(config).await.unwrap();
    assert_eq!(chat_request(&app, "model-a").await.0, StatusCode::OK);

    let body = json!({"model": "model-b", "messages": []});
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("X-LLMux-Priority", "background")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").unwrap(), "30");
    let status = switcher.controller_status().await;
    assert_eq!(status.queues["model-b"].background, 0);
}

#[tokio::test]
async fn sleep_failure_is_fail_closed() {
    let mut hooks_a = MockHooks::new(0, 0);
    hooks_a.sleep = "false".into();
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, switcher) = llmux::build_app(config).await.unwrap();
    assert_eq!(chat_request(&app, "model-a").await.0, StatusCode::OK);
    assert_eq!(
        chat_request(&app, "model-b").await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(switcher.active_model().await.as_deref(), Some("model-a"));
}

#[tokio::test]
async fn wake_failure_restores_previous_model() {
    let hooks_a = MockHooks::new(0, 0);
    let mut hooks_b = MockHooks::new(0, 0);
    hooks_b.wake = "false".into();
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, switcher) = llmux::build_app(config).await.unwrap();
    assert_eq!(chat_request(&app, "model-a").await.0, StatusCode::OK);
    assert_eq!(
        chat_request(&app, "model-b").await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(switcher.active_model().await.as_deref(), Some("model-a"));
}

#[tokio::test]
async fn startup_reconciliation_rejects_multiple_active_models() {
    let mut hooks_a = MockHooks::new(0, 0);
    let mut hooks_b = MockHooks::new(0, 0);
    hooks_a.alive = "true".into();
    hooks_b.alive = "true".into();
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    assert!(llmux::build_app(config).await.is_err());
}

#[tokio::test]
async fn pin_persists_and_is_restored_on_restart() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let mut config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let state_path = temp_state_path("pin");
    config.orchestration.state_path = Some(state_path.clone());

    let (app, _) = llmux::build_app(config.clone()).await.unwrap();
    assert_eq!(
        control_request(&app, "POST", "/control/v1/pin", Some("model-a"), None)
            .await
            .0,
        StatusCode::OK
    );
    let persisted: Value = serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(persisted["pinned_model"], "model-a");

    let (_restarted_app, restarted) = llmux::build_app(config).await.unwrap();
    let status = restarted.controller_status().await;
    assert_eq!(status.pinned_model.as_deref(), Some("model-a"));
    assert_eq!(status.active_model.as_deref(), Some("model-a"));
    std::fs::remove_file(state_path).unwrap();
}

#[tokio::test]
async fn cancelled_waiter_is_removed_from_priority_queue() {
    let hooks_a = MockHooks::new(0, 0);
    let hooks_b = MockHooks::new(0, 0);
    let (addr_a, _) = spawn_mock_backend(0).await;
    let (addr_b, _) = spawn_mock_backend(0).await;
    let config = test_config(addr_a.port(), addr_b.port(), &hooks_a, &hooks_b);
    let (app, switcher) = llmux::build_app(config).await.unwrap();
    assert_eq!(
        control_request(&app, "POST", "/control/v1/pin", Some("model-a"), None)
            .await
            .0,
        StatusCode::OK
    );

    let queued_app = app.clone();
    let waiter = tokio::spawn(async move {
        chat_request_with(&queued_app, "model-b", Some("background"), None).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        switcher.controller_status().await.queues["model-b"].background,
        1
    );
    waiter.abort();
    let _ = waiter.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        switcher.controller_status().await.queues["model-b"].background,
        0
    );
}
