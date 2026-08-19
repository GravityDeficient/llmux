//! Shared domain types used across the codebase.

/// Errors from the switcher
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPriority {
    Interactive,
    Background,
}

impl RequestPriority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SwitchError {
    #[error("model not found: {0}")]
    ModelNotFound(String),

    #[error("model not ready: {0}")]
    NotReady(String),

    #[error("request timeout")]
    Timeout,

    #[error("background request timed out waiting for the active model lease")]
    BackgroundTimeout,

    #[error("model is pinned: {0}")]
    Pinned(String),

    #[error("hook failed for {model}: {detail}")]
    HookFailed { model: String, detail: String },

    #[error("internal error: {0}")]
    Internal(String),
}

/// State of the model switcher
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SwitcherState {
    /// No model is currently active
    Idle,
    /// A model is awake and ready
    Active { model: String },
    /// Switching from one model to another
    Switching { from: Option<String>, to: String },
}
