use crate::ModelSwitcher;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    routing::{get, post},
};
use serde::Deserialize;
use std::future::Future;

#[derive(Clone)]
pub(crate) struct ControlApiState {
    switcher: ModelSwitcher,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelSelection {
    model: String,
}

pub(crate) fn router(switcher: ModelSwitcher) -> Router {
    Router::new()
        .route("/control/v1/state", get(get_state))
        .route("/control/v1/stop", post(stop_model))
        .route("/control/v1/switch", post(switch_model))
        .route("/control/v1/pin", post(pin_model).delete(unpin_model))
        .with_state(ControlApiState { switcher })
}

async fn get_state(State(state): State<ControlApiState>) -> impl IntoResponse {
    Json(state.switcher.controller_status().await)
}

async fn switch_model(
    State(state): State<ControlApiState>,
    Json(selection): Json<ModelSelection>,
) -> Response<Body> {
    // Lifecycle hooks can take many minutes. Run the operation independently
    // of the HTTP request so a browser disconnect or facade restart cannot
    // cancel it halfway through and strand the switcher in `switching`.
    let status_switcher = state.switcher.clone();
    let action_switcher = state.switcher;
    let model = selection.model;
    let result = detached_action(async move { action_switcher.control_switch(&model).await }).await;
    action_response(result, status_switcher.controller_status().await)
}

async fn pin_model(
    State(state): State<ControlApiState>,
    Json(selection): Json<ModelSelection>,
) -> Response<Body> {
    let status_switcher = state.switcher.clone();
    let action_switcher = state.switcher;
    let model = selection.model;
    let result = detached_action(async move { action_switcher.pin_model(&model).await }).await;
    action_response(result, status_switcher.controller_status().await)
}

async fn unpin_model(State(state): State<ControlApiState>) -> Response<Body> {
    let status_switcher = state.switcher.clone();
    let action_switcher = state.switcher;
    let result = detached_action(async move { action_switcher.unpin_model().await }).await;
    action_response(result, status_switcher.controller_status().await)
}

async fn detached_action<F>(action: F) -> Result<(), crate::SwitchError>
where
    F: Future<Output = Result<(), crate::SwitchError>> + Send + 'static,
{
    tokio::spawn(action)
        .await
        .map_err(|error| crate::SwitchError::Internal(format!("control task failed: {error}")))?
}

fn action_response(
    result: Result<(), crate::SwitchError>,
    status: crate::switcher::ControllerStatus,
) -> Response<Body> {
    match result {
        Ok(()) => Json(status).into_response(),
        Err(error) => {
            let code = match error {
                crate::SwitchError::ModelNotFound(_) => StatusCode::NOT_FOUND,
                crate::SwitchError::Pinned(_) | crate::SwitchError::NotReady(_) => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                code,
                Json(serde_json::json!({
                    "error": {"message": error.to_string(), "type": "llmux_control_error"},
                    "state": status,
                })),
            )
                .into_response()
        }
    }
}

async fn stop_model(State(state): State<ControlApiState>) -> Response<Body> {
    let status_switcher = state.switcher.clone();
    let result = detached_action(async move { state.switcher.manual_transition(None).await }).await;
    action_response(result, status_switcher.controller_status().await)
}
