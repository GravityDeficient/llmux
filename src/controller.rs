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
    action_response(
        state.switcher.control_switch(&selection.model).await,
        state.switcher.controller_status().await,
    )
}

async fn pin_model(
    State(state): State<ControlApiState>,
    Json(selection): Json<ModelSelection>,
) -> Response<Body> {
    action_response(
        state.switcher.pin_model(&selection.model).await,
        state.switcher.controller_status().await,
    )
}

async fn unpin_model(State(state): State<ControlApiState>) -> Response<Body> {
    action_response(
        state.switcher.unpin_model().await,
        state.switcher.controller_status().await,
    )
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
                crate::SwitchError::Pinned(_) => StatusCode::CONFLICT,
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
