use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use axum::middleware::Next;

#[derive(Clone)]
pub(crate) struct BearerAuth(pub(crate) Option<String>);

pub(crate) async fn require_bearer(
    axum::extract::State(auth): axum::extract::State<BearerAuth>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let Some(expected) = auth.0.as_deref() else {
        return next.run(request).await;
    };

    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    if supplied.is_some_and(|value| constant_time_eq(value.as_bytes(), expected.as_bytes())) {
        next.run(request).await
    } else {
        json_error(StatusCode::UNAUTHORIZED, "invalid or missing bearer token")
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let len = left.len().max(right.len());
    for i in 0..len {
        diff |= usize::from(*left.get(i).unwrap_or(&0) ^ *right.get(i).unwrap_or(&0));
    }
    diff == 0
}

pub(crate) fn json_error(status: StatusCode, message: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": {"message": message, "type": "llmux_error"}
    });
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid error response")
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn token_comparison() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
