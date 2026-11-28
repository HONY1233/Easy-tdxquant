//! Shared HTTP/WebSocket gateway runtime.
//!
//! The standalone HTTP binary and the embedded console share this module, the
//! JSON-RPC dispatcher and the quote subscription registry.

mod server;

pub use server::{health_payload, methods_payload, router, GatewayHandle};

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;

    #[test]
    fn methods_payload_keeps_the_gateway_contract() {
        let payload = methods_payload();
        let methods = match payload["methods"].as_array() {
            Some(methods) => methods,
            None => panic!("methods must be an array"),
        };
        assert_eq!(methods.len(), 261);
        assert_eq!(
            payload["websocket_only"],
            json!(["quotes.subscribe", "quotes.unsubscribe"])
        );
        assert_eq!(payload.as_object().map(serde_json::Map::len), Some(2));
    }

    #[test]
    fn health_payload_keeps_the_gateway_contract() {
        let payload = health_payload();
        assert_eq!(payload["ok"], true);
        assert_eq!(payload["service"], "eltdx");
        assert_eq!(payload["transports"], json!(["http", "websocket"]));
        assert_eq!(payload["native_push_method"], "quotes.subscribe");
    }

    #[test]
    fn request_errors_use_the_gateway_envelope() {
        let error = match crate::rpc::parse_request(json!({"id": 7})) {
            Ok(_) => panic!("missing method must fail"),
            Err(error) => error,
        };
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.envelope(json!(7)),
            json!({
                "id": 7,
                "ok": false,
                "error": {
                    "type": "GatewayRequestError",
                    "message": "method must be a non-empty string"
                }
            })
        );
    }

    #[tokio::test]
    async fn dispatch_returns_success_and_unknown_method_envelopes() {
        let client = match eltdx::TdxClient::builder().build() {
            Ok(client) => client,
            Err(error) => panic!("client build failed: {error}"),
        };
        let handle = GatewayHandle::new(client);

        let (status, body) = handle
            .dispatch(json!({"id": "a", "method": "clear_cache", "params": {}}))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"id": "a", "ok": true, "result": null}));

        let (status, body) = handle
            .dispatch(json!({"id": "b", "method": "not.a_method", "params": {}}))
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            body,
            json!({
                "id": "b",
                "ok": false,
                "error": {
                    "type": "GatewayMethodError",
                    "message": "unknown method: not.a_method"
                }
            })
        );

        handle.shutdown();
    }
}
