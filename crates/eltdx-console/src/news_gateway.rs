//! Gateway-backed implementation of the news RPC boundary.

use eltdx_tools::gateway::GatewayHandle;
use serde_json::Value;

use crate::news_collect::NewsRpc;

/// Adapter that forwards news calls to the local 7615 gateway.
pub struct GatewayNewsRpc {
    handle: GatewayHandle,
}

impl GatewayNewsRpc {
    pub fn new(handle: GatewayHandle) -> Self {
        Self { handle }
    }
}

impl NewsRpc for GatewayNewsRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        self.handle
            .call(method, params)
            .await
            .map_err(|error| error.to_string())
    }
}
