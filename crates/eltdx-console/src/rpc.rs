//! Production `Rpc` adapter: forwards collection calls to the shared gateway.

use eltdx_tools::gateway::GatewayHandle;
use serde_json::Value;

use crate::collect::{Rpc, RpcFuture};

impl Rpc for GatewayHandle {
    fn call(&self, method: &str, params: Value) -> RpcFuture<'_> {
        let handle = self.clone();
        let method = method.to_owned();
        Box::pin(async move {
            GatewayHandle::call(&handle, &method, params)
                .await
                .map_err(|error| error.to_string())
        })
    }
}
