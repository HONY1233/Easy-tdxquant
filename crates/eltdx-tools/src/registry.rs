//! Runtime capability registry shared by the gateway, MCP server and console.
//!
//! This module is the first step of the P1 library extraction: it exposes the
//! existing method/tool tables without changing their contents or ordering.

/// Gateway JSON-RPC methods, sorted by name.
pub fn gateway_methods() -> Vec<String> {
    crate::rpc::methods()
}

/// MCP tool names without the `eltdx_` prefix, in declaration order.
pub fn mcp_tool_names() -> Vec<&'static str> {
    crate::mcp_registry::names()
}

/// MCP tool schemas as returned by `tools/list`.
pub fn mcp_tool_schemas() -> Vec<serde_json::Value> {
    crate::mcp_registry::schemas()
}

/// `(gateway method count, MCP tool count)` for health and catalog checks.
pub fn counts() -> (usize, usize) {
    (gateway_methods().len(), mcp_tool_names().len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_methods_keep_their_baseline_contract() {
        let methods = gateway_methods();
        assert_eq!(methods.len(), 261);
        assert!(methods.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(methods
            .iter()
            .any(|method| method == "quotes.get_snapshots"));
        assert!(methods.iter().any(|method| method == "f10.company_profile"));
        assert!(methods.iter().any(|method| method == "quotes.subscribe"));
    }

    #[test]
    fn mcp_tools_keep_their_baseline_contract() {
        let names = mcp_tool_names();
        assert_eq!(names.len(), 35);
        assert_eq!(names.first().copied(), Some("quote"));
        assert_eq!(names.last().copied(), Some("docs_index"));

        let schemas = mcp_tool_schemas();
        assert_eq!(schemas.len(), names.len());
        for (name, schema) in names.iter().zip(schemas.iter()) {
            assert_eq!(schema["name"], format!("eltdx_{name}"));
        }
    }
}
