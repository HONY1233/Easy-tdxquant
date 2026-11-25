use std::io;

use serde_json::json;

pub fn run() -> io::Result<()> {
    let executable = std::env::current_exe()?;
    let report = json!({
        "product": "eltdx",
        "version": env!("CARGO_PKG_VERSION"),
        "target": std::env::consts::ARCH,
        "os": std::env::consts::OS,
        "executable": executable,
        "python_required": false,
        "components": {
            "cli": true,
            "http_websocket": true,
            "mcp_stdio": true,
            "gui": cfg!(windows),
            "local_upgrade": cfg!(windows),
        },
        "default_mode": if cfg!(windows) { "gui" } else { "cli_help" },
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
