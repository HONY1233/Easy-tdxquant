#[path = "cli/mod.rs"]
mod cli;
#[path = "unified/doctor.rs"]
mod doctor;
#[path = "eltdx-http.rs"]
mod http_entry;
#[path = "eltdx-mcp.rs"]
mod mcp_entry;
#[path = "unified/upgrade.rs"]
mod upgrade;

#[cfg(windows)]
#[path = "unified/gui.rs"]
mod gui;

use std::ffi::OsString;
use std::process::ExitCode;

const HELP: &str = "eltdx 统一工具\n\n用法:\n  eltdx                     启动图形界面（Windows）\n  eltdx gui                 启动图形界面（Windows）\n  eltdx cli <命令> [参数]   运行行情命令行\n  eltdx http [参数]         启动 HTTP/WebSocket 服务\n  eltdx mcp                 启动 MCP stdio 服务\n  eltdx doctor              检查运行环境\n  eltdx upgrade <命令>      查看状态或安装本地升级包\n  eltdx <原生命令> [参数]   兼容原有 CLI 用法\n\n示例:\n  eltdx quote sz000001 --depth\n  eltdx cli kline sz000001 --count 100\n  eltdx http --host 127.0.0.1 --port 8000\n  eltdx upgrade install --file D:\\updates\\eltdx.exe --sha256 <SHA256>";

fn main() -> ExitCode {
    dispatch(std::env::args_os().collect())
}

fn dispatch(argv: Vec<OsString>) -> ExitCode {
    let mode = argv.get(1).and_then(|value| value.to_str()).unwrap_or("");
    match mode {
        "" => {
            #[cfg(windows)]
            {
                run_gui()
            }
            #[cfg(not(windows))]
            {
                println!("{HELP}");
                ExitCode::SUCCESS
            }
        }
        "-h" | "--help" | "help" => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        "-V" | "--version" => {
            println!("eltdx {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        "gui" => run_gui(),
        "cli" => {
            let mut cli_argv = Vec::with_capacity(argv.len());
            cli_argv.push(argv.first().cloned().unwrap_or_else(|| "eltdx".into()));
            cli_argv.extend(argv.into_iter().skip(2));
            cli::main_with_args(cli_argv)
        }
        "http" => {
            let mut http_argv = vec![OsString::from("eltdx-http")];
            http_argv.extend(argv.into_iter().skip(2));
            match block_on(http_entry::run_with_args(http_argv)) {
                Ok(Ok(())) => ExitCode::SUCCESS,
                Ok(Err(error)) => fail(&error),
                Err(error) => fail(&error),
            }
        }
        "mcp" => match mcp_entry::run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => fail(&error),
        },
        "doctor" => match doctor::run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => fail(&error),
        },
        "upgrade" => match upgrade::run(&argv[2..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => fail(&error),
        },
        _ => cli::main_with_args(argv),
    }
}

#[cfg(windows)]
fn run_gui() -> ExitCode {
    match gui::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => fail(&error),
    }
}

#[cfg(not(windows))]
fn run_gui() -> ExitCode {
    fail("图形界面仅提供 Windows 版本；Linux/macOS 请使用 cli、http 或 mcp。")
}

fn fail(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("错误: {error}");
    ExitCode::FAILURE
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> std::io::Result<T> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    Ok(runtime.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_mode_forwards_arguments() {
        let args = vec![
            OsString::from("eltdx"),
            OsString::from("cli"),
            OsString::from("version"),
        ];
        assert_eq!(dispatch(args), ExitCode::SUCCESS);
    }
}
