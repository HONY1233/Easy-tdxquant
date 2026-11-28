//! `eltdx-console` binary: the loopback admin console for Easy-tdxquant.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::process::ExitCode;

use eltdx_console::{run, ConsoleConfig, DEFAULT_SESSION_TTL_MINUTES};

const USAGE: &str = "\
eltdx-console — 本地管理控制台

用法:
  eltdx-console [选项]

选项:
  --bind <ADDR>                 监听地址，默认 127.0.0.1
  --port <PORT>                 监听端口，默认 8787
  --db <PATH>                   SQLite 数据库文件，默认 data/console.db
  --session-ttl-minutes <N>     会话有效期（分钟），默认 720
  --secure-cookies              为 Cookie 添加 Secure 属性（需 HTTPS 反代）
  -h, --help                    显示本帮助
";

struct Args {
    bind: IpAddr,
    port: u16,
    db: PathBuf,
    session_ttl_minutes: i64,
    secure_cookies: bool,
}

enum Parsed {
    Help,
    Run(Args),
}

fn main() -> ExitCode {
    let parsed = match parse(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("参数错误: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let args = match parsed {
        Parsed::Help => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Parsed::Run(args) => args,
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("无法创建运行时: {error}");
            return ExitCode::FAILURE;
        }
    };
    let config = ConsoleConfig {
        bind: args.bind,
        port: args.port,
        db_path: args.db.clone(),
        session_ttl_minutes: args.session_ttl_minutes,
        secure_cookies: args.secure_cookies,
    };
    println!(
        "eltdx 控制台启动: http://{}:{}/  (db: {})",
        args.bind,
        args.port,
        args.db.display()
    );
    let served = runtime.block_on(run(config));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("控制台退出: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse<I>(mut args: I) -> Result<Parsed, String>
where
    I: Iterator<Item = String>,
{
    let mut parsed = Args {
        bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 8787,
        db: PathBuf::from("data/console.db"),
        session_ttl_minutes: DEFAULT_SESSION_TTL_MINUTES,
        secure_cookies: false,
    };
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_owned(), Some(value.to_owned())),
            None => (arg.clone(), None),
        };
        let mut value = |name: &str| -> Result<String, String> {
            match inline.clone() {
                Some(value) => Ok(value),
                None => args.next().ok_or_else(|| format!("{name} 缺少取值")),
            }
        };
        match flag.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--bind" => {
                let raw = value("--bind")?;
                parsed.bind = raw
                    .parse()
                    .map_err(|_| format!("无法解析监听地址: {raw}"))?;
            }
            "--port" => {
                let raw = value("--port")?;
                parsed.port = raw.parse().map_err(|_| format!("无法解析端口: {raw}"))?;
            }
            "--db" => parsed.db = PathBuf::from(value("--db")?),
            "--session-ttl-minutes" => {
                let raw = value("--session-ttl-minutes")?;
                let minutes: i64 = raw
                    .parse()
                    .map_err(|_| format!("无法解析会话有效期: {raw}"))?;
                if minutes <= 0 {
                    return Err("会话有效期必须为正数".to_owned());
                }
                parsed.session_ttl_minutes = minutes;
            }
            "--secure-cookies" => parsed.secure_cookies = true,
            other => return Err(format!("未知选项: {other}")),
        }
    }
    Ok(Parsed::Run(parsed))
}
