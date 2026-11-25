use std::cell::RefCell;
use std::io;
use std::os::windows::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use native_windows_gui as nwg;
use serde_json::json;

#[derive(Default)]
struct App {
    window: nwg::Window,
    code_label: nwg::Label,
    footer_label: nwg::Label,
    code_input: nwg::TextInput,
    quote_button: nwg::Button,
    start_http: nwg::Button,
    stop_http: nwg::Button,
    docs_button: nwg::Button,
    doctor_button: nwg::Button,
    upgrade_button: nwg::Button,
    help_button: nwg::Button,
    output: nwg::TextBox,
    status: nwg::Label,
    notice: nwg::Notice,
    #[allow(deprecated)]
    timer: nwg::Timer,
    receiver: RefCell<Option<Receiver<(String, bool)>>>,
    sender: RefCell<Option<Sender<(String, bool)>>>,
    http_child: RefCell<Option<Child>>,
    handler: RefCell<Option<nwg::EventHandler>>,
}

pub fn run() -> io::Result<()> {
    nwg::init().map_err(|error| io::Error::other(error.to_string()))?;
    nwg::Font::set_global_family("Segoe UI")
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut data = App::default();
    build(&mut data)?;
    let app = Rc::new(data);
    bind_events(&app)?;
    app.window.set_visible(true);
    nwg::dispatch_thread_events();
    Ok(())
}

#[allow(deprecated)]
fn build(app: &mut App) -> io::Result<()> {
    nwg::Window::builder()
        .size((1000, 720))
        .position((80, 60))
        .title("eltdx 行情工具")
        .build(&mut app.window)
        .map_err(nwg_error)?;
    nwg::Label::builder()
        .text("证券代码")
        .position((20, 20))
        .size((80, 24))
        .parent(&app.window)
        .build(&mut app.code_label)
        .map_err(nwg_error)?;

    nwg::TextInput::builder()
        .text("sz000001")
        .position((105, 18))
        .size((180, 26))
        .parent(&app.window)
        .focus(true)
        .build(&mut app.code_input)
        .map_err(nwg_error)?;
    button(&app.window, &mut app.quote_button, "查询快照", 305, 18, 110)?;
    button(&app.window, &mut app.start_http, "启动 HTTP", 425, 18, 110)?;
    button(&app.window, &mut app.stop_http, "停止 HTTP", 545, 18, 110)?;
    button(&app.window, &mut app.docs_button, "HTTP 文档", 665, 18, 110)?;
    button(
        &app.window,
        &mut app.doctor_button,
        "环境检查",
        785,
        18,
        110,
    )?;
    button(
        &app.window,
        &mut app.upgrade_button,
        "升级状态",
        20,
        58,
        110,
    )?;
    button(
        &app.window,
        &mut app.help_button,
        "MCP 配置 / 帮助",
        140,
        58,
        160,
    )?;

    nwg::Label::builder()
        .text("就绪。行情查询会访问远端服务器；本工具不自动交易。")
        .position((20, 98))
        .size((950, 24))
        .parent(&app.window)
        .build(&mut app.status)
        .map_err(nwg_error)?;
    nwg::TextBox::builder()
        .position((20, 132))
        .size((960, 500))
        .flags(
            nwg::TextBoxFlags::VISIBLE
                | nwg::TextBoxFlags::VSCROLL
                | nwg::TextBoxFlags::AUTOVSCROLL
                | nwg::TextBoxFlags::TAB_STOP,
        )
        .readonly(true)
        .parent(&app.window)
        .build(&mut app.output)
        .map_err(nwg_error)?;
    nwg::Label::builder()
        .text(&format!(
            "eltdx {} · 单文件原生 Rust · 无需 Python · 仅限个人非商业使用",
            env!("CARGO_PKG_VERSION")
        ))
        .position((20, 648))
        .size((950, 24))
        .parent(&app.window)
        .build(&mut app.footer_label)
        .map_err(nwg_error)?;

    nwg::Notice::builder()
        .parent(&app.window)
        .build(&mut app.notice)
        .map_err(nwg_error)?;
    nwg::Timer::builder()
        .parent(&app.window)
        .interval(1000)
        .stopped(true)
        .build(&mut app.timer)
        .map_err(nwg_error)?;
    Ok(())
}

fn bind_events(app: &Rc<App>) -> io::Result<()> {
    use nwg::Event as E;

    let (sender, receiver) = channel();
    *app.receiver.borrow_mut() = Some(receiver);
    *app.sender.borrow_mut() = Some(sender);
    let weak = Rc::downgrade(app);
    let callback = move |event, _data, handle| {
        let Some(app) = weak.upgrade() else {
            return;
        };
        match event {
            E::OnButtonClick if handle == app.quote_button.handle => query(&app),
            E::OnButtonClick if handle == app.start_http.handle => start_http(&app),
            E::OnButtonClick if handle == app.stop_http.handle => stop_http(&app),
            E::OnButtonClick if handle == app.docs_button.handle => open_docs(&app),
            E::OnButtonClick if handle == app.doctor_button.handle => {
                background(&app, "doctor", Vec::new())
            }
            E::OnButtonClick if handle == app.upgrade_button.handle => {
                background(&app, "upgrade", vec!["status".into()])
            }
            E::OnButtonClick if handle == app.help_button.handle => show_help(&app),
            E::OnNotice => receive(&app),
            E::OnTimerTick => check_http(&app),
            E::OnWindowClose if handle == app.window.handle => {
                stop_http(&app);
                nwg::stop_thread_dispatch();
            }
            _ => {}
        }
    };
    *app.handler.borrow_mut() = Some(nwg::full_bind_event_handler(&app.window.handle, callback));
    Ok(())
}

fn button(
    parent: &nwg::Window,
    out: &mut nwg::Button,
    text: &str,
    x: i32,
    y: i32,
    width: i32,
) -> io::Result<()> {
    nwg::Button::builder()
        .text(text)
        .position((x, y))
        .size((width, 30))
        .parent(parent)
        .build(out)
        .map_err(nwg_error)
}

fn query(app: &App) {
    let code = app.code_input.text().trim().to_owned();
    if code.is_empty() {
        app.status.set_text("请输入证券代码，例如 sz000001。");
        return;
    }
    app.status.set_text("正在查询快照，请稍候…");
    background(
        app,
        "cli",
        vec![
            "quote".into(),
            code.into(),
            "--depth".into(),
            "--timeout".into(),
            "8".into(),
        ],
    );
}

fn background(app: &App, mode: &str, args: Vec<std::ffi::OsString>) {
    let notice_sender = app.notice.sender();
    let Some(result_sender) = app.sender.borrow().clone() else {
        app.status.set_text("界面消息通道不可用。");
        return;
    };
    let executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            app.output
                .appendln(&format!("无法定位当前程序：{error}\r\n"));
            app.status.set_text("操作失败。");
            return;
        }
    };
    let mode = mode.to_owned();
    thread::spawn(move || {
        let output = Command::new(executable).arg(mode).args(args).output();
        let (message, success) = match output {
            Ok(output) => {
                let success = output.status.success();
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                let error = String::from_utf8_lossy(&output.stderr).into_owned();
                if !error.trim().is_empty() {
                    if !text.is_empty() {
                        text.push_str("\r\n");
                    }
                    text.push_str(&error);
                }
                (text, success)
            }
            Err(error) => (format!("无法启动内部命令：{error}"), false),
        };
        let _ = result_sender.send((message, success));
        notice_sender.notice();
    });
}

fn start_http(app: &App) {
    let running = app
        .http_child
        .borrow_mut()
        .as_mut()
        .is_some_and(|child| child.try_wait().ok().flatten().is_none());
    if running {
        app.status.set_text("HTTP 服务已经在运行。");
        return;
    }
    if let Some(mut child) = app.http_child.borrow_mut().take() {
        let _ = child.wait();
    }
    let executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            app.status.set_text(&format!("无法定位当前程序：{error}"));
            return;
        }
    };
    match Command::new(executable)
        .args(["http", "--host", "127.0.0.1", "--port", "8000"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x0800_0000)
        .spawn()
    {
        Ok(child) => {
            let pid = child.id();
            *app.http_child.borrow_mut() = Some(child);
            app.timer.start();
            app.status.set_text(&format!(
                "HTTP 进程已启动（PID {pid}），默认地址 http://127.0.0.1:8000 。"
            ));
            app.output.appendln("HTTP/WebSocket 服务已请求启动。\r\n健康检查：http://127.0.0.1:8000/health\r\n接口文档：http://127.0.0.1:8000/docs\r\n");
        }
        Err(error) => app.status.set_text(&format!("HTTP 启动失败：{error}")),
    }
}

fn stop_http(app: &App) {
    let mut slot = app.http_child.borrow_mut();
    if let Some(mut child) = slot.take() {
        match child.kill() {
            Ok(()) => {
                let _ = child.wait();
                app.timer.stop();
                app.status.set_text("HTTP 服务已停止。");
            }
            Err(error) => app.status.set_text(&format!("停止 HTTP 失败：{error}")),
        }
    } else {
        app.timer.stop();
        app.status.set_text("HTTP 服务未运行。");
    }
}

fn check_http(app: &App) {
    let mut slot = app.http_child.borrow_mut();
    let Some(child) = slot.as_mut() else {
        app.timer.stop();
        return;
    };
    if let Ok(Some(status)) = child.try_wait() {
        app.timer.stop();
        app.status.set_text(&format!("HTTP 进程已退出：{status}"));
        *slot = None;
    }
}

fn receive(app: &App) {
    let mut slot = app.receiver.borrow_mut();
    let Some(receiver) = slot.as_mut() else {
        return;
    };
    while let Ok((message, success)) = receiver.try_recv() {
        app.output.appendln(&message);
        app.output.appendln("\r\n");
        app.status.set_text(if success {
            "操作完成。"
        } else {
            "操作失败，请查看输出。"
        });
    }
    app.output.scroll_lastline();
}

fn open_docs(app: &App) {
    if let Err(error) = Command::new("cmd.exe")
        .args(["/C", "start", "", "http://127.0.0.1:8000/docs"])
        .creation_flags(0x0800_0000)
        .spawn()
    {
        app.status.set_text(&format!("无法打开浏览器：{error}"));
    }
}

fn show_help(app: &App) {
    let executable =
        std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("eltdx.exe"));
    let config = json!({
        "mcpServers": {
            "eltdx": {
                "command": executable,
                "args": ["mcp"]
            }
        }
    });
    let text = format!(
        "MCP 配置（JSON）：\r\n{}\r\n\r\n命令行示例：\r\neltdx quote sz000001 --depth\r\neltdx cli kline sz000001 --count 100\r\neltdx http --host 127.0.0.1 --port 8000\r\neltdx mcp\r\neltdx doctor\r\neltdx upgrade status\r\n\r\n升级：\r\n1. 从可信来源取得新版 eltdx.exe 和 SHA-256。\r\n2. 保存工作，关闭所有 eltdx 窗口和服务。\r\n3. 运行 eltdx upgrade install --file <新版.exe> --sha256 <哈希>。\r\n程序会在退出后先备份旧文件，再替换并校验；失败时尝试恢复并保留备份。成功后若备份清理失败，也会保留备份并记录日志。\r\n",
        serde_json::to_string_pretty(&config).unwrap_or_else(|_| "{}".into())
    );
    app.output.set_text(&text);
}

fn nwg_error(error: nwg::NwgError) -> io::Error {
    io::Error::other(error.to_string())
}
