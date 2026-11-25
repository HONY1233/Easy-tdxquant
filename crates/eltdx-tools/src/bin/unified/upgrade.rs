use std::error::Error;
use std::ffi::OsString;
#[cfg(any(windows, test))]
use std::fs;
#[cfg(any(windows, test))]
use std::fs::File;
#[cfg(any(windows, test))]
use std::io::Read;
#[cfg(any(windows, test))]
use std::path::Path;
use std::path::PathBuf;
#[cfg(windows)]
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};
use serde_json::json;
#[cfg(any(windows, test))]
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(name = "eltdx upgrade", about = "查看状态或安装本地升级包")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 显示当前版本、程序位置和升级能力
    Status,
    /// 安装同目录下的新 EXE；退出当前程序后由辅助脚本替换
    Install(Install),
}

#[derive(Args)]
struct Install {
    /// 新版本 EXE 的本地路径
    #[arg(long)]
    file: PathBuf,
    /// 新版本官方提供的 SHA-256（必填）
    #[arg(long)]
    sha256: String,
}

pub fn run(argv: &[OsString]) -> Result<()> {
    let cli = Cli::try_parse_from(
        std::iter::once(OsString::from("eltdx upgrade")).chain(argv.iter().cloned()),
    )?;
    match cli.command {
        Command::Status => status(),
        Command::Install(args) => install(args),
    }
}

fn status() -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "product": "eltdx",
            "version": env!("CARGO_PKG_VERSION"),
            "executable": std::env::current_exe()?,
            "install_supported": cfg!(windows),
            "network_download": false,
            "instruction": "下载官方新版本和 SHA-256 后运行：eltdx upgrade install --file <新版本.exe> --sha256 <哈希>。安装前请保存工作并关闭正在运行的 eltdx。"
        }))?
    );
    Ok(())
}

fn install(args: Install) -> Result<()> {
    #[cfg(not(windows))]
    {
        let _ = args;
        Err("本地 EXE 替换仅支持 Windows。".into())
    }

    #[cfg(windows)]
    {
        install_windows(args)
    }
}

#[cfg(windows)]
fn install_windows(args: Install) -> Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    let source = normalize_path(args.file.canonicalize()?);
    let target = normalize_path(std::env::current_exe()?.canonicalize()?);
    if source == target {
        return Err("升级文件不能是当前正在运行的 EXE。".into());
    }
    if !source.is_file() {
        return Err("升级文件不存在或不是普通文件。".into());
    }
    validate_pe_header(&source)?;
    let actual_hash = sha256_file(&source)?;
    let expected = args.sha256.trim();
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("SHA-256 必须是 64 位十六进制字符串。".into());
    }
    if !actual_hash.eq_ignore_ascii_case(expected) {
        return Err("SHA-256 校验失败，已取消安装。".into());
    }

    let parent = target.parent().ok_or("无法确定当前 EXE 所在目录。")?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let stem = target
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("eltdx");
    let staged = parent.join(format!("{stem}.upgrade-{}-{nonce}.exe", std::process::id()));
    let helper = parent.join(format!("{stem}.upgrade-{}-{nonce}.ps1", std::process::id()));
    let log = parent.join(format!("{stem}.upgrade-{}-{nonce}.log", std::process::id()));
    let status_file = parent.join(format!(
        "{stem}.upgrade-{}-{nonce}.status",
        std::process::id()
    ));
    let backup = parent.join(format!("{stem}.backup-{}-{nonce}.exe", std::process::id()));
    fs::copy(&source, &staged)?;
    if sha256_file(&staged)? != actual_hash {
        let _ = fs::remove_file(&staged);
        return Err("升级文件暂存后校验失败，已取消安装。".into());
    }

    let original_hash = match create_verified_backup(&target, &backup) {
        Ok(hash) => hash,
        Err(error) => {
            let _ = fs::remove_file(&staged);
            return Err(error);
        }
    };
    let script = replacement_script(
        &target,
        &staged,
        &backup,
        &log,
        &status_file,
        &actual_hash,
        &original_hash,
    );
    if let Err(error) = fs::write(&helper, utf16le_with_bom(&script)) {
        let _ = fs::remove_file(&staged);
        let _ = fs::remove_file(&backup);
        return Err(error.into());
    }
    let powershell = PathBuf::from(
        std::env::var_os("SystemRoot").unwrap_or_else(|| OsString::from("C:\\Windows")),
    )
    .join("System32")
    .join("WindowsPowerShell")
    .join("v1.0")
    .join("powershell.exe");
    let spawn_result = Command::new(powershell)
        .args([
            OsString::from("-NoProfile"),
            OsString::from("-WindowStyle"),
            OsString::from("Hidden"),
            OsString::from("-ExecutionPolicy"),
            OsString::from("Bypass"),
            OsString::from("-File"),
            helper.clone().into_os_string(),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .creation_flags(0x0800_0000)
        .spawn();
    if let Err(error) = spawn_result {
        let _ = fs::remove_file(&staged);
        let _ = fs::remove_file(&helper);
        let _ = fs::remove_file(&backup);
        return Err(format!("无法启动升级辅助进程: {error}").into());
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": "staged",
            "version": env!("CARGO_PKG_VERSION"),
            "target": target,
            "staged_file": staged,
            "helper": helper,
            "log": log,
            "status_file": status_file,
            "backup": backup,
            "sha256": actual_hash,
            "sha256_verified": true,
            "next_step": "请保存工作并正常关闭所有 eltdx 窗口/服务。旧文件已在安装前备份并校验 SHA-256；辅助进程会在当前 EXE 退出后替换并再次校验新文件，成功后删除备份，失败尝试恢复并保留备份。若成功后备份清理失败，会保留备份并记录日志。结果写入 status 文件，详情写入 log 文件。"
        }))?
    );
    Ok(())
}

#[cfg(windows)]
fn normalize_path(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = text.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path
    }
}

#[cfg(any(windows, test))]
fn replacement_script(
    target: &Path,
    staged: &Path,
    backup: &Path,
    log: &Path,
    status_file: &Path,
    expected_hash: &str,
    original_hash: &str,
) -> String {
    let target = ps_quote(target);
    let staged = ps_quote(staged);
    let backup = ps_quote(backup);
    let log = ps_quote(log);
    let status_file = ps_quote(status_file);
    let expected_hash = ps_quote(Path::new(expected_hash));
    let original_hash = ps_quote(Path::new(original_hash));
    let pid = std::process::id();
    format!(
        "$ErrorActionPreference = 'Stop'\n\
    $target = {target}\n\
    $staged = {staged}\n\
    $backup = {backup}\n\
    $log = {log}\n\
    $statusFile = {status_file}\n\
    $expectedHash = {expected_hash}\n\
    $originalHash = {original_hash}\n\
    $processId = {pid}\n\
    try {{\n\
    'eltdx upgrade started' | Set-Content -LiteralPath $log -Encoding UTF8\n\
    while (Get-Process -Id $processId -ErrorAction SilentlyContinue) {{ Start-Sleep -Milliseconds 300 }}\n\
    $stagedHash = (Get-FileHash -LiteralPath $staged -Algorithm SHA256).Hash.ToLowerInvariant()\n\
    if ($stagedHash -ne $expectedHash) {{ throw 'Staged file SHA-256 mismatch' }}\n\
    $backupHash = (Get-FileHash -LiteralPath $backup -Algorithm SHA256).Hash.ToLowerInvariant()\n\
    if ($backupHash -ne $originalHash) {{ throw 'Backup file SHA-256 mismatch' }}\n\
    try {{\n\
        $replacementStarted = $false\n\
        Move-Item -LiteralPath $staged -Destination $target -Force\n\
        $replacementStarted = $true\n\
        $installedHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()\n\
        if ($installedHash -ne $expectedHash) {{ throw 'Installed file SHA-256 mismatch' }}\n\
        'success' | Set-Content -LiteralPath $statusFile -Encoding ASCII\n\
        'eltdx upgrade succeeded' | Add-Content -LiteralPath $log -Encoding UTF8\n\
    }} catch {{\n\
        $upgradeError = $_\n\
        $rollbackFailed = $false\n\
        if ($replacementStarted -and (Test-Path -LiteralPath $backup)) {{\n\
            try {{\n\
                Copy-Item -LiteralPath $backup -Destination $target -Force\n\
                $restoredHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()\n\
                if ($restoredHash -ne $originalHash) {{ throw 'Restored file SHA-256 mismatch' }}\n\
            }} catch {{\n\
                $rollbackFailed = $true\n\
                $_ | Out-String | Add-Content -LiteralPath $log -Encoding UTF8\n\
            }}\n\
        }}\n\
        if ($rollbackFailed) {{ throw ('Upgrade failed and rollback failed. Backup retained at ' + $backup + '. ' + $upgradeError.Exception.Message) }}\n\
        throw $upgradeError\n\
    }}\n\
    try {{\n\
        Remove-Item -LiteralPath $backup -Force\n\
    }} catch {{\n\
        try {{\n\
            ('Upgrade succeeded but backup cleanup failed. Backup retained at ' + $backup + '. ' + $_.Exception.Message) | Add-Content -LiteralPath $log -Encoding UTF8\n\
        }} catch {{}}\n\
    }}\n\
    }} catch {{\n\
    try {{ $_ | Out-String | Add-Content -LiteralPath $log -Encoding UTF8 }} catch {{}}\n\
    try {{ 'failed' | Set-Content -LiteralPath $statusFile -Encoding ASCII }} catch {{}}\n\
    exit 1\n\
    }}\n"
    )
}

#[cfg(any(windows, test))]
fn create_verified_backup(target: &Path, backup: &Path) -> Result<String> {
    let original_hash = sha256_file(target)?;
    fs::copy(target, backup)?;
    if sha256_file(backup)? != original_hash {
        let _ = fs::remove_file(backup);
        return Err("升级备份校验失败，已取消安装。".into());
    }
    Ok(original_hash)
}

#[cfg(any(windows, test))]
fn ps_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "''"))
}

#[cfg(any(windows, test))]
fn utf16le_with_bom(text: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + text.len() * 2);
    bytes.extend_from_slice(&[0xff, 0xfe]);
    for unit in text.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

#[cfg(any(windows, test))]
fn validate_pe_header(path: &Path) -> Result<()> {
    let mut file = File::open(path)?;
    let mut header = [0_u8; 2];
    file.read_exact(&mut header)?;
    if header != *b"MZ" {
        return Err("升级文件不是有效的 Windows 可执行文件（缺少 MZ 头）。".into());
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn validates_windows_header() {
        let path = std::env::temp_dir().join(format!("eltdx-header-{}.bin", std::process::id()));
        let mut file = File::create(&path).unwrap_or_else(|_| panic!("create temp file"));
        file.write_all(b"MZtest")
            .unwrap_or_else(|_| panic!("write temp file"));
        drop(file);
        assert!(validate_pe_header(&path).is_ok());
        fs::write(&path, b"not executable").unwrap_or_else(|_| panic!("rewrite temp file"));
        assert!(validate_pe_header(&path).is_err());
        let _ = fs::remove_file(path);
    }

    #[cfg(windows)]
    #[test]
    fn normalizes_windows_paths_for_powershell() {
        assert_eq!(
            normalize_path(PathBuf::from(r"\\?\C:\eltdx\eltdx.exe")),
            PathBuf::from(r"C:\eltdx\eltdx.exe")
        );
        assert_eq!(
            normalize_path(PathBuf::from(r"\\?\UNC\server\share\eltdx.exe")),
            PathBuf::from(r"\\server\share\eltdx.exe")
        );
    }

    #[test]
    fn replacement_script_verifies_backup_before_replacing() {
        let script = replacement_script(
            Path::new(r"C:\工具\eltdx.exe"),
            Path::new(r"C:\工具\new.exe"),
            Path::new(r"C:\工具\backup.exe"),
            Path::new(r"C:\工具\upgrade.log"),
            Path::new(r"C:\工具\upgrade.status"),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        );
        let backup_at = script
            .find("if ($backupHash -ne $originalHash)")
            .unwrap_or(usize::MAX);
        let replace_at = script
            .find("Move-Item -LiteralPath $staged -Destination $target -Force")
            .unwrap_or(usize::MAX);
        assert!(backup_at < replace_at);
        assert!(script.contains("Backup file SHA-256 mismatch"));
        assert!(script.contains("Installed file SHA-256 mismatch"));
        assert!(script.contains("$replacementStarted = $true"));
        assert!(script.contains("if ($replacementStarted -and (Test-Path -LiteralPath $backup))"));
        assert!(script.contains("Copy-Item -LiteralPath $backup -Destination $target -Force"));
        assert!(script.contains("Backup retained at"));
        assert!(script.contains("backup cleanup failed. Backup retained at"));
        let success_at = script
            .find("'success' | Set-Content -LiteralPath $statusFile")
            .unwrap_or(usize::MAX);
        let cleanup_at = script
            .find("Remove-Item -LiteralPath $backup -Force")
            .unwrap_or(usize::MAX);
        assert!(success_at < cleanup_at);
        assert!(script.contains(r"C:\工具\eltdx.exe"));
    }

    #[test]
    fn creates_backup_with_matching_hash() {
        let dir = std::env::temp_dir().join(format!("eltdx-backup-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap_or_else(|_| panic!("create temp dir"));
        let target = dir.join("target.exe");
        let backup = dir.join("backup.exe");
        fs::write(&target, b"MZoriginal").unwrap_or_else(|_| panic!("write target"));
        let hash = create_verified_backup(&target, &backup).unwrap_or_default();
        assert_eq!(hash, sha256_file(&target).unwrap_or_default());
        assert_eq!(
            sha256_file(&backup).unwrap_or_default(),
            sha256_file(&target).unwrap_or_default()
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn encodes_powershell_as_utf16le_with_bom() {
        let bytes = utf16le_with_bom("eltdx 升级");
        assert_eq!(&bytes[..2], &[0xff, 0xfe]);
        let units: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect();
        assert_eq!(String::from_utf16(&units).unwrap_or_default(), "eltdx 升级");
    }

    #[test]
    fn hashes_file_content() {
        let path = std::env::temp_dir().join(format!("eltdx-hash-{}.bin", std::process::id()));
        fs::write(&path, b"abc").unwrap_or_else(|_| panic!("write temp file"));
        assert_eq!(
            sha256_file(&path).unwrap_or_else(|_| panic!("hash temp file")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let _ = fs::remove_file(path);
    }
}
