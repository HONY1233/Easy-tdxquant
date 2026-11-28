# AGENTS.md — 项目级规则：easy-tdxquant

> status: active ｜ version: 1.3 ｜ owner: qtt19
> 规则正本：本文件；全局补充：~/.agent/AGENTS.md

## 0. Commands（命令前置，带参数）

```bash
cd ~/projects/easy-tdxquant
source ~/.agent/config/env.sh
export RUSTUP_HOME="$HOME/.agent/tools/rustup"
export CARGO_HOME="$HOME/.agent/tools/cargo"
export PATH="$CARGO_HOME/bin:$PATH"

# 环境准备（新增依赖前须取得用户授权）
rustup toolchain install 1.89.0 --profile minimal --component rustfmt,clippy
uv venv --python 3.14 .venv
uv pip install --python .venv/bin/python -e ".[dev,mcp,http]"

# Rust 质量门禁
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 check --workspace --all-targets --locked
cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
cargo +1.89.0 test --workspace --locked

# Python 回归
.venv/bin/python scripts/verification/run_python_test_group.py round4-local
.venv/bin/python scripts/verification/run_python_test_group.py round5-loopback
.venv/bin/python scripts/ci/run_wire_golden.py

# 文档门禁
.venv/bin/python scripts/package/sync_docs.py --check
.venv/bin/python -m mkdocs build --strict
.venv/bin/python scripts/verification/check_pages_links.py site

# 提交前检查
git diff --check
git status --porcelain
```

## 1. Project overview

eltdx 是通达信行情与数据客户端，Python API 与纯 Rust SDK 共用 Rust 协议核心，并提供原生 CLI、HTTP/WebSocket 网关和 MCP 服务。

## 2. Stack（写清版本）

- Rust：工作区 `3.4.0-beta.2`，edition 2021，MSRV 1.89；使用 Cargo 与 `Cargo.lock`。
- Python：CPython 3.10–3.14；绑定通过 PyO3 0.29 与 maturin 构建。
- Python 依赖：项目内 `.venv`，使用 uv；禁止全局安装。
- 文档：MkDocs strict；根 `docs/` 为权威源，`src/eltdx/docs/` 为包内镜像。

## 3. Project structure

- `crates/eltdx`：独立 Rust SDK；`crates/eltdx-runtime`：连接与运行时；`crates/eltdx-protocol`：协议模型。
- `crates/eltdx-python`：PyO3 绑定；`crates/eltdx-tools`：CLI、HTTP、MCP 与统一 Windows EXE。
- `src/eltdx`：Python 公共 API；`tests/contracts`、`tests/integration`、`tests/packaging`：Python 合同与交付验证。
- `docs/rust-sdk`：跨会话恢复点、统一验证记录和最终验收索引。
- `data/ outputs/ tmp/ logs/`：软链到 `~/.agent/projects/easy-tdxquant/`，不进 Git。

## 4. Testing / 完成的定义（DoD）

- 修改 Rust 后至少运行受影响的 `cargo +1.89.0 test`，并运行 `fmt --check` 与严格 Clippy。
- 修改 Python 后运行对应测试组；修改文档后运行镜像检查与 MkDocs strict。
- 任一步失败即停止并报告，不跳过、不静默降级、不伪造输出；未验证不写“已完成”。
- 收尾运行 `agent-clean` 预览，检查 `git status --porcelain`，确认软链未被跟踪。

## 5. Code style & Git workflow

- 保持最小改动，不顺手格式化无关代码；依赖变更必须同步锁文件并说明原因。
- `main` 是唯一长期分支；改动使用 `feature/`、`bugfix/`、`hotfix/` 短命分支。
- 提交信息使用 `类型: 简述`；只暂存明确文件，不使用 `git add -A`。
- 不擅自 commit、push、合并或发布；AI 改动须经人审阅 diff。

## 6. Boundaries（三档）

Always

- 在 WSL / Ubuntu、项目根目录、加载 `env.sh` 后执行命令。
- 修改前读取本文件、相关源码与测试，以及 `docs/rust-sdk/IMPLEMENTATION_STATUS.md` 的最新恢复点。
- 真实配置放被忽略的 `.env`；只提交 `.env.example` 或 `*.example`。

Ask first

- 新增或升级依赖、切换包管理器、修改公开接口。
- `sudo`、删除、覆写配置、批量文件操作。
- commit、push、合并、发布及任何远程写入。

Never

- 在 Windows 侧安装依赖或运行项目测试；PATH 不得混入 `/mnt/c`。
- 提交 `.env`、密钥、日志、临时文件、数据文件或 `~/.agent/` 内容。
- 把本地 patch、PATH 隔离或旧候选证据表述为公开 registry、物理隔离或最新候选验收。

## 7. 已知的坑

- `crates/eltdx-tools/src/bin/cli/extension.rs` 与 `crates/eltdx-tools/src/gateway/helpers.rs` 的 `#[cfg(test)] mod tests` 必须位于文件末尾，否则严格 Clippy 会触发 `items_after_test_module`。
- `tests/contracts/manifests/python_test_groups.json` 必须恰好登记每个 `tests/**/test_*.py` 一次；新增测试文件未登记会让 `round4-local` 的分组契约失败。
- maturin 会在 sdist 中裁剪非 Python workspace 成员但保留完整 `Cargo.lock`；不要在 `[tool.maturin]` 全局设置 `locked = true`，发布 wheel 的锁定由工作流显式传入 `--locked`。
- Windows 统一 EXE 的升级冒烟只能在 Windows runner 验证；WSL 静态检查不能替代该工作流。
- GitHub Pages 部署 404 通常表示仓库尚未启用 Pages 或构建来源未设为 GitHub Actions，不是 MkDocs 构建失败。

## 8. 维护

规则只记录代码无法直接表达的真实约束；每次实质修改更新 version。
