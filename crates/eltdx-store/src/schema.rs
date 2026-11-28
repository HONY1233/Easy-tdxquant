//! Forward-only SQLite schema for the local console.

pub const SCHEMA_VERSION: i64 = 2;

pub const MIGRATION_1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
    expires_at TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    port INTEGER NOT NULL,
    dataset TEXT NOT NULL,
    method TEXT NOT NULL,
    params_json TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental', 'backfill', 'refresh')),
    watermark_type TEXT,
    overlap_days INTEGER NOT NULL DEFAULT 0 CHECK (overlap_days >= 0),
    shard_size INTEGER NOT NULL DEFAULT 100 CHECK (shard_size > 0),
    max_pages INTEGER,
    max_records INTEGER,
    schedule_kind TEXT NOT NULL DEFAULT 'manual',
    schedule_expr TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    timeout_ms INTEGER NOT NULL DEFAULT 8000 CHECK (timeout_ms > 0),
    retry INTEGER NOT NULL DEFAULT 2 CHECK (retry >= 0),
    concurrency INTEGER NOT NULL DEFAULT 1 CHECK (concurrency > 0),
    retention_days INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES collection_tasks(id) ON DELETE CASCADE,
    "trigger" TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental', 'backfill', 'refresh')),
    range_start TEXT,
    range_end TEXT,
    watermark_before TEXT,
    watermark_after TEXT,
    pages INTEGER NOT NULL DEFAULT 0,
    resumed_from INTEGER REFERENCES collection_runs(id),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    rows INTEGER NOT NULL DEFAULT 0,
    bytes INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    duration_ms INTEGER
);

CREATE TABLE IF NOT EXISTS dataset_state (
    dataset TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    watermark TEXT,
    last_full_at TEXT,
    last_incremental_at TEXT,
    last_run_id INTEGER,
    status TEXT NOT NULL DEFAULT 'idle',
    rows INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (dataset, scope_key)
);

CREATE TABLE IF NOT EXISTS collection_checkpoints (
    run_id INTEGER NOT NULL REFERENCES collection_runs(id) ON DELETE CASCADE,
    shard_key TEXT NOT NULL,
    cursor_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    rows INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, shard_key)
);

CREATE TABLE IF NOT EXISTS collected_payloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    port INTEGER NOT NULL,
    dataset TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL DEFAULT '',
    params_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE (dataset, code, trade_date, params_hash)
);

CREATE TABLE IF NOT EXISTS quote_snapshots (
    code TEXT NOT NULL,
    ts TEXT NOT NULL,
    price REAL,
    pre_close REAL,
    open REAL,
    high REAL,
    low REAL,
    volume REAL,
    amount REAL,
    depth_json TEXT,
    run_id INTEGER,
    PRIMARY KEY (code, ts)
);

CREATE TABLE IF NOT EXISTS kline_bars (
    code TEXT NOT NULL,
    source TEXT NOT NULL,
    period TEXT NOT NULL,
    adjust TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    run_id INTEGER,
    PRIMARY KEY (code, source, period, adjust, trade_date)
);

CREATE TABLE IF NOT EXISTS company_info_sections (
    code TEXT NOT NULL,
    market TEXT NOT NULL,
    filename TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    length INTEGER NOT NULL,
    section_name TEXT,
    text TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    run_id INTEGER,
    PRIMARY KEY (code, market, filename, "offset")
);

CREATE TABLE IF NOT EXISTS bond_metrics (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    bond_price REAL,
    conversion_price REAL,
    conversion_value REAL,
    premium_rate REAL,
    run_id INTEGER,
    PRIMARY KEY (code, trade_date)
);

CREATE TABLE IF NOT EXISTS fund_components (
    fund_code TEXT NOT NULL,
    report_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    quantity REAL,
    weight REAL,
    market_value REAL,
    run_id INTEGER,
    PRIMARY KEY (fund_code, report_date, symbol)
);

CREATE TABLE IF NOT EXISTS minute_bars (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    minute TEXT NOT NULL,
    price REAL,
    volume REAL,
    amount REAL,
    avg_price REAL,
    run_id INTEGER,
    PRIMARY KEY (code, trade_date, minute)
);

CREATE TABLE IF NOT EXISTS trade_ticks (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    seq INTEGER NOT NULL,
    time TEXT,
    price REAL,
    volume REAL,
    side TEXT,
    run_id INTEGER,
    PRIMARY KEY (code, trade_date, seq)
);

CREATE TABLE IF NOT EXISTS f10_documents (
    code TEXT NOT NULL,
    entry TEXT NOT NULL,
    section TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    run_id INTEGER,
    PRIMARY KEY (code, entry, section)
);

CREATE TABLE IF NOT EXISTS change_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    op TEXT NOT NULL,
    row_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_tools (
    tool_name TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_call_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tool TEXT NOT NULL,
    params_digest TEXT NOT NULL,
    started_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
    error_code TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    ip TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_task_started ON collection_runs(task_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_checkpoints_run_status ON collection_checkpoints(run_id, status);
CREATE INDEX IF NOT EXISTS idx_payloads_dataset_fetched ON collected_payloads(dataset, fetched_at);
CREATE INDEX IF NOT EXISTS idx_payloads_code ON collected_payloads(code);
CREATE INDEX IF NOT EXISTS idx_klines_code_date ON kline_bars(code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_quotes_ts ON quote_snapshots(ts DESC);
CREATE INDEX IF NOT EXISTS idx_minutes_code_date ON minute_bars(code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ticks_code_date ON trade_ticks(code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_changes_created ON change_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);

INSERT OR IGNORE INTO settings(key, value, updated_at) VALUES
    ('console.port', '8787', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('data.retention.raw_days', '7', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('data.retention.snapshot_days', '30', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('data.retention.change_days', '7', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('security.remote_bind', 'false', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
"#;

pub const MIGRATION_2: &str = r#"
CREATE TABLE IF NOT EXISTS news_channels (
    source TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    group_name TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source, channel_id)
);

CREATE TABLE IF NOT EXISTS news_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    source TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    code TEXT,
    channel_id INTEGER,
    kind TEXT NOT NULL,
    rec_id TEXT NOT NULL,
    article_key TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TEXT,
    summary TEXT,
    info_src TEXT,
    url TEXT,
    attach_file TEXT,
    is_research INTEGER NOT NULL DEFAULT 0 CHECK (is_research IN (0, 1)),
    content_hash TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    run_id INTEGER,
    UNIQUE (source, scope_key, article_key)
);

CREATE TABLE IF NOT EXISTS news_bodies (
    article_id INTEGER NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    detail_type TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT,
    fetched_at TEXT NOT NULL,
    run_id INTEGER,
    PRIMARY KEY (article_id, detail_type)
);

CREATE INDEX IF NOT EXISTS idx_news_articles_published
    ON news_articles(dataset, published_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_scope
    ON news_articles(scope_key, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_code
    ON news_articles(code, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_channel
    ON news_articles(channel_id, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_last_seen
    ON news_articles(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_news_bodies_fetched
    ON news_bodies(fetched_at);

INSERT OR IGNORE INTO settings(key, value, updated_at) VALUES
    ('data.retention.news_days', '365', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('data.retention.news_body_days', '90', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('news.max_pages', '20', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
"#;
