//! Collection orchestration and local admin console for Easy-tdxquant.
//!
//! The news slice normalizes 7615 news pages into storage inputs, decides
//! whether a paginated run should continue, and drives that page loop through
//! an injectable RPC boundary. The server slice adds the loopback admin
//! surface: administrator bootstrap, argon2 login, session cookies, audit
//! entries and a session-gated proxy onto the shared data plane.

pub mod api_data;
pub mod auth;
pub mod clock;
pub mod collect;
pub mod export;
pub mod news;
pub mod news_collect;
pub mod news_gateway;
pub mod retention;
pub mod rpc;
pub mod runs;
pub mod schedule;
pub mod scheduler;
pub mod server;

pub use auth::{
    clear_cookie, hash_token, new_token, read_cookie, session_cookie, validate_password,
    validate_username, LoginThrottle, DEFAULT_SESSION_TTL_MINUTES, SESSION_COOKIE,
};
pub use clock::{format_iso_utc, now_epoch, parse_iso_utc, shanghai_date};
pub use collect::{
    build_shards, count_rows, execute_run, execute_run_cancellable, run_task, start_run,
    CollectError, Rpc, RunOptions, RunReport,
};
pub use news::{
    channel_scopes, decide_page, home_scopes, parse_news_page, NewsPage, NewsScope, PageDecision,
    StopReason, HOME_TABS,
};
pub use news_collect::{
    collect_news_scope, NewsCollectError, NewsRpc, NewsRunOptions, NewsRunReport,
};
pub use news_gateway::GatewayNewsRpc;
pub use retention::{prune, prune_blocking, RetentionPolicy, RetentionReport};
pub use runs::RunRegistry;
pub use schedule::{is_due, next_due, Schedule, ScheduleError};
pub use server::{router, run, ConsoleConfig, ConsoleError, ConsoleState};
