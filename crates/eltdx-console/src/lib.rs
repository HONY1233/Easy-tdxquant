//! Collection orchestration for the Easy-tdxquant local console.
//!
//! The first slice normalizes 7615 news pages into storage inputs, decides
//! whether a paginated run should continue, and drives that page loop through
//! an injectable RPC boundary. Network clients stay outside this crate so the
//! logic can be tested without a live server.

pub mod news;
pub mod news_collect;
pub mod news_gateway;

pub use news::{
    channel_scopes, decide_page, home_scopes, parse_news_page, NewsPage, NewsScope, PageDecision,
    StopReason, HOME_TABS,
};
pub use news_collect::{
    collect_news_scope, NewsCollectError, NewsRpc, NewsRunOptions, NewsRunReport,
};
pub use news_gateway::GatewayNewsRpc;
