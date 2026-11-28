//! Read-only data browsing, exports, audit log and the MCP catalog.
//!
//! Every route is session-gated. CSV exports reuse the same query parameters as
//! their JSON counterparts so the console UI can offer one "导出" button.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use eltdx_store::StoreError;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::export::{cell, to_csv};
use crate::server::{audit, require_actor, ApiError, ConsoleState};

pub fn routes() -> Router<Arc<ConsoleState>> {
    Router::new()
        .route("/api/audit", get(audit_log))
        .route("/api/data/payloads", get(payloads))
        .route("/api/data/payloads/{id}", get(payload_one))
        .route("/api/data/klines", get(klines))
        .route("/api/data/quotes", get(quotes))
        .route("/api/data/news", get(news))
        .route("/api/mcp/tools", get(mcp_tools))
        .route("/api/mcp/tools/{name}/toggle", post(toggle_mcp_tool))
        .route("/api/mcp/calls", get(mcp_calls))
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PayloadQuery {
    dataset: Option<String>,
    code: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RangeQuery {
    code: Option<String>,
    period: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    format: Option<String>,
}

fn limit_of(value: Option<i64>, default: i64, max: i64) -> i64 {
    value.unwrap_or(default).clamp(1, max)
}

fn wants_csv(format: &Option<String>) -> bool {
    matches!(format.as_deref(), Some("csv"))
}

fn csv_response(filename: &str, body: String) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    response
}

fn store_error(error: StoreError) -> Response {
    ApiError::internal(error.to_string()).into_response()
}

async fn audit_log(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 100, 1000);
    let store = state.store_handle();
    match tokio::task::spawn_blocking(move || store.recent_audit(limit)).await {
        Ok(Ok(entries)) => Json(json!({
            "items": entries.into_iter().map(|entry| json!({
                "id": entry.id,
                "ts": entry.ts,
                "actor": entry.actor,
                "action": entry.action,
                "target": entry.target,
                "detail": entry.detail,
                "ip": entry.ip,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn payloads(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<PayloadQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 50, 1000);
    let offset = query.offset.unwrap_or(0).max(0);
    let dataset = query.dataset.clone();
    let code = query.code.clone();
    let store = state.store_handle();
    let rows = tokio::task::spawn_blocking(move || {
        store.list_payloads(dataset.as_deref(), code.as_deref(), limit, offset)
    })
    .await;
    match rows {
        Ok(Ok(rows)) => {
            if wants_csv(&query.format) {
                let body = to_csv(
                    &[
                        "id",
                        "dataset",
                        "code",
                        "trade_date",
                        "fetched_at",
                        "bytes",
                        "run_id",
                    ],
                    &rows
                        .iter()
                        .map(|row| {
                            vec![
                                row.id.to_string(),
                                row.dataset.clone(),
                                row.code.clone(),
                                row.trade_date.clone(),
                                row.fetched_at.clone(),
                                row.bytes.to_string(),
                                cell(row.run_id),
                            ]
                        })
                        .collect::<Vec<_>>(),
                );
                return csv_response("payloads.csv", body);
            }
            Json(json!({
                "items": rows,
                "limit": limit,
                "offset": offset,
            }))
            .into_response()
        }
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn payload_one(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let store = state.store_handle();
    match tokio::task::spawn_blocking(move || store.get_payload(id)).await {
        Ok(Ok(Some(payload))) => Json(json!({ "id": id, "payload": payload })).into_response(),
        Ok(Ok(None)) => ApiError::new(
            StatusCode::NOT_FOUND,
            "payload_not_found",
            format!("payload {id} does not exist"),
        )
        .into_response(),
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn klines(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<RangeQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 200, 5000);
    let offset = query.offset.unwrap_or(0).max(0);
    let code = query.code.clone();
    let period = query.period.clone();
    let store = state.store_handle();
    let rows = tokio::task::spawn_blocking(move || {
        store.list_klines(code.as_deref(), period.as_deref(), limit, offset)
    })
    .await;
    match rows {
        Ok(Ok(rows)) => {
            if wants_csv(&query.format) {
                let body = to_csv(
                    &[
                        "code",
                        "source",
                        "period",
                        "adjust",
                        "trade_date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                    ],
                    &rows
                        .iter()
                        .map(|row| {
                            vec![
                                row.code.clone(),
                                row.source.clone(),
                                row.period.clone(),
                                row.adjust.clone(),
                                row.trade_date.clone(),
                                cell(row.open),
                                cell(row.high),
                                cell(row.low),
                                cell(row.close),
                                cell(row.volume),
                                cell(row.amount),
                            ]
                        })
                        .collect::<Vec<_>>(),
                );
                return csv_response("klines.csv", body);
            }
            Json(json!({ "items": rows, "limit": limit, "offset": offset })).into_response()
        }
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn quotes(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<RangeQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 200, 5000);
    let offset = query.offset.unwrap_or(0).max(0);
    let code = query.code.clone();
    let store = state.store_handle();
    let rows = tokio::task::spawn_blocking(move || {
        store.list_quote_snapshots(code.as_deref(), limit, offset)
    })
    .await;
    match rows {
        Ok(Ok(rows)) => {
            if wants_csv(&query.format) {
                let body = to_csv(
                    &[
                        "code",
                        "ts",
                        "price",
                        "pre_close",
                        "open",
                        "high",
                        "low",
                        "volume",
                        "amount",
                    ],
                    &rows
                        .iter()
                        .map(|row| {
                            vec![
                                row.code.clone(),
                                row.ts.clone(),
                                cell(row.price),
                                cell(row.pre_close),
                                cell(row.open),
                                cell(row.high),
                                cell(row.low),
                                cell(row.volume),
                                cell(row.amount),
                            ]
                        })
                        .collect::<Vec<_>>(),
                );
                return csv_response("quotes.csv", body);
            }
            Json(json!({ "items": rows, "limit": limit, "offset": offset })).into_response()
        }
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn news(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 50, 1000);
    let offset = query.offset.unwrap_or(0).max(0);
    let store = state.store_handle();
    let rows =
        tokio::task::spawn_blocking(move || store.list_news_page(None, None, limit, offset)).await;
    match rows {
        Ok(Ok(rows)) => {
            if wants_csv(&query.format) {
                let body = to_csv(
                    &[
                        "id",
                        "published_at",
                        "source",
                        "scope_key",
                        "kind",
                        "title",
                        "url",
                    ],
                    &rows
                        .iter()
                        .map(|row| {
                            vec![
                                row.id.to_string(),
                                cell(row.published_at.clone()),
                                row.source.clone(),
                                row.scope_key.clone(),
                                row.kind.clone(),
                                row.title.clone(),
                                cell(row.url.clone()),
                            ]
                        })
                        .collect::<Vec<_>>(),
                );
                return csv_response("news.csv", body);
            }
            Json(json!({ "items": rows, "limit": limit, "offset": offset })).into_response()
        }
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn mcp_tools(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let store = state.store_handle();
    let states = tokio::task::spawn_blocking(move || store.list_mcp_tool_states()).await;
    let states = match states {
        Ok(Ok(states)) => states,
        Ok(Err(error)) => return store_error(error),
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    let schemas = eltdx_tools::mcp_registry::schemas();
    let names = eltdx_tools::mcp_registry::names();
    let tools = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let stored = states.iter().find(|state| state.tool_name == *name);
            json!({
                "name": format!("eltdx_{name}"),
                "short_name": name,
                "enabled": stored.map(|state| state.enabled).unwrap_or(true),
                "updated_at": stored.map(|state| state.updated_at.clone()),
                "schema": schemas.get(index).cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    let gateway_methods = eltdx_tools::registry::gateway_methods();
    Json(json!({
        "tools": tools,
        "gateway_methods": gateway_methods,
        "counts": {
            "tools": tools.len(),
            "gateway_methods": gateway_methods.len(),
        }
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct ToggleRequest {
    enabled: bool,
}

async fn toggle_mcp_tool(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<ToggleRequest>,
) -> Response {
    let actor = match require_actor(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    if !eltdx_tools::mcp_registry::names().contains(&name.as_str()) {
        return ApiError::new(
            StatusCode::NOT_FOUND,
            "tool_not_found",
            format!("mcp tool {name} does not exist"),
        )
        .into_response();
    }
    let store = state.store_handle();
    let toggled = tokio::task::spawn_blocking({
        let name = name.clone();
        move || store.set_mcp_tool_enabled(&name, body.enabled)
    })
    .await;
    match toggled {
        Ok(Ok(())) => {
            audit(
                &state,
                &actor,
                if body.enabled {
                    "mcp.enable"
                } else {
                    "mcp.disable"
                },
                &format!("mcp_tools/{name}"),
                json!({ "enabled": body.enabled }),
                None,
            );
            Json(json!({ "name": name, "enabled": body.enabled })).into_response()
        }
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn mcp_calls(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    if let Err(error) = require_actor(&state, &headers) {
        return error.into_response();
    }
    let limit = limit_of(query.limit, 100, 1000);
    let store = state.store_handle();
    match tokio::task::spawn_blocking(move || store.recent_mcp_calls(limit)).await {
        Ok(Ok(calls)) => Json(json!({ "items": calls })).into_response(),
        Ok(Err(error)) => store_error(error),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}
