//! HTTP surface of the local console: admin bootstrap, login, session-gated
//! APIs and an embedded data-plane proxy.
//!
//! Everything binds to loopback by default. State-changing endpoints only
//! accept `application/json`, and the session cookie is `SameSite=Strict`, so a
//! cross-site form post cannot carry the credential.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Json, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use eltdx::TdxClient;
use eltdx_store::{AdminSession, CollectionMode, NewTask, RunSummary, Store, StoreError};
use eltdx_tools::gateway::GatewayHandle;
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

use crate::auth::{
    clear_cookie, hash_token, new_token, read_cookie, session_cookie, validate_password,
    validate_username, LoginThrottle, SESSION_COOKIE,
};
use crate::collect::{execute_run_cancellable, start_run, CollectError, RunOptions};
use crate::retention::{prune, RetentionPolicy, RetentionReport};
use crate::runs::RunRegistry;

const SERVICE: &str = "eltdx-console";
const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Re-write the session row at most this often while a client keeps browsing.
const TOUCH_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Error)]
pub enum ConsoleError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("client: {0}")]
    Client(#[from] eltdx::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub bind: IpAddr,
    pub port: u16,
    pub db_path: PathBuf,
    pub session_ttl_minutes: i64,
    pub secure_cookies: bool,
}

pub struct ConsoleState {
    store: Arc<Store>,
    gateway: GatewayHandle,
    throttle: Mutex<LoginThrottle>,
    touches: Mutex<HashMap<String, Instant>>,
    runs: RunRegistry,
    session_ttl_minutes: i64,
    secure_cookies: bool,
    started: Instant,
}

impl ConsoleState {
    pub fn new(
        store: Arc<Store>,
        gateway: GatewayHandle,
        session_ttl_minutes: i64,
        secure_cookies: bool,
    ) -> Self {
        Self {
            store,
            gateway,
            throttle: Mutex::new(LoginThrottle::default()),
            touches: Mutex::new(HashMap::new()),
            runs: RunRegistry::default(),
            session_ttl_minutes: session_ttl_minutes.max(1),
            secure_cookies,
            started: Instant::now(),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Cloneable handle so background collection shares the data plane.
    pub fn gateway(&self) -> GatewayHandle {
        self.gateway.clone()
    }

    pub fn store_handle(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    pub fn runs(&self) -> &RunRegistry {
        &self.runs
    }
}

pub fn router(state: Arc<ConsoleState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/setup", post(setup))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/overview", get(overview))
        .route("/api/rpc", post(rpc))
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route("/api/tasks/{id}/toggle", post(toggle_task))
        .route("/api/tasks/{id}/run", post(run_task_now))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/maintenance/prune", post(prune_now))
        .route("/api/maintenance/policy", get(retention_policy))
        .merge(crate::api_data::routes())
        .with_state(state)
}

/// Builds the shared state, binds the listener and serves until shutdown.
pub async fn run(config: ConsoleConfig) -> Result<(), ConsoleError> {
    let store = Arc::new(Store::open(&config.db_path)?);
    store.purge_expired_sessions()?;
    if let Some(port) = store.get_setting("console.port")? {
        if port.parse::<u16>().ok() != Some(config.port) {
            store.set_setting("console.port", &config.port.to_string())?;
        }
    }
    let client = TdxClient::builder().build()?;
    let state = Arc::new(ConsoleState::new(
        store,
        GatewayHandle::new(client),
        config.session_ttl_minutes,
        config.secure_cookies,
    ));
    let app = router(Arc::clone(&state));
    crate::scheduler::spawn(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind(SocketAddr::new(config.bind, config.port)).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

async fn shutdown_signal() {
    if tokio::signal::ctrl_c().await.is_err() {
        // Without a signal handler the process simply serves until killed.
        std::future::pending::<()>().await;
    }
}

// ---------------------------------------------------------------- handlers

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health(State(state): State<Arc<ConsoleState>>) -> Response {
    let store = Arc::clone(&state.store);
    let probe = tokio::task::spawn_blocking(move || {
        let has_admin = store.has_admin()?;
        let schema_version = store.schema_version()?;
        Ok::<_, StoreError>((has_admin, schema_version))
    })
    .await;
    let (has_admin, schema_version) = match probe {
        Ok(Ok(values)) => values,
        Ok(Err(error)) => return ApiError::internal(error.to_string()).into_response(),
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    Json(json!({
        "ok": true,
        "service": SERVICE,
        "version": VERSION,
        "schema_version": schema_version,
        "has_admin": has_admin,
        "uptime_ms": state.started.elapsed().as_millis() as u64,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct Credentials {
    username: String,
    password: String,
}

async fn setup(
    State(state): State<Arc<ConsoleState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<Credentials>,
) -> Response {
    let username = body.username.trim().to_owned();
    if let Err(message) = validate_username(&username) {
        return ApiError::bad_request(message).into_response();
    }
    if let Err(message) = validate_password(&body.password) {
        return ApiError::bad_request(message).into_response();
    }
    let store = Arc::clone(&state.store);
    let password = body.password;
    let created = tokio::task::spawn_blocking({
        let username = username.clone();
        move || -> Result<Option<i64>, StoreError> {
            if store.has_admin()? {
                return Ok(None);
            }
            store.create_admin(&username, &password).map(Some)
        }
    })
    .await;
    let user_id = match created {
        Ok(Ok(Some(id))) => id,
        Ok(Ok(None)) => {
            return ApiError::new(
                StatusCode::CONFLICT,
                "already_initialised",
                "an administrator already exists",
            )
            .into_response()
        }
        Ok(Err(error)) => return ApiError::internal(error.to_string()).into_response(),
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    audit(
        &state,
        &username,
        "auth.setup",
        "admin_users",
        json!({ "user_id": user_id }),
        peer_ip(&peer, &headers),
    );
    issue_session(&state, user_id, &username, StatusCode::CREATED)
}

async fn login(
    State(state): State<Arc<ConsoleState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<Credentials>,
) -> Response {
    let username = body.username.trim().to_owned();
    let ip = peer_ip(&peer, &headers);
    let throttle_key = format!("{username}@{}", ip.clone().unwrap_or_default());
    if let Some(wait) = lock_throttle(&state).retry_after(&throttle_key) {
        return ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_attempts",
            format!("try again in {} seconds", wait.as_secs().max(1)),
        )
        .into_response();
    }

    let store = Arc::clone(&state.store);
    let password = body.password;
    let lookup = {
        let username = username.clone();
        tokio::task::spawn_blocking(move || store.verify_admin(&username, &password)).await
    };
    let user = match lookup {
        Ok(Ok(Some(user))) => user,
        Ok(Ok(None)) => {
            lock_throttle(&state).record_failure(&throttle_key);
            audit(
                &state,
                &username,
                "auth.login_failed",
                "admin_sessions",
                json!({ "reason": "invalid_credentials" }),
                ip,
            );
            return ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid_credentials",
                "username or password is incorrect",
            )
            .into_response();
        }
        Ok(Err(error)) => return ApiError::internal(error.to_string()).into_response(),
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    lock_throttle(&state).record_success(&throttle_key);
    audit(
        &state,
        &user.username,
        "auth.login",
        "admin_sessions",
        json!({ "user_id": user.id }),
        ip,
    );
    issue_session(&state, user.id, &user.username, StatusCode::OK)
}

async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Some(session) = read_session(&state, &headers) {
        let store = Arc::clone(&state.store);
        let token_hash = hash_token(&session.token);
        let _ = tokio::task::spawn_blocking(move || store.delete_session(&token_hash)).await;
        audit(
            &state,
            &session.admin.user.username,
            "auth.logout",
            "admin_sessions",
            json!({}),
            None,
        );
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Ok(value) = clear_cookie().parse() {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    response
}

async fn me(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    Json(json!({
        "user": {
            "username": session.admin.user.username,
            "role": session.admin.user.role,
            "created_at": session.admin.user.created_at,
        },
        "expires_at": session.admin.expires_at,
        "created_at": session.admin.created_at,
        "idle_timeout_minutes": state.session_ttl_minutes,
    }))
    .into_response()
}

async fn overview(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Err(error) = require_session(&state, &headers) {
        return error.into_response();
    }
    let store = Arc::clone(&state.store);
    let snapshot = tokio::task::spawn_blocking(move || -> Result<Value, StoreError> {
        let tasks = store.list_tasks()?;
        let enabled = tasks.iter().filter(|task| task.enabled).count();
        let channels = store.list_news_channels(false)?.len();
        Ok(json!({
            "tasks": { "total": tasks.len(), "enabled": enabled },
            "sessions": { "active": store.count_active_sessions()? },
            "news": {
                "channels": channels,
                "articles": store.count_news_articles()?,
                "bodies": store.count_news_bodies()?,
            },
            "storage": {
                "payloads": store.count_payloads()?,
                "klines": store.count_klines()?,
                "quotes": store.count_quote_snapshots()?,
            },
            "changes": { "latest_seq": store.latest_change_seq()? },
            "runs": store.recent_runs(5)?.into_iter().map(run_summary_json).collect::<Vec<_>>(),
            "audit": store.recent_audit(10)?.into_iter().map(|entry| json!({
                "id": entry.id,
                "ts": entry.ts,
                "actor": entry.actor,
                "action": entry.action,
                "target": entry.target,
            })).collect::<Vec<_>>(),
        }))
    })
    .await;
    match snapshot {
        Ok(Ok(payload)) => Json(payload).into_response(),
        Ok(Err(error)) => ApiError::internal(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    method: String,
    #[serde(default)]
    params: Value,
}

async fn rpc(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Json(body): Json<RpcRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    if body.method.trim().is_empty() {
        return ApiError::bad_request("method must not be empty").into_response();
    }
    match state.gateway.call(&body.method, body.params).await {
        Ok(result) => Json(json!({ "result": result })).into_response(),
        Err(error) => {
            audit(
                &state,
                &session.admin.user.username,
                "rpc.error",
                &body.method,
                json!({ "error": error.to_string() }),
                None,
            );
            ApiError::new(StatusCode::BAD_GATEWAY, "gateway_error", error.to_string())
                .into_response()
        }
    }
}

// ------------------------------------------------------------ task endpoints

fn default_port() -> u16 {
    7709
}

fn default_mode() -> String {
    "incremental".to_owned()
}

fn default_shard_size() -> i64 {
    100
}

fn default_timeout_ms() -> i64 {
    8_000
}

fn default_retry() -> i64 {
    2
}

fn default_concurrency() -> i64 {
    1
}

fn default_schedule() -> String {
    "manual".to_owned()
}

#[derive(Debug, Deserialize)]
struct TaskRequest {
    name: String,
    #[serde(default = "default_port")]
    port: u16,
    dataset: String,
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default = "default_mode")]
    mode: String,
    watermark_type: Option<String>,
    #[serde(default)]
    overlap_days: i64,
    #[serde(default = "default_shard_size")]
    shard_size: i64,
    max_pages: Option<i64>,
    max_records: Option<i64>,
    #[serde(default = "default_schedule")]
    schedule_kind: String,
    schedule_expr: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: i64,
    #[serde(default = "default_retry")]
    retry: i64,
    #[serde(default = "default_concurrency")]
    concurrency: i64,
    retention_days: Option<i64>,
}

impl TaskRequest {
    fn into_task(self) -> Result<NewTask, ApiError> {
        let mode = CollectionMode::from_str(self.mode.trim()).map_err(|_| {
            ApiError::bad_request(format!(
                "mode must be one of full, incremental, backfill, refresh (got {})",
                self.mode
            ))
        })?;
        if !self.params.is_object() && !self.params.is_null() {
            return Err(ApiError::bad_request("params must be a JSON object"));
        }
        let mut task = NewTask::new(
            self.name,
            self.port,
            self.dataset,
            self.method,
            self.params,
            mode,
        );
        task.watermark_type = self.watermark_type;
        task.overlap_days = self.overlap_days;
        task.shard_size = self.shard_size;
        task.max_pages = self.max_pages;
        task.max_records = self.max_records;
        task.schedule_kind = self.schedule_kind;
        task.schedule_expr = self.schedule_expr;
        task.timeout_ms = self.timeout_ms;
        task.retry = self.retry;
        task.concurrency = self.concurrency;
        task.retention_days = self.retention_days;
        Ok(task)
    }
}

async fn list_tasks(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Err(error) = require_session(&state, &headers) {
        return error.into_response();
    }
    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking(move || store.list_tasks()).await {
        Ok(Ok(tasks)) => Json(json!({ "tasks": tasks })).into_response(),
        Ok(Err(error)) => ApiError::internal(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn create_task(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Json(body): Json<TaskRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    let task = match body.into_task() {
        Ok(task) => task,
        Err(error) => return error.into_response(),
    };
    let store = Arc::clone(&state.store);
    let created = tokio::task::spawn_blocking(move || store.create_task(&task)).await;
    match created {
        Ok(Ok(id)) => {
            audit(
                &state,
                &session.admin.user.username,
                "task.create",
                &format!("collection_tasks/{id}"),
                json!({ "id": id }),
                None,
            );
            (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
        }
        Ok(Err(error)) => ApiError::bad_request(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct ToggleRequest {
    enabled: bool,
}

async fn toggle_task(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<ToggleRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    let store = Arc::clone(&state.store);
    let outcome = tokio::task::spawn_blocking(move || -> Result<bool, StoreError> {
        if store.get_task(id)?.is_none() {
            return Ok(false);
        }
        store.set_task_enabled(id, body.enabled)?;
        Ok(true)
    })
    .await;
    match outcome {
        Ok(Ok(true)) => {
            audit(
                &state,
                &session.admin.user.username,
                if body.enabled {
                    "task.enable"
                } else {
                    "task.disable"
                },
                &format!("collection_tasks/{id}"),
                json!({ "enabled": body.enabled }),
                None,
            );
            Json(json!({ "id": id, "enabled": body.enabled })).into_response()
        }
        Ok(Ok(false)) => ApiError::new(
            StatusCode::NOT_FOUND,
            "task_not_found",
            format!("task {id} does not exist"),
        )
        .into_response(),
        Ok(Err(error)) => ApiError::internal(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn run_task_now(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    let task = {
        let store = Arc::clone(&state.store);
        match tokio::task::spawn_blocking(move || store.get_task(id)).await {
            Ok(Ok(Some(task))) => task,
            Ok(Ok(None)) => {
                return ApiError::new(
                    StatusCode::NOT_FOUND,
                    "task_not_found",
                    format!("task {id} does not exist"),
                )
                .into_response()
            }
            Ok(Err(error)) => return ApiError::internal(error.to_string()).into_response(),
            Err(error) => return ApiError::internal(error.to_string()).into_response(),
        }
    };
    if !task.enabled {
        return ApiError::new(
            StatusCode::CONFLICT,
            "task_disabled",
            format!("task {id} is disabled"),
        )
        .into_response();
    }
    if !state.runs.claim_task(id) {
        return ApiError::new(
            StatusCode::CONFLICT,
            "task_running",
            format!("task {id} is already running"),
        )
        .into_response();
    }

    let run_id = match start_run(
        &state.store,
        &task,
        &RunOptions {
            trigger: "console".to_owned(),
            resumed_from: None,
        },
    )
    .await
    {
        Ok(run_id) => run_id,
        Err(error) => {
            state.runs.release_task(id);
            return collect_error(error);
        }
    };
    let flag = state.runs.register_run(run_id);

    let background = Arc::clone(&state);
    let pending = task.clone();
    tokio::spawn(async move {
        let store = Arc::clone(&background.store);
        let gateway = background.gateway();
        let task_id = pending.id;
        if let Err(error) = execute_run_cancellable(store, gateway, pending, run_id, flag).await {
            let store = Arc::clone(&background.store);
            let message = error.to_string();
            let _ = tokio::task::spawn_blocking(move || {
                store.append_audit(
                    "system",
                    "task.run_error",
                    &format!("collection_runs/{run_id}"),
                    &json!({ "error": message }),
                    None,
                )
            })
            .await;
        }
        background.runs.finish_run(run_id);
        background.runs.release_task(task_id);
    });

    audit(
        &state,
        &session.admin.user.username,
        "task.run",
        &format!("collection_tasks/{id}"),
        json!({ "run_id": run_id }),
        None,
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({ "run_id": run_id, "task_id": id, "status": "running" })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct RunsQuery {
    limit: Option<i64>,
}

async fn list_runs(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<RunsQuery>,
) -> Response {
    if let Err(error) = require_session(&state, &headers) {
        return error.into_response();
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking(move || store.recent_runs(limit)).await {
        Ok(Ok(runs)) => Json(json!({ "runs": runs })).into_response(),
        Ok(Err(error)) => ApiError::internal(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

fn collect_error(error: CollectError) -> Response {
    match error {
        CollectError::TaskNotFound(id) => ApiError::new(
            StatusCode::NOT_FOUND,
            "task_not_found",
            format!("task {id} does not exist"),
        )
        .into_response(),
        CollectError::TaskDisabled { task_id } => ApiError::new(
            StatusCode::CONFLICT,
            "task_disabled",
            format!("task {task_id} is disabled"),
        )
        .into_response(),
        CollectError::Store(error) => ApiError::internal(error.to_string()).into_response(),
        CollectError::MissingMethod { task_id } => {
            ApiError::bad_request(format!("task {task_id} has no gateway method")).into_response()
        }
    }
}

async fn cancel_run(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    if !state.runs.cancel_run(id) {
        return ApiError::new(
            StatusCode::CONFLICT,
            "run_not_active",
            format!("run {id} is not active in this process"),
        )
        .into_response();
    }
    audit(
        &state,
        &session.admin.user.username,
        "run.cancel",
        &format!("collection_runs/{id}"),
        json!({}),
        None,
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({ "run_id": id, "cancel": "requested" })),
    )
        .into_response()
}

async fn retention_policy(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Err(error) = require_session(&state, &headers) {
        return error.into_response();
    }
    let store = state.store_handle();
    match tokio::task::spawn_blocking(move || RetentionPolicy::from_settings(&store)).await {
        Ok(Ok(policy)) => Json(json!({ "policy": policy })).into_response(),
        Ok(Err(error)) => ApiError::internal(error.to_string()).into_response(),
        Err(error) => ApiError::internal(error.to_string()).into_response(),
    }
}

async fn prune_now(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    let store = state.store_handle();
    let policy =
        match tokio::task::spawn_blocking(move || RetentionPolicy::from_settings(&store)).await {
            Ok(Ok(policy)) => policy,
            Ok(Err(error)) => return ApiError::internal(error.to_string()).into_response(),
            Err(error) => return ApiError::internal(error.to_string()).into_response(),
        };
    let report: RetentionReport = match prune(&state.store, policy).await {
        Ok(report) => report,
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    audit(
        &state,
        &session.admin.user.username,
        "maintenance.prune",
        "retention",
        json!({ "report": report, "mode": "manual" }),
        None,
    );
    Json(json!({ "policy": policy, "report": report })).into_response()
}

fn run_summary_json(run: RunSummary) -> Value {
    json!({
        "id": run.id,
        "task_id": run.task_id,
        "trigger": run.trigger,
        "mode": run.mode,
        "status": run.status,
        "rows": run.rows,
        "bytes": run.bytes,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_ms": run.duration_ms,
        "error": run.error,
    })
}

// ------------------------------------------------------------------ helpers

struct SessionHandle {
    token: String,
    admin: AdminSession,
}

fn session_token(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(header::COOKIE)?.to_str().ok()?;
    read_cookie(header, SESSION_COOKIE).map(str::to_owned)
}

fn read_session(state: &ConsoleState, headers: &HeaderMap) -> Option<SessionHandle> {
    let token = session_token(headers)?;
    let admin = state.store.resolve_session(&hash_token(&token)).ok()??;
    Some(SessionHandle { token, admin })
}

fn require_session(state: &ConsoleState, headers: &HeaderMap) -> Result<SessionHandle, ApiError> {
    let session = read_session(state, headers).ok_or_else(ApiError::unauthorized)?;
    let token_hash = hash_token(&session.token);
    let touch = {
        let mut touches = state
            .touches
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match touches.get(&token_hash) {
            Some(last) if last.elapsed() < TOUCH_INTERVAL => false,
            _ => {
                touches.insert(token_hash.clone(), Instant::now());
                true
            }
        }
    };
    if touch {
        let _ = state
            .store
            .touch_session(&token_hash, state.session_ttl_minutes);
    }
    Ok(session)
}

fn issue_session(
    state: &ConsoleState,
    user_id: i64,
    username: &str,
    status: StatusCode,
) -> Response {
    let token = new_token();
    let token_hash = hash_token(&token);
    let created = state
        .store
        .create_session(user_id, &token_hash, state.session_ttl_minutes);
    let expires_at = match created {
        Ok(expires_at) => expires_at,
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    let mut response = (
        status,
        Json(json!({
            "user": { "username": username, "role": "admin" },
            "expires_at": expires_at,
        })),
    )
        .into_response();
    match session_cookie(&token, state.session_ttl_minutes, state.secure_cookies).parse() {
        Ok(value) => {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
        Err(_) => return ApiError::internal("failed to build session cookie").into_response(),
    }
    response
}

/// Session gate shared with the data/catalog routes; returns the actor name.
pub(crate) fn require_actor(state: &ConsoleState, headers: &HeaderMap) -> Result<String, ApiError> {
    let session = require_session(state, headers)?;
    Ok(session.admin.user.username)
}

pub(crate) fn audit(
    state: &ConsoleState,
    actor: &str,
    action: &str,
    target: &str,
    detail: Value,
    ip: Option<String>,
) {
    let store = Arc::clone(&state.store);
    let actor = actor.to_owned();
    let action = action.to_owned();
    let target = target.to_owned();
    // Audit writes must never fail a request, but they stay best-effort only
    // for logging; the caller already owns the authoritative result.
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn_blocking(move || {
            let _ = store.append_audit(&actor, &action, &target, &detail, ip.as_deref());
        });
    }
}

fn lock_throttle(state: &ConsoleState) -> std::sync::MutexGuard<'_, LoginThrottle> {
    state
        .throttle
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

fn peer_ip(peer: &SocketAddr, headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(|| Some(peer.ip().to_string()))
}

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "sign in to continue",
        )
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": { "code": self.code, "message": self.message } })),
        )
            .into_response()
    }
}

const INDEX_HTML: &str = include_str!("web/index.html");
