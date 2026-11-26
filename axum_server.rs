//! axum on a multi-threaded Tokio runtime, terminating TLS through rustls, and stopping on SIGINT or SIGTERM.

use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode, Uri},
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use futures::{future::try_join_all, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};

use serveval::{
    arguments::{self, DownloadBody, Security, ServerArgs},
    payloads, security, stats,
};

const JSON: [(header::HeaderName, &str); 1] = [(header::CONTENT_TYPE, payloads::JSON)];
const OCTETS: [(header::HeaderName, &str); 1] = [(header::CONTENT_TYPE, payloads::OCTETS)];

/// axum serving the tool, context, download and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    server: ServerArgs,
}

/// The pooled client and upstream URIs of the fan-out route.
#[derive(Clone)]
struct Fanout {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    tools: Arc<[Uri]>,
}

impl Fanout {
    fn new(args: &ServerArgs) -> std::io::Result<Self> {
        // `enable_http1` below offers http/1.1 itself and panics on a config that already names an ALPN protocol.
        let config = security::tls_client_config(&args.common, &[])?;
        let scheme = if args.common.security == Security::Tls {
            "https"
        } else {
            "http"
        };
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        http.enforce_http(false);
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(Arc::unwrap_or_clone(config))
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let tools = args
            .upstreams
            .iter()
            .map(|upstream| format!("{scheme}://{upstream}/tool").parse());
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
            tools: tools.collect::<Result<_, _>>().map_err(std::io::Error::other)?,
        })
    }
}

#[derive(serde::Deserialize)]
struct Width {
    width: usize,
}

async fn raw(body: Bytes) -> impl IntoResponse {
    (OCTETS, body.slice(..payloads::REPLY_BYTES.min(body.len())))
}

async fn tool(Json(call): Json<payloads::ToolCall>) -> impl IntoResponse {
    (JSON, payloads::tool_reply(&call.id, call.check()))
}

async fn full(
    Path(name): Path<String>,
    Query(query): Query<payloads::FullQuery>,
    headers: HeaderMap,
    Json(call): Json<payloads::FullCall>,
) -> Result<impl IntoResponse, StatusCode> {
    if headers.get(header::AUTHORIZATION).map(|value| value.as_bytes()) != Some(payloads::AUTHORIZATION.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let traceparent = headers.get("traceparent").map_or(&b""[..], |value| value.as_bytes());
    let trace = payloads::trace_id(traceparent).ok_or(StatusCode::BAD_REQUEST)?;
    let check = query.after + u64::from(call.top_k);
    Ok((JSON, payloads::full_reply(&call.id, &name, trace, &query.lang, check)))
}

async fn context(body: Body) -> Result<String, StatusCode> {
    let mut chunks = body.into_data_stream();
    let mut bytes = 0u64;
    while let Some(chunk) = chunks.next().await {
        bytes += chunk.map_err(|_| StatusCode::BAD_REQUEST)?.len() as u64;
    }
    Ok(payloads::count_reply(bytes))
}

/// The first `bytes` bytes of the prepared download body, sent as one body hyper writes as the socket drains; 416 when
/// the server prepared fewer, 500 when it could not prepare any.
fn download(prepared: &DownloadBody, query: payloads::DownloadQuery) -> Result<impl IntoResponse, StatusCode> {
    let prepared = prepared.get().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let body = payloads::download(prepared, query.bytes).ok_or(StatusCode::RANGE_NOT_SATISFIABLE)?;
    Ok((OCTETS, body))
}

async fn fanout(
    State(fan): State<Fanout>,
    Query(Width { width }): Query<Width>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let call: payloads::ToolCall = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    if fan.tools.is_empty() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let expected = payloads::tool_reply(&call.id, call.check());
    let gathered = fan.tools.iter().cycle().take(width).map(|uri| {
        let request = axum::http::Request::post(uri.clone())
            .header(header::CONTENT_TYPE, payloads::JSON)
            .header(header::ACCEPT_ENCODING, "identity")
            .body(Full::new(body.clone()));
        let (client, expected) = (fan.client.clone(), &expected);
        async move {
            let request = request.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let response = client.request(request).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
            let reply = response
                .into_body()
                .collect()
                .await
                .map_err(|_| StatusCode::BAD_GATEWAY)?
                .to_bytes();
            (reply == expected).then_some(()).ok_or(StatusCode::BAD_GATEWAY)
        }
    });
    try_join_all(gathered).await?;
    Ok((JSON, payloads::fanout_reply(&call.id, width)))
}

fn main() -> std::io::Result<()> {
    let Cli { server: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "axum");
    let interface = args.single_interface("axum").clone();
    let prepared = args.download_body();
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(args.common.threads.0.get());
    runtime.enable_all().build()?.block_on(async {
        let listener = arguments::listen(&interface, args.common.port, args.admission())?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let router = Router::new()
            .route("/raw", post(raw))
            .route("/tool", post(tool))
            .route("/tool/{name}", post(full))
            .route(
                "/context",
                put(context).get(move |Query(query): Query<payloads::DownloadQuery>| {
                    std::future::ready(download(&prepared, query))
                }),
            )
            .route("/fanout", post(fanout))
            .route("/stats", get(|| async { (JSON, stats::unit_json()) }))
            .route(
                "/health",
                get(|| async { (JSON, payloads::health("axum", "hyper", None)) }),
            )
            .with_state(Fanout::new(&args)?);
        let workers = tokio::runtime::Handle::current().metrics().num_workers();
        println!(
            "Listening on {interface} port {} with {workers} Tokio workers",
            args.common.port
        );
        match security::tls_server_config(&args.common, &[b"http/1.1"])? {
            None => {
                axum::serve(listener, router)
                    .with_graceful_shutdown(arguments::stop_signal())
                    .await
            }
            Some(config) => {
                let listener = security::TlsListener::new(listener, config)?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(arguments::stop_signal())
                    .await
            }
        }
    })
}
