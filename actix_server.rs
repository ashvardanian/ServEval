//! actix-web with its own workers, terminating TLS through rustls, and stopping gracefully on SIGINT or SIGTERM.

use std::sync::Arc;

use actix_web::{
    http::{
        header::{self, ContentType},
        KeepAlive,
    },
    web, App, HttpRequest, HttpResponse, HttpServer,
};
use futures::{future::try_join_all, StreamExt};

use serveval::{
    arguments::{self, DownloadBody, Security, ServerArgs},
    payloads, security, stats,
};

/// actix-web serving the tool, context, download and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    server: ServerArgs,
}

/// The fan-out route's per-worker client and upstream URLs.
struct Fanout {
    client: awc::Client,
    tools: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Width {
    width: usize,
}

async fn raw(body: web::Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::octet_stream())
        .body(body.slice(..payloads::REPLY_BYTES.min(body.len())))
}

async fn tool(call: web::Json<payloads::ToolCall>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(payloads::tool_reply(&call.id, call.check()))
}

async fn full(
    name: web::Path<String>,
    query: web::Query<payloads::FullQuery>,
    request: HttpRequest,
    call: web::Json<payloads::FullCall>,
) -> HttpResponse {
    let headers = request.headers();
    if headers.get(header::AUTHORIZATION).map(|value| value.as_bytes()) != Some(payloads::AUTHORIZATION.as_bytes()) {
        return HttpResponse::Unauthorized().finish();
    }
    let traceparent = headers.get("traceparent").map_or(&b""[..], |value| value.as_bytes());
    let Some(trace) = payloads::trace_id(traceparent) else {
        return HttpResponse::BadRequest().finish();
    };
    let check = query.after + u64::from(call.top_k);
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(payloads::full_reply(&call.id, &name, trace, &query.lang, check))
}

async fn context(mut body: web::Payload) -> actix_web::Result<String> {
    let mut bytes = 0u64;
    while let Some(chunk) = body.next().await {
        bytes += chunk?.len() as u64;
    }
    Ok(payloads::count_reply(bytes))
}

/// The first `bytes` bytes of the prepared download body, which actix writes as the socket drains; 416 when the server
/// prepared fewer, 500 when it could not prepare any.
async fn download(prepared: web::Data<DownloadBody>, query: web::Query<payloads::DownloadQuery>) -> HttpResponse {
    let Some(prepared) = prepared.get() else {
        return HttpResponse::InternalServerError().finish();
    };
    match payloads::download(prepared, query.bytes) {
        Some(body) => HttpResponse::Ok().content_type(ContentType::octet_stream()).body(body),
        None => HttpResponse::RangeNotSatisfiable().finish(),
    }
}

async fn fanout(fan: web::Data<Fanout>, query: web::Query<Width>, body: web::Bytes) -> HttpResponse {
    let Ok(call) = serde_json::from_slice::<payloads::ToolCall>(&body) else {
        return HttpResponse::BadRequest().finish();
    };
    if fan.tools.is_empty() {
        return HttpResponse::ServiceUnavailable().finish();
    }
    let expected = payloads::tool_reply(&call.id, call.check());
    let gathered = fan.tools.iter().cycle().take(query.width).map(|url| {
        let request = fan
            .client
            .post(url)
            .insert_header(ContentType::json())
            .insert_header((header::ACCEPT_ENCODING, "identity"))
            .send_body(body.clone());
        let expected = &expected;
        async move {
            let mut response = request.await.map_err(drop)?;
            let reply = response.body().await.map_err(drop)?;
            (reply == expected).then_some(()).ok_or(())
        }
    });
    match try_join_all(gathered).await {
        Ok(_) => HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(payloads::fanout_reply(&call.id, query.width)),
        Err(()) => HttpResponse::BadGateway().finish(),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let Cli { server: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "actix");
    let interface = args.single_interface("actix").clone();
    let workers = args.common.threads.0.get();
    let prepared = web::Data::from(args.download_body());
    let client_config = security::tls_client_config(&args.common, &[b"http/1.1"])?;
    let scheme = if args.common.security == Security::Tls {
        "https"
    } else {
        "http"
    };
    let tools: Vec<String> = args
        .upstreams
        .iter()
        .map(|upstream| format!("{scheme}://{upstream}/tool"))
        .collect();
    let server = HttpServer::new(move || {
        // Unlimited like the Python servers' httpx; awc's default caps a worker at 100 upstream connections.
        let connector = awc::Connector::new().limit(0).rustls_0_23(Arc::clone(&client_config));
        let client = awc::Client::builder()
            .connector(connector)
            .disable_timeout()
            .no_default_headers()
            .finish();
        App::new()
            .app_data(web::Data::new(Fanout {
                client,
                tools: tools.clone(),
            }))
            .route("/raw", web::post().to(raw))
            .route("/tool", web::post().to(tool))
            .route("/tool/{name}", web::post().to(full))
            .app_data(prepared.clone())
            .route("/context", web::put().to(context))
            .route("/context", web::get().to(download))
            .route("/fanout", web::post().to(fanout))
            .route(
                "/stats",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .content_type(ContentType::json())
                        .body(stats::unit_json())
                }),
            )
            .route(
                "/health",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .content_type(ContentType::json())
                        .body(payloads::health("actix", "awc", None))
                }),
            )
    })
    .workers(workers)
    // actix's default 5s idle timeout would reconnect open-loop rows mid-window, unlike the other servers.
    .keep_alive(KeepAlive::Os)
    // actix's default of 25K connections per worker would cap the 1M shape; twice the even share leaves headroom, as
    // the kernel spreads connections over the workers unevenly.
    .max_connections(2 * args.admission().div_ceil(workers));
    let listener = arguments::listen(&interface, args.common.port, args.admission())?;
    let server = match security::tls_server_config(&args.common, &[b"http/1.1"])? {
        None => server.listen(listener)?,
        Some(config) => server.listen_rustls_0_23(listener, Arc::unwrap_or_clone(config))?,
    };
    println!(
        "Listening on {interface} port {} with {workers} actix workers",
        args.common.port
    );
    server.run().await
}
