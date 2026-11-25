//! hyper's connection-level HTTP/1.1 client: every connection opened up front from the paired interface, each slot
//! cycling through its own connections, or opening one per call in the handshake phase.

use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::{client::conn::http1, header, Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio_rustls::TlsConnector;

use serveval::{
    arguments::{self, BetweenRuns, ClientArgs, Phase, Run, Workload},
    payloads::{self, Exchange, ToolParts},
    results::{Pair, Report, Sample},
    security,
    stats::{self, Failure, Slot, Tally, UnitStats, Window},
};

/// hyper driving an HTTP server through the tool, context and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    client: ClientArgs,
}

type Sender = http1::SendRequest<Full<Bytes>>;

/// Everything every call of the process shares.
struct Shared {
    args: ClientArgs,
    targets: Vec<SocketAddr>,
    tls: Option<TlsConnector>,
    ring: Vec<ToolParts>,
}

/// One configuration's calls: the context upload, or one exchange per ring entry.
struct Load {
    shared: Arc<Shared>,
    run: Run,
    exchanges: Vec<Exchange>,
    context: Bytes,
}

async fn open(shared: &Shared, target: usize, index: usize) -> io::Result<Sender> {
    let address = shared.targets[target];
    let stream = security::connect(shared.args.interface_of(target), index, address, shared.tls.as_ref()).await?;
    let (sender, connection) = http1::handshake(TokioIo::new(stream)).await.map_err(io::Error::other)?;
    tokio::spawn(connection);
    Ok(sender)
}

/// Sends one request and reads the whole reply.
async fn exchange(sender: &mut Sender, request: Request<Full<Bytes>>) -> Result<(StatusCode, Bytes), Failure> {
    let failure = |error: hyper::Error| Failure::of(&error);
    sender.ready().await.map_err(failure)?;
    let response = sender.send_request(request).await.map_err(failure)?;
    let status = response.status();
    Ok((
        status,
        response.into_body().collect().await.map_err(failure)?.to_bytes(),
    ))
}

/// The request of call `sequence`, its body's length, and the reply it must get back.
fn request(load: &Load, target: usize, sequence: usize) -> (Request<Full<Bytes>>, u64, Bytes) {
    let mut builder = Request::builder()
        .header(header::HOST, load.shared.targets[target].to_string())
        .header(header::CONTENT_TYPE, payloads::JSON)
        .header(header::ACCEPT_ENCODING, "identity");
    if load.run.phase == Phase::Handshake {
        builder = builder.header(header::CONNECTION, "close");
    }
    let (body, reply) = match load.run.workload {
        Workload::Context => {
            builder = builder.method(Method::PUT).uri("/context");
            let reply = payloads::count_reply(load.context.len() as u64);
            (load.context.clone(), Bytes::from(reply))
        }
        _ => {
            let exchange = &load.exchanges[sequence % payloads::RING];
            builder = builder.method(Method::POST).uri(&exchange.path);
            if let Some(traceparent) = &exchange.traceparent {
                builder = builder
                    .header(header::AUTHORIZATION, payloads::AUTHORIZATION)
                    .header("traceparent", traceparent);
            }
            (exchange.body.clone(), exchange.reply.clone())
        }
    };
    let length = body.len() as u64;
    (
        builder.body(Full::new(body)).expect("the request parts are valid"),
        length,
        reply,
    )
}

/// One verified call: the body bytes sent and received, or why it failed.
async fn call(load: &Load, sender: &mut Sender, target: usize, sequence: usize) -> Result<(u64, u64), Failure> {
    let (request, sent, expected) = request(load, target, sequence);
    let (status, reply) = exchange(sender, request).await?;
    match (status == StatusCode::OK, reply == expected) {
        (false, _) => Err(Failure::Status),
        (true, false) => Err(Failure::WrongReply),
        (true, true) => Ok((sent, reply.len() as u64)),
    }
}

async fn get(shared: &Shared, target: usize, path: &str) -> Option<Bytes> {
    let mut sender = open(shared, target, 0).await.ok()?;
    let request = Request::get(path)
        .header(header::HOST, shared.targets[target].to_string())
        .header(header::ACCEPT_ENCODING, "identity");
    let (status, reply) = exchange(&mut sender, request.body(Full::default()).ok()?).await.ok()?;
    (status == StatusCode::OK).then_some(reply)
}

async fn server_stats(shared: &Shared) -> UnitStats {
    let mut all = Vec::new();
    for target in 0..shared.targets.len() {
        let reply = get(shared, target, "/stats").await;
        all.push(
            reply
                .and_then(|reply| serde_json::from_slice(&reply).ok())
                .unwrap_or_default(),
        );
    }
    UnitStats::sum(&all)
}

/// Takes every connection from the open ones in `kept` and new ones, handing slot `target + i * targets` the
/// connections `j` of that target with `j % slots == i`, so each connection carries at most one call at a time.
async fn open_all(load: &Load, kept: &mut [Vec<Sender>]) -> io::Result<Vec<Vec<Sender>>> {
    let (shared, run) = (&load.shared, &load.run);
    let targets = shared.targets.len();
    let mut slots: Vec<Vec<Sender>> = (0..run.in_flight).map(|_| Vec::new()).collect();
    for target in 0..targets {
        let slots_here = arguments::share(run.in_flight, target, targets);
        if slots_here == 0 {
            continue;
        }
        let count = arguments::share(run.connections, target, targets);
        let mut senders: Vec<Sender> = kept[target]
            .drain(..)
            .filter(|sender| !sender.is_closed())
            .take(count)
            .collect();
        let opened = futures::stream::iter(senders.len()..count)
            .map(|index| open(shared, target, index))
            .buffered(1024);
        for sender in opened.collect::<Vec<io::Result<Sender>>>().await {
            senders.push(sender?);
        }
        for (index, sender) in senders.into_iter().enumerate() {
            slots[target + (index % slots_here) * targets].push(sender);
        }
    }
    Ok(slots)
}

/// Runs one slot, returning its tally and handing its connections back, open until the caller samples the server.
async fn drive(load: Arc<Load>, window: Window, slot: usize, mut senders: Vec<Sender>) -> (Tally, Vec<Sender>) {
    let target = slot % load.shared.targets.len();
    let mut schedule = Slot::new(window, slot);
    let mut turn = 0;
    while let Some((intended, sequence)) = schedule.next().await {
        let outcome = match load.run.phase {
            Phase::Steady => {
                turn += 1;
                let index = turn % senders.len();
                let sender = &mut senders[index];
                call(&load, sender, target, sequence).await
            }
            Phase::Handshake => match open(&load.shared, target, sequence).await {
                Ok(mut sender) => call(&load, &mut sender, target, sequence).await,
                Err(error) => Err(Failure::of_io(&error)),
            },
        };
        schedule.record(intended, outcome);
    }
    (schedule.tally, senders)
}

async fn measure(args: ClientArgs) -> io::Result<()> {
    let shared = Arc::new(Shared {
        targets: args
            .targets
            .iter()
            .map(|&address| SocketAddr::new(address, args.common.port))
            .collect(),
        tls: security::tls_connector(&args.common, &[b"http/1.1"])?,
        ring: payloads::tool_ring(args.common.seed.into()),
        args,
    });
    let args = &shared.args;
    let health = get(&shared, 0, "/health")
        .await
        .ok_or_else(|| io::Error::other("no /health reply"))?;
    let pair: Pair = serde_json::from_slice(&health)?;
    let runs = args.runs(pair.batching, false);
    let mut report = Report::new(args, pair);
    let targets = shared.targets.len();
    let mut kept: Vec<Vec<Sender>> = (0..targets).map(|_| Vec::new()).collect();
    for (number, run) in runs.into_iter().enumerate() {
        let context = match run.workload {
            Workload::Context => payloads::context_body(args.common.seed.into(), run.body_bytes),
            _ => Bytes::new(),
        };
        let exchanges = match run.workload {
            Workload::Context => Vec::new(),
            _ => payloads::exchanges(&shared.ring, &run, args.width),
        };
        let load = Arc::new(Load {
            shared: shared.clone(),
            run,
            exchanges,
            context,
        });
        let probe = match open(&shared, 0, 0).await {
            Ok(mut sender) => exchange(&mut sender, request(&load, 0, 0).0)
                .await
                .map(|(status, _)| status)
                .ok(),
            Err(_) => None,
        };
        if let Some(StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED) = probe {
            report.unsupported(&run);
            continue;
        }
        let server_before = server_stats(&shared).await;
        let cpu_before = stats::process_cpu();
        let slots = match run.phase {
            Phase::Steady => open_all(&load, &mut kept).await?,
            Phase::Handshake => (0..run.in_flight).map(|_| Vec::new()).collect(),
        };
        let window = Window::new(
            arguments::start_at(number),
            args.warmup,
            args.time_limit,
            args.calls_per_second,
            run.in_flight,
        );
        let opens_before = stats::tcp_active_opens();
        let tasks: Vec<_> = slots
            .into_iter()
            .enumerate()
            .map(|(slot, senders)| tokio::spawn(drive(load.clone(), window, slot, senders)))
            .collect();
        let mut tally = Tally::default();
        for (slot, task) in tasks.into_iter().enumerate() {
            let (slot_tally, senders) = task.await.map_err(io::Error::other)?;
            tally.merge(&slot_tally);
            kept[slot % targets].extend(senders);
        }
        let opens_after = stats::tcp_active_opens();
        let server_after = server_stats(&shared).await;
        let sample = Sample {
            server_before,
            server_after,
            client_cpu: stats::process_cpu() - cpu_before,
            client_opens: [opens_before, opens_after],
        };
        report.add(args, &run, &window, &tally, &sample)?;
        if args.between_runs == BetweenRuns::Reconnect {
            kept.iter_mut().for_each(Vec::clear);
            let mut earlier = (stats::sockets_in_use(), server_stats(&shared).await.memory_bytes);
            for _ in 0..stats::SETTLE_ROUNDS {
                tokio::time::sleep(stats::SETTLE_INTERVAL).await;
                let later = (stats::sockets_in_use(), server_stats(&shared).await.memory_bytes);
                if stats::settled(earlier, later) {
                    break;
                }
                earlier = later;
            }
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let Cli { client: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "hyper");
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(args.common.threads.0.get());
    runtime.enable_all().build()?.block_on(measure(args))
}
