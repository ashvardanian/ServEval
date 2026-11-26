//! awc on one actix system per thread, each with a pooled client per target sized to its share of the connections
//! and filled up front by concurrent health checks, over sockets `arguments::connect` opens from the paired interface;
//! awc cannot bind a device.
//! The threads live across configurations, so their pools can stay open under `--between-runs keep`.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::Duration,
};

use actix_tls::connect::{ConnectError as TcpConnectError, ConnectInfo, Connection as TcpConnection};
use actix_web::{
    dev::fn_service,
    http::{
        header::{self, ContentType},
        StatusCode, Uri,
    },
    rt::System,
};
use awc::error::{ConnectError, SendRequestError};
use bytes::Bytes;
use futures::{future::join_all, StreamExt};
use rustls::ClientConfig;

use serveval::{
    arguments::{self, BetweenRuns, ClientArgs, Phase, Run, Security, Workload},
    payloads::{self, Exchange, ToolParts},
    results::{Pair, Report, Sample},
    security,
    stats::{self, Failure, ServerStats, Slot, Tally, UnitStats, Window},
};

/// awc driving an HTTP server through the tool, context, download and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    client: ClientArgs,
}

struct Shared {
    args: ClientArgs,
    /// `http://ADDRESS:PORT` or `https://…` per target.
    origins: Vec<String>,
    tls: Arc<ClientConfig>,
    ring: Vec<ToolParts>,
}

/// One configuration's calls: the context upload or download, or one exchange per ring entry.
struct Load {
    shared: Arc<Shared>,
    run: Run,
    exchanges: Vec<Exchange>,
    /// Each target's URL per exchange, or its one context URL.
    urls: Vec<Vec<String>>,
    /// The body a context upload sends, or a download must get back.
    context: Bytes,
}

fn client(shared: &Shared, target: usize, limit: usize) -> awc::Client {
    // awc's own TCP connector binds its local address with port 0, which claims an ephemeral port before the target is
    // known, so a run of 65,536 connections exhausts them; it also leaves Nagle's algorithm on. `arguments::connect`
    // binds with `IP_BIND_ADDRESS_NO_PORT`, letting `connect` pick a port per target, and turns Nagle's algorithm off,
    // as for hyper and tonic, each connection taking the interface's addresses in turn.
    let interface = shared.args.interface_of(target).clone();
    let opened = Arc::new(AtomicUsize::new(0));
    let bound = fn_service(move |info: ConnectInfo<Uri>| {
        let (interface, index) = (interface.clone(), opened.fetch_add(1, Ordering::Relaxed));
        async move {
            // The origins name targets by address, so no resolver runs; a bracketed IPv6 host loses its brackets.
            let host = info.hostname().trim_start_matches('[').trim_end_matches(']');
            let address = info.addrs().next().or_else(|| {
                let address: IpAddr = host.parse().ok()?;
                Some(SocketAddr::new(address, info.port()))
            });
            let address = address.ok_or(TcpConnectError::Unresolved)?;
            let stream = arguments::connect(&interface, index, address)
                .await
                .map_err(TcpConnectError::Io)?;
            Ok::<_, TcpConnectError>(TcpConnection::new(info.request().clone(), stream))
        }
    });
    // awc's default 15s idle and 75s lifetime would recycle pooled connections inside a measured window.
    let connector = awc::Connector::new()
        .connector(bound)
        .limit(limit)
        .conn_keep_alive(Duration::MAX)
        .conn_lifetime(Duration::MAX)
        .rustls_0_23(Arc::clone(&shared.tls));
    awc::Client::builder()
        .connector(connector)
        .disable_timeout()
        .no_default_headers()
        .finish()
}

fn failure(error: &SendRequestError) -> Failure {
    match error {
        SendRequestError::Connect(ConnectError::Io(error)) => Failure::of_io(error),
        SendRequestError::Connect(ConnectError::Timeout) | SendRequestError::Timeout => Failure::Timeout,
        _ => Failure::Connect,
    }
}

/// The request of call `sequence` with its body, and the reply it must get back.
fn request(load: &Load, client: &awc::Client, target: usize, sequence: usize) -> (awc::ClientRequest, Bytes, Bytes) {
    let urls = &load.urls[target];
    let url = &urls[sequence % urls.len()];
    let (request, body, reply) = match (load.run.workload, load.exchanges.get(sequence % payloads::RING)) {
        (Workload::Download, _) => (client.get(url), Bytes::new(), load.context.clone()),
        (_, None) => {
            let reply = payloads::count_reply(load.context.len() as u64);
            (client.put(url), load.context.clone(), Bytes::from(reply))
        }
        (_, Some(exchange)) => {
            let mut request = client.post(url);
            if let Some(traceparent) = &exchange.traceparent {
                request = request
                    .insert_header((header::AUTHORIZATION, payloads::AUTHORIZATION))
                    .insert_header(("traceparent", traceparent.as_str()));
            }
            (request, exchange.body.clone(), exchange.reply.clone())
        }
    };
    let mut request = request
        .insert_header(ContentType::json())
        .insert_header((header::ACCEPT_ENCODING, "identity"));
    if load.run.phase == Phase::Handshake {
        request = request.force_close();
    }
    (request, body, reply)
}

/// Sends a download and compares its reply chunk by chunk with `expected`, returning the bytes received.
async fn download(request: awc::ClientRequest, expected: &[u8]) -> Result<u64, Failure> {
    let mut response = request.send().await.map_err(|error| failure(&error))?;
    if response.status() != StatusCode::OK {
        return Err(Failure::Status);
    }
    let mut check = payloads::DownloadCheck::new(expected);
    while let Some(chunk) = response.next().await {
        check.chunk(&chunk.map_err(|_| Failure::Connect)?);
    }
    check.matched().ok_or(Failure::WrongReply)
}

/// One verified call: the body bytes sent and received, or why it failed.
async fn call(load: &Load, client: &awc::Client, target: usize, sequence: usize) -> Result<(u64, u64), Failure> {
    let (request, body, expected) = request(load, client, target, sequence);
    if load.run.workload == Workload::Download {
        return Ok((0, download(request, &expected).await?));
    }
    let sent = body.len() as u64;
    let mut response = request.send_body(body).await.map_err(|error| failure(&error))?;
    let status = response.status();
    let reply = response.body().limit(1 << 20).await.map_err(|_| Failure::Connect)?;
    match (status == StatusCode::OK, reply == expected) {
        (false, _) => Err(Failure::Status),
        (true, false) => Err(Failure::WrongReply),
        (true, true) => Ok((sent, reply.len() as u64)),
    }
}

/// A one-off GET on its own actix system, for `/health` and `/stats`.
fn get(shared: &Shared, target: usize, path: &str) -> Option<Bytes> {
    let url = format!("{}{path}", shared.origins[target]);
    System::new().block_on(async {
        let request = client(shared, target, 1).get(url);
        let mut response = request
            .insert_header((header::ACCEPT_ENCODING, "identity"))
            .send()
            .await
            .ok()?;
        let body = response.body().await.ok()?;
        (response.status() == StatusCode::OK).then_some(body)
    })
}

fn server_stats(shared: &Shared) -> ServerStats {
    let all: Vec<UnitStats> = (0..shared.origins.len())
        .map(|target| {
            get(shared, target, "/stats")
                .and_then(|reply| serde_json::from_slice(&reply).ok())
                .unwrap_or_default()
        })
        .collect();
    UnitStats::sum(&all)
}

/// Status of one call, to tell an absent route from a failing one.
fn probe(load: &Load) -> Option<StatusCode> {
    System::new().block_on(async {
        let (request, body, _) = request(load, &client(&load.shared, 0, 1), 0, 0);
        Some(request.send_body(body).await.ok()?.status())
    })
}

/// What a thread receives for one configuration and reports back.
struct Steps {
    loads: mpsc::Receiver<Arc<Load>>,
    windows: mpsc::Receiver<Window>,
    ready: mpsc::Sender<()>,
    finished: mpsc::Sender<Tally>,
    sampled: Arc<Barrier>,
}

/// One thread's slots `thread + i * threads` of every load, slot `i` on target `i % targets`, so each target's client
/// holds at least as many connections as the slots it carries. The thread reports `ready` once connected, runs the
/// window it then receives, sends its tally, and keeps its connections open until the caller has sampled the
/// servers; it reuses its clients for the next load when the policy and the connection shares allow.
fn drive_thread(thread: usize, threads: usize, steps: Steps) {
    System::new().block_on(async move {
        let mut clients: Vec<(awc::Client, usize)> = Vec::new();
        while let Ok(load) = steps.loads.recv() {
            let (shared, run) = (&load.shared, &load.run);
            let targets = shared.origins.len();
            let connections = arguments::share(run.connections, thread, threads);
            let limits: Vec<usize> = (0..targets)
                .map(|target| arguments::share(connections, target, targets).max(1))
                .collect();
            if !clients.iter().map(|(_, limit)| *limit).eq(limits.iter().copied()) {
                clients = limits
                    .iter()
                    .enumerate()
                    .map(|(target, &limit)| (client(shared, target, limit), limit))
                    .collect();
            }
            if run.phase == Phase::Steady {
                let warmups = clients.iter().enumerate().flat_map(|(target, (client, limit))| {
                    (0..*limit).map(move |_| async move {
                        // A reply left unread would close its connection rather than pool it.
                        let health = client.get(format!("{}/health", shared.origins[target]));
                        let health = health.insert_header((header::ACCEPT_ENCODING, "identity"));
                        if let Ok(mut response) = health.send().await {
                            let _ = response.body().await;
                        }
                    })
                });
                join_all(warmups).await;
            }
            let _ = steps.ready.send(());
            let Ok(opened) = steps.windows.recv() else {
                return;
            };
            let slots = (thread..run.in_flight).step_by(threads).map(|slot| {
                let (load, clients) = (&load, &clients);
                async move {
                    let target = slot / threads % targets;
                    let mut schedule = Slot::new(opened, slot);
                    while let Some((intended, sequence)) = schedule.next().await {
                        let outcome = call(load, &clients[target].0, target, sequence).await;
                        schedule.record(intended, outcome);
                    }
                    schedule.tally
                }
            });
            let mut tally = Tally::default();
            join_all(slots).await.iter().for_each(|slot| tally.merge(slot));
            let _ = steps.finished.send(tally);
            steps.sampled.wait();
            if shared.args.between_runs == BetweenRuns::Reconnect {
                clients.clear();
            }
        }
    })
}

fn main() -> io::Result<()> {
    let Cli { client: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "awc");
    arguments::require_no_device(&args.common, "awc");
    let scheme = if args.common.security == Security::Tls {
        "https"
    } else {
        "http"
    };
    let shared = Arc::new(Shared {
        origins: args
            .targets
            .iter()
            .map(|address| format!("{scheme}://{address}:{}", args.common.port))
            .collect(),
        tls: security::tls_client_config(&args.common, &[b"http/1.1"])?,
        ring: payloads::tool_ring(args.common.seed.into()),
        args,
    });
    let args = &shared.args;
    let threads = args.common.threads.0.get();
    let health = get(&shared, 0, "/health").ok_or_else(|| io::Error::other("no /health reply"))?;
    // The row names this client, not the one `/health` names for the server's own pair.
    let pair = Pair {
        client: "awc".into(),
        ..serde_json::from_slice(&health)?
    };
    let runs = args.runs(pair.batching, false, pair.max_concurrent_streams_per_connection);
    let mut report = Report::new(args, pair);
    let (ready, readied) = mpsc::channel();
    let (finished, tallies) = mpsc::channel();
    let sampled = Arc::new(Barrier::new(threads + 1));
    let (senders, workers): (Vec<_>, Vec<_>) = (0..threads)
        .map(|thread| {
            let (load, loads) = mpsc::channel();
            let (window, windows) = mpsc::channel();
            let steps = Steps {
                loads,
                windows,
                ready: ready.clone(),
                finished: finished.clone(),
                sampled: sampled.clone(),
            };
            (
                (load, window),
                thread::spawn(move || drive_thread(thread, threads, steps)),
            )
        })
        .unzip();
    drop((ready, finished));
    for (number, run) in runs.into_iter().enumerate() {
        if let Some(reason) = run.unsupported {
            report.unsupported(args, &run, reason)?;
            continue;
        }
        let (context, exchanges) = match run.workload {
            Workload::Context | Workload::Download => (
                payloads::context_body(args.common.seed.into(), run.body_bytes),
                Vec::new(),
            ),
            _ => (Bytes::new(), payloads::exchanges(&shared.ring, &run, args.width)),
        };
        let download_path = payloads::download_path(run.body_bytes);
        let paths: Vec<&str> = match run.workload {
            Workload::Context => vec!["/context"],
            Workload::Download => vec![download_path.as_str()],
            _ => exchanges.iter().map(|exchange| exchange.path.as_str()).collect(),
        };
        let urls = shared
            .origins
            .iter()
            .map(|origin| paths.iter().map(|path| format!("{origin}{path}")).collect())
            .collect();
        let load = Arc::new(Load {
            shared: shared.clone(),
            run,
            exchanges,
            urls,
            context,
        });
        if let Some(StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED) = probe(&load)
        {
            report.unsupported(args, &run, arguments::NOT_SERVED)?;
            continue;
        }
        let server_before = server_stats(&shared);
        let cpu_before = stats::process_cpu();
        senders
            .iter()
            .for_each(|(sender, _)| sender.send(load.clone()).expect("a worker waiting for its load"));
        readied.iter().take(threads).for_each(drop);
        let window = Window::new(
            arguments::start_at(number),
            args.warmup,
            args.time_limit,
            args.calls_per_second,
            run.in_flight,
        );
        let opens_before = stats::tcp_active_opens();
        senders
            .iter()
            .for_each(|(_, sender)| sender.send(window).expect("a worker waiting for its window"));
        let mut tally = Tally::default();
        tallies.iter().take(threads).for_each(|other| tally.merge(&other));
        let opens_after = stats::tcp_active_opens();
        let server_after = server_stats(&shared);
        sampled.wait();
        let sample = Sample {
            server_before,
            server_after,
            client_cpu: stats::process_cpu() - cpu_before,
            client_opens: [opens_before, opens_after],
        };
        report.add(args, &run, &window, &tally, &sample)?;
        if args.between_runs == BetweenRuns::Reconnect {
            let mut earlier = (stats::sockets_in_use(), server_stats(&shared).memory_current_bytes);
            for _ in 0..stats::SETTLE_ROUNDS {
                thread::sleep(stats::SETTLE_INTERVAL);
                let later = (stats::sockets_in_use(), server_stats(&shared).memory_current_bytes);
                if stats::settled(earlier, later) {
                    break;
                }
                earlier = later;
            }
        }
    }
    drop(senders);
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("a client thread panicked"))?;
    }
    Ok(())
}
