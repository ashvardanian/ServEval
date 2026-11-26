//! One JSON report per configuration: printed to stdout, echoed as a Markdown row to stderr, and written atomically
//! as `<framework>-<config-hash>.json`, with the same fields `results.py` writes.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use crate::{
    arguments::{self, ClientArgs, Run, Security, Variant},
    stats::{self, Failures, Histogram, ServerStats, Tally, Window},
};

/// The report layout's version, which `scripts/plot.py` checks.
pub const SCHEMA_VERSION: u32 = 3;
/// The sysctls that cap connection counts and socket memory, recorded with every report, as `launch.py` prints them.
pub const KERNEL_LIMITS: [&str; 9] = [
    "fs.nr_open",
    "net.core.somaxconn",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.tcp_max_syn_backlog",
    "net.ipv4.tcp_max_tw_buckets",
    "net.ipv4.tcp_rmem",
    "net.ipv4.tcp_tw_reuse",
    "net.ipv4.tcp_wmem",
    "net.netfilter.nf_conntrack_max",
];

#[derive(Default, Serialize)]
pub struct Latency {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// The host, and the cores and memory this client may use.
#[derive(Serialize)]
pub struct Machine {
    pub cpu_model: String,
    pub logical_cores: usize,
    pub physical_cores: Option<usize>,
    pub ram_bytes: u64,
    pub kernel: Option<String>,
    pub os: Option<String>,
    pub architecture: &'static str,
    /// `KERNEL_LIMITS` as `/proc/sys` holds them, `None` where it does not.
    pub kernel_limits: BTreeMap<&'static str, Option<String>>,
    pub budget: Budget,
}

impl Machine {
    /// The host's CPU, memory and OS, with this client's `budget`.
    pub fn collect(budget: Budget) -> Self {
        let refresh = RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing())
            .with_memory(MemoryRefreshKind::nothing().with_ram());
        let system = System::new_with_specifics(refresh);
        Self {
            cpu_model: system
                .cpus()
                .first()
                .map_or_else(String::new, |cpu| cpu.brand().trim().to_owned()),
            logical_cores: system.cpus().len(),
            physical_cores: System::physical_core_count(),
            ram_bytes: system.total_memory(),
            kernel: System::kernel_version(),
            os: System::long_os_version(),
            architecture: std::env::consts::ARCH,
            kernel_limits: KERNEL_LIMITS
                .into_iter()
                .map(|name| {
                    let path = Path::new("/proc/sys").join(name.replace('.', "/"));
                    let value = std::fs::read_to_string(path).ok();
                    (
                        name,
                        value.map(|value| value.split_whitespace().collect::<Vec<_>>().join(" ")),
                    )
                })
                .collect(),
            budget,
        }
    }
}

/// This process's affinity mask and memory limit.
#[derive(Serialize)]
pub struct Budget {
    pub cores: usize,
    /// The affinity mask as a cpuset list, like `0-15`.
    pub cpus: String,
    /// Bytes the client may use, page cache included.
    pub memory_limit: u64,
}

impl Budget {
    /// The environment's budget, narrowed by `--memory-limit`, which may not exceed the cgroup's limit.
    pub fn resolve(memory_limit: Option<u64>) -> Result<Self, String> {
        let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let cpus = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .map_or_else(|| format!("0-{}", cores - 1), |list| list.trim().to_owned());
        let environment = crate::stats::unit_stats().memory_limit.unwrap_or_else(|| {
            System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()))
                .total_memory()
        });
        let memory_limit = match memory_limit {
            Some(bytes) if bytes > environment => {
                return Err(format!(
                    "--memory-limit {bytes} exceeds the {environment} bytes this process may use"
                ))
            }
            Some(bytes) => bytes,
            None => environment,
        };
        Ok(Self {
            cores,
            cpus,
            memory_limit,
        })
    }
}

/// The server budget `SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE` names, as the launcher sets it, or `None`.
pub fn server_cores_per_network_interface() -> Option<usize> {
    let text = std::env::var("SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE").unwrap_or_default();
    if text.is_empty() {
        return None;
    }
    let count = text
        .parse::<usize>()
        .ok()
        .filter(|&count| count != 0 && text.bytes().all(|byte| byte.is_ascii_digit()));
    Some(count.unwrap_or_else(|| {
        arguments::exit(&format!(
            "SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE=\"{text}\" does not parse, expected a positive count"
        ))
    }))
}

/// The fields that name a configuration, which the file name hashes; launch.py merges reports equal in all but
/// `process`, `threads`, `connections` and `in_flight`.
#[derive(Serialize)]
pub struct Config {
    pub framework: String,
    pub client: String,
    pub language: &'static str,
    /// `latency` with one call in flight per connection, `throughput` with more.
    pub test: &'static str,
    pub workload: String,
    pub variant: String,
    pub parsing: String,
    pub security: String,
    pub phase: String,
    pub process: usize,
    pub processes: usize,
    pub interfaces: usize,
    pub threads: usize,
    pub connections: usize,
    pub calls_in_flight_per_connection: usize,
    /// `flag`, `server-advertised` or `rfc9113-minimum`.
    pub calls_in_flight_per_connection_source: &'static str,
    /// `connections` times `calls_in_flight_per_connection`.
    pub in_flight: usize,
    pub pipeline_depth: usize,
    pub body_bytes: u64,
    pub width: usize,
    pub offered_calls_per_second: Option<usize>,
    pub server_cores_per_network_interface: Option<usize>,
    pub seed: u32,
    pub between_runs: String,
    /// The server's `--batch-size` for a batched run; plain runs and servers that do not batch have none, and leave
    /// the field out of their hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
}

/// A configuration's report; launch.py merges the reports of several client processes from the raw counters.
#[derive(Serialize)]
pub struct ConfigReport<'a> {
    pub schema_version: u32,
    pub started_unix_seconds: u64,
    pub machine: &'a Machine,
    pub config: Config,
    /// Why the configuration could not run, `None` when it ran; such a row measured nothing and holds zeros.
    pub unsupported: Option<&'static str>,
    pub tls_suite: Option<String>,
    /// Distinct memory domains of the client's interfaces, filled in by launch.py when it merges processes.
    pub domains: Option<usize>,
    pub seconds: f64,
    /// The measured window's first and last Unix millisecond, to line it up with `scripts/record.sh` samples.
    pub window_unix_ms: [u64; 2],
    pub calls: u64,
    pub errors: u64,
    pub error_classes: Failures,
    pub unsent: u64,
    pub all_calls: u64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
    /// TCP connections the client's box opened over the window, warmup included.
    pub reconnects: Option<u64>,
    /// TCP connections the server's box opened from before connecting to the end, like fan-out upstreams.
    pub upstream_connections: Option<u64>,
    /// The systemd units of the servers measured, each once.
    pub server_units: Vec<String>,
    /// The union of the servers' cores, like `8-9,104-105`.
    pub server_cpu_list: Option<String>,
    pub server_cores: Option<u64>,
    pub server_memory_limit: Option<u64>,
    pub server_cpu_microseconds: Option<u64>,
    /// The servers' `memory.peak` from before connecting to the window's end.
    pub server_memory_peak_bytes: Option<u64>,
    /// The servers' `memory.peak` over the idle time before connecting, since their previous sample.
    pub server_memory_idle_peak_bytes: Option<u64>,
    pub client_cpu_seconds: f64,
    pub calls_per_second: f64,
    pub calls_per_second_per_server_core: Option<f64>,
    pub body_gigabytes_per_second: f64,
    pub latency_us: Latency,
    pub latency_buckets: Vec<[u64; 2]>,
    pub send_lateness_us: Latency,
    pub send_lateness_buckets: Vec<[u64; 2]>,
    /// Server CPU over the warmup and window, per call of both.
    pub server_cpu_microseconds_per_call: Option<f64>,
    /// Client CPU over the warmup and window, per call of both.
    pub client_cpu_microseconds_per_call: Option<f64>,
    /// The share of the serving cores the window's calls, failed ones included, kept busy at the server's CPU per call;
    /// 1 means all of them.
    pub server_busy_share: Option<f64>,
    /// The memory peak's growth over the idle peak, never negative, per connection.
    pub server_kilobytes_per_connection: Option<f64>,
    /// The growth of the server library's own event-loop counters over the warmup and window, like UCall's.
    pub server_event_loop_counters: Option<BTreeMap<String, u64>>,
    /// Each nonzero counter of `server_event_loop_counters` per request the server parsed, like kernel enters.
    pub server_event_loop_counters_per_request: Option<BTreeMap<String, f64>>,
    pub client_cpu_percent: f64,
    /// Client CPU time over the window as a share of the client's own cores, from 0 to 1.
    pub client_cpu_share: f64,
    /// Under 99% of the offered rate went out, the p99 send lateness exceeds 5% of the p50 latency, or the client used
    /// over 90% of its own cores.
    pub client_limited: bool,
}

/// The counters around one configuration: the servers before connecting and at the end, and the client's CPU time.
/// The servers' memory peak before connecting covers the idle time since their previous sample, and at the end, the
/// connections and the window.
pub struct Sample {
    pub server_before: ServerStats,
    pub server_after: ServerStats,
    pub client_cpu: Duration,
    /// The client box's `tcp_active_opens` as the window opened and closed.
    pub client_opens: [Option<u64>; 2],
}

/// The pair a client measures, read from its server's `/health`.
#[derive(serde::Deserialize)]
pub struct Pair {
    pub framework: String,
    pub client: String,
    pub batching: bool,
    /// The `SETTINGS_MAX_CONCURRENT_STREAMS` the server advertises, `None` for no limit.
    #[serde(default)]
    pub max_concurrent_streams_per_connection: Option<u32>,
    /// The most calls one server batch gathers, `None` for a server that does not batch.
    #[serde(default)]
    pub batch_size: Option<usize>,
}

pub struct Report {
    pair: Pair,
    machine: Machine,
    started_unix_seconds: u64,
    output: PathBuf,
    process: (usize, usize),
    server_cores_per_network_interface: Option<usize>,
    printed_header: bool,
}

fn spell_list<T>(values: &[T], spell: impl Fn(&T) -> String) -> String {
    values.iter().map(spell).collect::<Vec<_>>().join(",")
}

impl Report {
    /// Prints every setting as `- Name: value` in the grammar its flag reads, and prepares the reports of `pair`.
    pub fn new(args: &ClientArgs, pair: Pair) -> Self {
        let budget = Budget::resolve(args.memory_limit).unwrap_or_else(|message| arguments::exit(&message));
        let common = &args.common;
        let optional = |count: Option<usize>| count.map_or_else(|| "none".to_owned(), |count| count.to_string());
        let server_cores_per_network_interface = server_cores_per_network_interface();
        eprintln!("- Framework: {}", pair.framework);
        eprintln!("- Client: {}", pair.client);
        eprintln!("- Threads: {}", common.threads);
        eprintln!("- Interfaces: {}", spell_list(&common.interfaces, ToString::to_string));
        eprintln!("- Port: {}", common.port);
        eprintln!("- Security: {}", arguments::spell(&common.security));
        eprintln!("- Data dir: {}", common.data_dir.display());
        eprintln!("- Seed: {}", common.seed);
        eprintln!("- Targets: {}", spell_list(&args.targets, ToString::to_string));
        eprintln!("- Connections: {}", spell_list(&args.connections, ToString::to_string));
        eprintln!("- Workloads: {}", spell_list(&args.workloads, arguments::spell));
        eprintln!("- Variants: {}", spell_list(&args.variants, arguments::spell));
        eprintln!("- Phases: {}", spell_list(&args.phases, arguments::spell));
        eprintln!("- Parsing: {}", spell_list(&args.parsing, arguments::spell));
        eprintln!(
            "- Body bytes: {}",
            spell_list(args.body_bytes(), |&bytes| arguments::spell_size(bytes))
        );
        eprintln!(
            "- Calls in flight per connection: {}",
            spell_list(&args.calls_in_flight_per_connection, ToString::to_string)
        );
        eprintln!("- Calls per second: {}", optional(args.calls_per_second));
        eprintln!("- Width: {}", args.width);
        eprintln!("- Time limit: {}", arguments::spell_duration(args.time_limit));
        eprintln!("- Warmup: {}", arguments::spell_duration(args.warmup));
        eprintln!("- Between runs: {}", arguments::spell(&args.between_runs));
        eprintln!("- Output: {}", args.output.display());
        eprintln!("- Cores: {} ({})", budget.cores, budget.cpus);
        eprintln!("- Memory limit: {}", arguments::spell_size(budget.memory_limit));
        Self {
            pair,
            machine: Machine::collect(budget),
            started_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            output: args.output.clone(),
            process: arguments::process(),
            server_cores_per_network_interface,
            printed_header: false,
        }
    }

    /// The fields that name `run`'s configuration.
    fn config(&self, args: &ClientArgs, run: &Run) -> Config {
        Config {
            framework: self.pair.framework.clone(),
            client: self.pair.client.clone(),
            language: "rust",
            test: run.test(),
            workload: arguments::spell(&run.workload),
            variant: arguments::spell(&run.variant),
            parsing: arguments::spell(&run.parsing),
            security: match args.common.security {
                Security::Tls => "tls13".into(),
                other => arguments::spell(&other),
            },
            phase: arguments::spell(&run.phase),
            process: self.process.0,
            processes: self.process.1,
            interfaces: args.common.interfaces.len(),
            threads: args.common.threads.0.get(),
            connections: run.connections,
            calls_in_flight_per_connection: run.calls_in_flight_per_connection,
            calls_in_flight_per_connection_source: run.calls_in_flight_per_connection_source.name(),
            in_flight: run.in_flight,
            pipeline_depth: 1,
            body_bytes: run.body_bytes,
            width: args.width,
            offered_calls_per_second: args.calls_per_second,
            server_cores_per_network_interface: self.server_cores_per_network_interface,
            seed: args.common.seed.0,
            between_runs: arguments::spell(&args.between_runs),
            batch_size: self.pair.batch_size.filter(|_| run.variant == Variant::Batched),
        }
    }

    /// Writes the row of a configuration that cannot run, `unsupported` naming why: its configuration and machine,
    /// with zeros where a run would have measured, since no call went out and no server was sampled.
    pub fn unsupported(&mut self, args: &ClientArgs, run: &Run, reason: &'static str) -> io::Result<PathBuf> {
        let report = ConfigReport {
            schema_version: SCHEMA_VERSION,
            started_unix_seconds: self.started_unix_seconds,
            machine: &self.machine,
            config: self.config(args, run),
            unsupported: Some(reason),
            tls_suite: None,
            domains: None,
            seconds: 0.0,
            window_unix_ms: [0, 0],
            calls: 0,
            errors: 0,
            error_classes: Failures::default(),
            unsent: 0,
            all_calls: 0,
            request_body_bytes: 0,
            response_body_bytes: 0,
            reconnects: None,
            upstream_connections: None,
            server_units: Vec::new(),
            server_cpu_list: None,
            server_cores: None,
            server_memory_limit: None,
            server_cpu_microseconds: None,
            server_memory_peak_bytes: None,
            server_memory_idle_peak_bytes: None,
            client_cpu_seconds: 0.0,
            calls_per_second: 0.0,
            calls_per_second_per_server_core: None,
            body_gigabytes_per_second: 0.0,
            latency_us: Latency::default(),
            latency_buckets: Vec::new(),
            send_lateness_us: Latency::default(),
            send_lateness_buckets: Vec::new(),
            server_cpu_microseconds_per_call: None,
            client_cpu_microseconds_per_call: None,
            server_busy_share: None,
            server_kilobytes_per_connection: None,
            server_event_loop_counters: None,
            server_event_loop_counters_per_request: None,
            client_cpu_percent: 0.0,
            client_cpu_share: 0.0,
            client_limited: false,
        };
        print(&mut self.printed_header, &report)?;
        write_report(&self.output, &report)
    }

    /// Assembles the report of `run`, prints it, and writes its file.
    pub fn add(
        &mut self,
        args: &ClientArgs,
        run: &Run,
        window: &Window,
        tally: &Tally,
        sample: &Sample,
    ) -> io::Result<PathBuf> {
        let window_seconds = window.seconds();
        let percentiles = |histogram: &Histogram| {
            let microseconds = |fraction: f64| histogram.percentile(fraction) as f64 / 1000.0;
            Latency {
                p50: microseconds(0.5),
                p90: microseconds(0.9),
                p99: microseconds(0.99),
                p999: microseconds(0.999),
                max: microseconds(1.0),
            }
        };
        let (latency_us, send_lateness_us) = (percentiles(&tally.latencies), percentiles(&tally.lateness));
        let (before, after) = (&sample.server_before, &sample.server_after);
        let server_cpu = after.cpu_microseconds.zip(before.cpu_microseconds);
        let server_cpu = server_cpu.map(|(after, before)| after.saturating_sub(before));
        let (peak, idle_peak) = (after.memory_peak_bytes, before.memory_peak_bytes);
        let growth = peak.zip(idle_peak).map(|(peak, idle)| peak.saturating_sub(idle));
        let counters = stats::counter_growth(&before.event_loop_counters, &after.event_loop_counters);
        let server_cores = (after.cores != 0).then_some(after.cores);
        let client_cpu_seconds = sample.client_cpu.as_secs_f64();
        let client_cpu_percent = 100.0 * client_cpu_seconds / (args.warmup + args.time_limit).as_secs_f64();
        let errors = tally.failures.total();
        let sent_per_second = (tally.calls + errors) as f64 / window_seconds;
        let offered_short = args
            .calls_per_second
            .is_some_and(|offered| sent_per_second < 0.99 * offered as f64);
        let client_cpu_share = client_cpu_percent / 100.0 / self.machine.budget.cores as f64;
        let late = send_lateness_us.p99 > 0.05 * latency_us.p50;
        let client_limited = offered_short || late || client_cpu_share > 0.9;
        let opens = |[before, after]: [Option<u64>; 2]| Some(after?.saturating_sub(before?));
        let calls_per_second = tally.calls as f64 / window_seconds;
        // Both sides' CPU covers the warmup as well as the window, so both divide by every call.
        let all_calls = tally.all_calls.max(1) as f64;
        let server_cpu_microseconds_per_call = server_cpu.map(|cpu| cpu as f64 / all_calls);
        let server_busy_share = server_cpu_microseconds_per_call
            .zip(server_cores)
            .map(|(per_call, cores)| per_call * sent_per_second / 1e6 / cores as f64);
        let report = ConfigReport {
            schema_version: SCHEMA_VERSION,
            started_unix_seconds: self.started_unix_seconds,
            machine: &self.machine,
            config: self.config(args, run),
            unsupported: None,
            tls_suite: crate::security::negotiated_suite(),
            domains: None,
            seconds: window_seconds,
            window_unix_ms: window.unix_ms(),
            calls: tally.calls,
            errors,
            error_classes: tally.failures,
            unsent: tally.unsent,
            all_calls: tally.all_calls,
            request_body_bytes: tally.request_body_bytes,
            response_body_bytes: tally.response_body_bytes,
            reconnects: opens(sample.client_opens),
            upstream_connections: opens([before.tcp_active_opens, after.tcp_active_opens]),
            server_units: after.units.clone(),
            server_cpu_list: (!after.cpu_list.is_empty()).then(|| after.cpu_list.clone()),
            server_cores,
            server_memory_limit: after.memory_limit,
            server_cpu_microseconds: server_cpu,
            server_memory_peak_bytes: peak,
            server_memory_idle_peak_bytes: idle_peak,
            client_cpu_seconds,
            calls_per_second,
            calls_per_second_per_server_core: server_cores.map(|cores| calls_per_second / cores as f64),
            body_gigabytes_per_second: (tally.request_body_bytes + tally.response_body_bytes) as f64
                / window_seconds
                / 1e9,
            latency_us,
            latency_buckets: tally.latencies.sparse(),
            send_lateness_us,
            send_lateness_buckets: tally.lateness.sparse(),
            server_cpu_microseconds_per_call,
            client_cpu_microseconds_per_call: Some(client_cpu_seconds * 1e6 / all_calls),
            server_busy_share,
            server_kilobytes_per_connection: growth.map(|bytes| bytes as f64 / 1024.0 / run.connections as f64),
            server_event_loop_counters_per_request: stats::per_request(&counters),
            server_event_loop_counters: counters,
            client_cpu_percent,
            client_cpu_share,
            client_limited,
        };
        print(&mut self.printed_header, &report)?;
        write_report(&self.output, &report)
    }
}

/// Echoes `report` as a Markdown row to stderr and as compact JSON to stdout, where launch.py collects it.
fn print(printed_header: &mut bool, report: &ConfigReport) -> io::Result<()> {
    if !*printed_header {
        eprintln!("| Machine | Framework | Test | Workload | Parsing | Security | Phase | Connections | Calls per connection | calls/s | calls/s per server core | p50, µs | p99, µs | Body GB/s | Server CPU, µs/call | Client CPU, µs/call | KB per connection |");
        eprintln!("| :------ | :-------- | :--- | :------- | :------ | :------- | :---- | ----------: | -------------------: | ------: | ----------------------: | ------: | ------: | --------: | ------------------: | ------------------: | ----------------: |");
        *printed_header = true;
    }
    let optional = |value: Option<f64>, digits: usize| value.map_or("n/a".into(), |value| format!("{value:.digits$}"));
    let config = &report.config;
    let measured = match report.unsupported {
        Some(reason) => format!("unsupported: {reason}"),
        None => format!(
            "{:.0} | {} | {:.1} | {:.1} | {:.3} | {} | {} | {}",
            report.calls_per_second,
            optional(report.calls_per_second_per_server_core, 0),
            report.latency_us.p50,
            report.latency_us.p99,
            report.body_gigabytes_per_second,
            optional(report.server_cpu_microseconds_per_call, 1),
            optional(report.client_cpu_microseconds_per_call, 1),
            optional(report.server_kilobytes_per_connection, 1),
        ),
    };
    eprintln!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {measured} |",
        report.machine.cpu_model,
        config.framework,
        config.test,
        config.workload,
        config.parsing,
        config.security,
        config.phase,
        config.connections,
        config.calls_in_flight_per_connection,
    );
    println!("{}", serde_json::to_string(report)?);
    Ok(())
}

/// The FNV-1a hash of `config`'s compact JSON, as 16 hexadecimal digits.
pub fn config_hash(config: &Config) -> io::Result<String> {
    let hash = serde_json::to_vec(config)?
        .iter()
        .fold(0xCBF2_9CE4_8422_2325u64, |hash, &byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01B3)
        });
    Ok(format!("{hash:016x}"))
}

/// Writes `report` as `<framework>-<config-hash>.json`, flushed to the drive before it replaces an older one.
fn write_report(directory: &Path, report: &ConfigReport) -> io::Result<PathBuf> {
    std::fs::create_dir_all(directory)?;
    let hash = config_hash(&report.config)?;
    let path = directory.join(format!("{}-{hash}.json", report.config.framework));
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let file = std::fs::File::create(&temporary)?;
    serde_json::to_writer_pretty(&file, report)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_hash_matches_python() {
        let config = Config {
            framework: "axum".into(),
            client: "hyper".into(),
            language: "rust",
            test: "latency",
            workload: "tool".into(),
            variant: "plain".into(),
            parsing: "body".into(),
            security: "tls13".into(),
            phase: "steady".into(),
            process: 0,
            processes: 2,
            interfaces: 1,
            threads: 8,
            connections: 10_000,
            calls_in_flight_per_connection: 1,
            calls_in_flight_per_connection_source: "flag",
            in_flight: 10_000,
            pipeline_depth: 1,
            body_bytes: 0,
            width: 8,
            offered_calls_per_second: None,
            server_cores_per_network_interface: Some(2),
            seed: 42,
            between_runs: "keep".into(),
            batch_size: None,
        };
        assert_eq!(config_hash(&config).unwrap(), "e77eb7cb6641786b");
    }
}
