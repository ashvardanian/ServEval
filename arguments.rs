//! Flags shared by every Rust server and client, each defaulting to its `SERVEVAL_<FLAG>` variable.

use std::{
    fmt,
    hash::{BuildHasher, Hasher},
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::ValueEnum;
use socket2::{Domain, Protocol, Socket, Type};

/// Flags every server and client reads.
#[derive(clap::Args)]
pub struct CommonArgs {
    /// Workers, 0 for every core in the affinity mask.
    #[arg(long, env = "SERVEVAL_THREADS", default_value = "0", value_parser = parse_threads_flag)]
    pub threads: Threads,
    /// Local interfaces as DEVICE=ADDRESS or ADDRESS, extra addresses joined by +, like ens5=10.0.1.5,ens6=10.0.2.5.
    #[arg(long, env = "SERVEVAL_INTERFACES", value_delimiter = ',', default_value = "127.0.0.1", value_parser = parse_interface_flag)]
    pub interfaces: Vec<Interface>,
    /// Port every server listens on.
    #[arg(long, env = "SERVEVAL_PORT", default_value = "8545", value_parser = parse_port_flag)]
    pub port: u16,
    #[arg(long, env = "SERVEVAL_SECURITY", value_enum, default_value = "none")]
    pub security: Security,
    /// Directory holding the `tls` and `noise` keys that keys.sh writes.
    #[arg(long, env = "SERVEVAL_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    /// Seed of every payload, or `random` to draw one.
    #[arg(long, env = "SERVEVAL_SEED", default_value = "42", value_parser = parse_seed_flag)]
    pub seed: Seed,
}

/// A server's flags.
#[derive(clap::Args)]
pub struct ServerArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Connection counts, like 64,10K,1M; the largest is the admission budget and listen backlog.
    #[arg(long, env = "SERVEVAL_CONNECTIONS", value_delimiter = ',', default_value = "10K", value_parser = parse_count_flag)]
    pub connections: Vec<usize>,
    /// Fan-out targets as ADDRESS:PORT.
    #[arg(long, env = "SERVEVAL_UPSTREAMS", value_delimiter = ',')]
    pub upstreams: Vec<SocketAddr>,
    /// Most calls one batch gathers, for frameworks that ship batching.
    #[arg(long, env = "SERVEVAL_BATCH_SIZE", default_value = "32", value_parser = parse_count_flag)]
    pub batch_size: usize,
    /// Longest a batch waits to fill.
    #[arg(long, env = "SERVEVAL_BATCH_WAIT", default_value = "10ms", value_parser = parse_duration_flag)]
    pub batch_wait: Duration,
}

impl ServerArgs {
    pub fn admission(&self) -> usize {
        self.connections.iter().copied().max().unwrap_or(1)
    }

    /// The one interface a competitor process binds; launch.py starts one process per memory domain.
    pub fn single_interface(&self, framework: &str) -> &Interface {
        match self.common.interfaces.as_slice() {
            [interface] => interface,
            _ => exit(&format!(
                "{framework} binds one interface per process; launch.py starts one per memory domain"
            )),
        }
    }
}

/// A client's flags.
#[derive(clap::Args)]
pub struct ClientArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Server addresses, paired with --interfaces by position.
    #[arg(long, env = "SERVEVAL_TARGETS", value_delimiter = ',', default_value = "127.0.0.1")]
    pub targets: Vec<IpAddr>,
    /// Connection counts to sweep, like 64,10K,1M.
    #[arg(long, env = "SERVEVAL_CONNECTIONS", value_delimiter = ',', default_value = "64", value_parser = parse_count_flag)]
    pub connections: Vec<usize>,
    #[arg(
        long,
        env = "SERVEVAL_WORKLOADS",
        value_enum,
        value_delimiter = ',',
        default_value = "tool,context,fanout"
    )]
    pub workloads: Vec<Workload>,
    #[arg(
        long,
        env = "SERVEVAL_VARIANTS",
        value_enum,
        value_delimiter = ',',
        default_value = "plain,batched"
    )]
    pub variants: Vec<Variant>,
    #[arg(
        long,
        env = "SERVEVAL_PHASES",
        value_enum,
        value_delimiter = ',',
        default_value = "steady,handshake"
    )]
    pub phases: Vec<Phase>,
    /// Tool call shapes to sweep: raw bytes, a typed body, or a typed body with path, query and header values.
    #[arg(
        long,
        env = "SERVEVAL_PARSING",
        value_enum,
        value_delimiter = ',',
        default_value = "body"
    )]
    pub parsing: Vec<Parsing>,
    /// Context upload sizes to sweep, like 256KB,4MB,64MB.
    #[arg(long, env = "SERVEVAL_BODY_BYTES", value_delimiter = ',', default_value = "256KB", value_parser = parse_size_flag)]
    pub body_bytes: Vec<u64>,
    /// Calls outstanding at once, one per connection when unset.
    #[arg(long, env = "SERVEVAL_IN_FLIGHT", value_parser = parse_count_flag)]
    pub in_flight: Option<usize>,
    /// Scheduled calls per second across this process; unset runs a closed loop.
    #[arg(long, env = "SERVEVAL_CALLS_PER_SECOND", value_parser = parse_count_flag)]
    pub calls_per_second: Option<usize>,
    /// Upstream calls per fan-out.
    #[arg(long, env = "SERVEVAL_WIDTH", default_value = "8", value_parser = parse_count_flag)]
    pub width: usize,
    /// Measured time per configuration, such as 10s or 500ms.
    #[arg(long, env = "SERVEVAL_TIME_LIMIT", default_value = "10s", value_parser = parse_duration_flag)]
    pub time_limit: Duration,
    /// Unmeasured calls before each measured window.
    #[arg(long, env = "SERVEVAL_WARMUP", default_value = "1s", value_parser = parse_duration_flag)]
    pub warmup: Duration,
    /// Client memory budget, at most the cgroup's.
    #[arg(long, env = "SERVEVAL_MEMORY_LIMIT", value_parser = parse_size_flag)]
    pub memory_limit: Option<u64>,
    #[arg(long, env = "SERVEVAL_BETWEEN_RUNS", value_enum, default_value = "reconnect")]
    pub between_runs: BetweenRuns,
    /// Directory for `<framework>-<config-hash>.json` reports.
    #[arg(long, env = "SERVEVAL_OUTPUT", default_value = "results")]
    pub output: PathBuf,
}

/// One measured configuration.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    pub workload: Workload,
    pub variant: Variant,
    /// Swept only for plain tool calls, `body` for every other run.
    pub parsing: Parsing,
    pub phase: Phase,
    pub connections: usize,
    pub in_flight: usize,
    /// Upload size of a context run, 0 for the others.
    pub body_bytes: u64,
}

impl ClientArgs {
    /// Every configuration to measure; batched variants only where the server batches, and never for uploads.
    /// Without multiplexing a connection carries one call at a time, so calls in flight stop at the connections.
    pub fn runs(&self, batching: bool, multiplexed: bool) -> Vec<Run> {
        let mut runs = Vec::new();
        for &workload in &self.workloads {
            let sizes = match workload {
                Workload::Context => self.body_bytes.clone(),
                _ => vec![0],
            };
            for &variant in &self.variants {
                if variant == Variant::Batched && (!batching || workload == Workload::Context) {
                    continue;
                }
                let shapes = match (workload, variant) {
                    (Workload::Tool, Variant::Plain) => self.parsing.clone(),
                    _ => vec![Parsing::Body],
                };
                for &parsing in &shapes {
                    for &phase in &self.phases {
                        for &connections in &self.connections {
                            let wanted = self.in_flight.unwrap_or(connections);
                            let in_flight = if multiplexed { wanted } else { wanted.min(connections) };
                            for &body_bytes in &sizes {
                                runs.push(Run {
                                    workload,
                                    variant,
                                    parsing,
                                    phase,
                                    connections,
                                    in_flight,
                                    body_bytes,
                                });
                            }
                        }
                    }
                }
            }
        }
        runs
    }

    /// The local interface paired with target `index`, a single interface serving every target.
    pub fn interface_of(&self, index: usize) -> &Interface {
        let interfaces = &self.common.interfaces;
        &interfaces[index % interfaces.len()]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Security {
    None,
    NoiseXx,
    NoiseIk,
    Tls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Workload {
    Tool,
    Context,
    Fanout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Variant {
    Plain,
    Batched,
}

/// How much of a tool call the server must read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Parsing {
    /// `POST /raw`, answered with the first 256 body bytes unchanged.
    Raw,
    /// `POST /tool`, answered with the id and `after + top_k` from the typed body.
    Body,
    /// `POST /tool/{name}?lang=..&after=..` behind a bearer token and a `traceparent`, answered with all of them.
    Full,
}

/// What happens to a client's connections between two configurations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BetweenRuns {
    /// Reuse the open connections, opening only those a larger count adds.
    Keep,
    /// Close every connection, and open fresh ones for the next configuration.
    Reconnect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Phase {
    /// Calls reuse established connections.
    Steady,
    /// Every call opens, and for secure modes handshakes, its own connection.
    Handshake,
}

/// Spells a value the way the command line takes it.
pub fn spell(value: &impl ValueEnum) -> String {
    value
        .to_possible_value()
        .map_or_else(String::new, |value| value.get_name().to_owned())
}

/// A local device and its addresses; an empty device routes by address alone.
#[derive(Clone, Debug)]
pub struct Interface {
    pub device: Option<String>,
    pub addresses: Vec<IpAddr>,
}

impl std::fmt::Display for Interface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        let addresses: Vec<String> = self.addresses.iter().map(IpAddr::to_string).collect();
        match &self.device {
            Some(device) => write!(formatter, "{device}={}", addresses.join("+")),
            None => write!(formatter, "{}", addresses.join("+")),
        }
    }
}

/// Exits if any interface names a device, for frameworks that bind by address alone.
pub fn require_no_device(common: &CommonArgs, framework: &str) {
    if common.interfaces.iter().any(|interface| interface.device.is_some()) {
        exit(&format!(
            "{framework} binds by address alone; list interfaces as ADDRESS"
        ));
    }
}

/// Opens a listener on the interface's first address, bound to its device, with `backlog` pending connections.
pub fn listen(interface: &Interface, port: u16, backlog: usize) -> io::Result<std::net::TcpListener> {
    let address = SocketAddr::new(interface.addresses[0], port);
    let socket = Socket::new(Domain::for_address(address), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    // Accepted connections inherit it, so no framework waits on Nagle's algorithm.
    socket.set_tcp_nodelay(true)?;
    bind_device(&socket, interface)?;
    socket.bind(&address.into())?;
    socket.listen(i32::try_from(backlog).unwrap_or(i32::MAX))?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Connects to `target` through the interface's device, from its addresses in turn by `index`.
pub async fn connect(interface: &Interface, index: usize, target: SocketAddr) -> io::Result<tokio::net::TcpStream> {
    let socket = Socket::new(Domain::for_address(target), Type::STREAM, Some(Protocol::TCP))?;
    bind_device(&socket, interface)?;
    defer_port_choice(&socket)?;
    let source = interface.addresses[index % interface.addresses.len()];
    socket.bind(&SocketAddr::new(source, 0).into())?;
    socket.set_tcp_nodelay(true)?;
    socket.set_nonblocking(true)?;
    tokio::net::TcpSocket::from_std_stream(socket.into())
        .connect(target)
        .await
}

fn bind_device(
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] socket: &Socket,
    interface: &Interface,
) -> io::Result<()> {
    match &interface.device {
        None => Ok(()),
        #[cfg(target_os = "linux")]
        Some(device) => socket.bind_device(Some(device.as_bytes())),
        #[cfg(not(target_os = "linux"))]
        Some(device) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("binding device {device} needs Linux"),
        )),
    }
}

/// Lets `connect` pick the source port, so one source address reaches past 64K ports across several targets.
#[cfg(target_os = "linux")]
fn defer_port_choice(socket: &Socket) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let enabled: libc::c_int = 1;
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_BIND_ADDRESS_NO_PORT,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn defer_port_choice(_: &Socket) -> io::Result<()> {
    Ok(())
}

/// Resolves on SIGINT or SIGTERM, which systemd sends to stop a unit.
pub async fn stop_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let (Ok(mut interrupt), Ok(mut terminate)) = (signal(SignalKind::interrupt()), signal(SignalKind::terminate()))
    else {
        return std::future::pending().await;
    };
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
    }
}

/// This process's place among the clients launch.py starts, from `SERVEVAL_PROCESS` as `INDEX/COUNT`.
pub fn process() -> (usize, usize) {
    let text = std::env::var("SERVEVAL_PROCESS").unwrap_or_default();
    let parsed = text
        .split_once('/')
        .and_then(|(index, count)| Some((index.parse().ok()?, count.parse().ok()?)));
    match parsed {
        Some((index, count)) if index < count => (index, count),
        None if text.is_empty() => (0, 1),
        _ => exit(&format!(
            "SERVEVAL_PROCESS=\"{text}\" does not parse, expected INDEX/COUNT like 0/2"
        )),
    }
}

/// How long after the last client finished connecting the window opens.
const BARRIER_DELAY: Duration = Duration::from_millis(100);

/// Run `run` starts 100ms after the last launched client finished connecting for it: each client writes its Unix
/// nanoseconds to `<SERVEVAL_BARRIER>/<run>-<process>` and waits for the files of every process, or starts now
/// without the variable.
pub fn start_at(run: usize) -> Instant {
    let Ok(directory) = std::env::var("SERVEVAL_BARRIER") else {
        return Instant::now();
    };
    let (index, count) = process();
    let directory = Path::new(&directory);
    let unix = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let mine = directory.join(format!("{run}-{index}"));
    let staged = mine.with_extension("tmp");
    let written = std::fs::write(&staged, unix().as_nanos().to_string()).and_then(|()| std::fs::rename(&staged, &mine));
    if let Err(error) = written {
        exit(&format!(
            "SERVEVAL_BARRIER=\"{}\" is not writable: {error}",
            directory.display()
        ));
    }
    let latest = loop {
        let read = (0..count).map(|other| std::fs::read_to_string(directory.join(format!("{run}-{other}"))).ok());
        if let Some(stamps) = read.collect::<Option<Vec<String>>>() {
            break stamps
                .iter()
                .filter_map(|stamp| stamp.parse::<u64>().ok())
                .max()
                .unwrap_or_default();
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let start = Duration::from_nanos(latest) + BARRIER_DELAY;
    Instant::now() + start.saturating_sub(unix())
}

/// Part `index` of `total` split evenly over `parts`, the first parts taking the remainder.
pub fn share(total: usize, index: usize, parts: usize) -> usize {
    total / parts + usize::from(index < total % parts)
}

/// Prints `message` and exits with 1.
pub fn exit(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}

/// Parses the command line; a bad value prints `--name="value" does not parse, expected ...` and exits with 1.
pub fn parse_cli<T: clap::Parser>() -> T {
    use clap::error::{ContextKind, ContextValue, ErrorKind};

    let error = match T::try_parse() {
        Ok(cli) => return cli,
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => error,
    };
    let context = |kind| match error.get(kind) {
        Some(ContextValue::String(text)) => text.as_str(),
        _ => "",
    };
    let expected = match (
        error.kind(),
        std::error::Error::source(&error),
        error.get(ContextKind::ValidValue),
    ) {
        (ErrorKind::ValueValidation, Some(expected), _) => expected.to_string(),
        (ErrorKind::InvalidValue, _, Some(ContextValue::Strings(values))) if !values.is_empty() => {
            format!("expected one of {}", values.join(", "))
        }
        (ErrorKind::InvalidValue, _, _) => "expected a non-empty value".into(),
        _ => {
            eprint!("{error}");
            std::process::exit(1)
        }
    };
    let flag = context(ContextKind::InvalidArg).split(' ').next().unwrap_or_default();
    eprintln!(
        "{flag}=\"{}\" does not parse, {expected}",
        context(ContextKind::InvalidValue)
    );
    std::process::exit(1)
}

/// Parses a 32-bit unsigned integer, or `random` as 32 bits from the OS entropy source.
pub fn parse_seed(text: &str) -> Option<Seed> {
    if text == "random" {
        return Some(Seed(std::hash::RandomState::new().build_hasher().finish() as u32));
    }
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok().map(Seed)).flatten()
}

/// Parses a thread count like `8`, or `0` as `all_cores`.
pub fn parse_threads(text: &str, all_cores: NonZeroUsize) -> Option<Threads> {
    match text {
        "0" => Some(Threads(all_cores)),
        _ => parse_count(text).and_then(NonZeroUsize::new).map(Threads),
    }
}

/// Parses a count like `10000` or `10M`: `K`, `M`, `G`, `T` are powers of 1000 in any case, and zero is `None`.
pub fn parse_count(text: &str) -> Option<usize> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let exponent = ["", "k", "m", "g", "t"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit))?;
    let count = text[..split]
        .parse::<usize>()
        .ok()?
        .checked_mul(1000usize.pow(exponent as u32))?;
    (count != 0).then_some(count)
}

fn parse_digits(text: &str) -> Option<u64> {
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok()).flatten().filter(|&count| count != 0)
}

/// Parses a duration like `200ms` or `10s`; a bare number, a fraction or zero is `None`.
pub fn parse_duration(text: &str) -> Option<Duration> {
    match text.strip_suffix("ms") {
        Some(count) => parse_digits(count).map(Duration::from_millis),
        None => parse_digits(text.strip_suffix('s')?).map(Duration::from_secs),
    }
}

/// Parses a size like `4096`, `64KB` or `16GB`, in powers of 1024; zero is `None`.
pub fn parse_size(text: &str) -> Option<u64> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let level = ["", "kb", "mb", "gb", "tb"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit))?;
    let bytes = text[..split].parse::<u64>().ok()?.checked_mul(1 << (10 * level))?;
    (bytes != 0).then_some(bytes)
}

/// Spells a size the way `parse_size` reads it: `16GB`, `1000`.
pub fn spell_size(bytes: u64) -> String {
    let (mut count, mut unit) = (bytes, "");
    for larger in ["KB", "MB", "GB", "TB"] {
        if count == 0 || count % 1024 != 0 {
            break;
        }
        count /= 1024;
        unit = larger;
    }
    format!("{count}{unit}")
}

/// Spells a duration the way `parse_duration` reads it: `1s`, `1500ms`.
pub fn spell_duration(duration: Duration) -> String {
    let milliseconds = duration.as_millis();
    match milliseconds % 1000 {
        0 => format!("{}s", milliseconds / 1000),
        _ => format!("{milliseconds}ms"),
    }
}

/// A 32-bit run seed, an integer or drawn from the OS for `random`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seed(pub u32);

impl From<Seed> for u64 {
    fn from(seed: Seed) -> u64 {
        u64::from(seed.0)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A thread count; `0` in the variable resolves to every core when read, so it is never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Threads(pub NonZeroUsize);

impl Threads {
    /// Every core in this process's affinity mask, at least one.
    pub fn all_cores() -> Self {
        Self(std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }
}

impl fmt::Display for Threads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Parses `DEVICE=ADDRESS[+ADDRESS...]` or `ADDRESS[+ADDRESS...]`.
pub fn parse_interface(text: &str) -> Option<Interface> {
    let (device, addresses) = match text.split_once('=') {
        Some((device, addresses)) if !device.is_empty() => (Some(device.to_owned()), addresses),
        Some(_) => return None,
        None => (None, text),
    };
    let addresses = addresses
        .split('+')
        .map(|address| address.parse().ok())
        .collect::<Option<Vec<IpAddr>>>()?;
    Some(Interface { device, addresses })
}

/// `parse_count` for clap.
pub fn parse_count_flag(text: &str) -> Result<usize, String> {
    parse_count(text).ok_or_else(|| "expected a positive count like 1000 or 10M".into())
}

/// `parse_threads` for clap, with `0` resolved to every available core.
pub fn parse_threads_flag(text: &str) -> Result<Threads, String> {
    parse_threads(text, Threads::all_cores().0).ok_or_else(|| "expected a count, 0 for all cores".into())
}

/// `parse_size` for clap.
pub fn parse_size_flag(text: &str) -> Result<u64, String> {
    parse_size(text).ok_or_else(|| "expected a size like 4096, 64KB or 16GB".into())
}

/// `parse_seed` for clap.
pub fn parse_seed_flag(text: &str) -> Result<Seed, String> {
    parse_seed(text).ok_or_else(|| "expected an unsigned integer or random".into())
}

/// `parse_duration` for clap.
pub fn parse_duration_flag(text: &str) -> Result<Duration, String> {
    parse_duration(text).ok_or_else(|| "expected a duration like 200ms or 10s".into())
}

/// `parse_digits` for clap, as a TCP port from 1 to 65535.
pub fn parse_port_flag(text: &str) -> Result<u16, String> {
    parse_digits(text)
        .and_then(|port| u16::try_from(port).ok())
        .ok_or_else(|| "expected a port from 1 to 65535".into())
}

/// `parse_interface` for clap.
pub fn parse_interface_flag(text: &str) -> Result<Interface, String> {
    parse_interface(text).ok_or_else(|| "expected DEVICE=ADDRESS or ADDRESS, extra addresses joined by +".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar() {
        assert_eq!(parse_count("10K"), Some(10_000));
        assert_eq!(parse_count("0"), None);
        assert_eq!(parse_size("64KB"), Some(65_536));
        assert_eq!(parse_duration("200ms"), Some(Duration::from_millis(200)));
        assert_eq!(parse_duration("2m"), None);
        let interface = parse_interface("ens5=10.0.1.16+10.0.1.17").unwrap();
        assert_eq!(interface.device.as_deref(), Some("ens5"));
        assert_eq!(interface.addresses.len(), 2);
    }
}
