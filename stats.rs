//! Latency histograms that merge exactly across processes, the call schedule, and cgroup and process counters.

use std::{
    collections::BTreeMap,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SUB_BINS: usize = 16;
/// Bits of the largest value with an exact bin: every latency below 32 ns gets one.
const EXACT_BITS: u32 = 5;
const BINS: usize = (64 - EXACT_BITS as usize + 2) * SUB_BINS;

/// Nanosecond latencies in 16 linear bins per power of two, as UCall's driver keeps them: percentiles read at most
/// 1/16 high, and the same bins in `stats.py` let reports from every language merge.
#[derive(Clone)]
pub struct Histogram {
    pub bins: Vec<u64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Self { bins: vec![0; BINS] }
    }
}

impl Histogram {
    pub fn bin_of(nanoseconds: u64) -> usize {
        if nanoseconds < 2 * SUB_BINS as u64 {
            return nanoseconds as usize;
        }
        let shift = 64 - nanoseconds.leading_zeros() - EXACT_BITS;
        shift as usize * SUB_BINS + (nanoseconds >> shift) as usize
    }

    pub fn upper_edge(bin: usize) -> u64 {
        if bin < 2 * SUB_BINS {
            return bin as u64;
        }
        let shift = bin / SUB_BINS - 1;
        let leading = (bin % SUB_BINS + SUB_BINS) as u64;
        ((leading + 1) << shift).wrapping_sub(1)
    }

    pub fn record(&mut self, latency: Duration) {
        self.bins[Self::bin_of(latency.as_nanos().min(u128::from(u64::MAX)) as u64)] += 1;
    }

    pub fn merge(&mut self, other: &Self) {
        self.bins
            .iter_mut()
            .zip(&other.bins)
            .for_each(|(bin, other)| *bin += other);
    }

    /// The upper edge in nanoseconds of the bin holding the nearest-rank `fraction`, like 0.999 for p99.9.
    pub fn percentile(&self, fraction: f64) -> u64 {
        let total: u64 = self.bins.iter().sum();
        if total == 0 {
            return 0;
        }
        let rank = ((fraction * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0;
        let bin = self.bins.iter().position(|&count| {
            cumulative += count;
            cumulative >= rank
        });
        Self::upper_edge(bin.unwrap_or(BINS - 1))
    }

    /// The non-empty bins as `[bin, count]` pairs.
    pub fn sparse(&self) -> Vec<[u64; 2]> {
        let bins = self.bins.iter().enumerate().filter(|(_, &count)| count != 0);
        bins.map(|(bin, &count)| [bin as u64, count]).collect()
    }
}

/// Why a call failed.
#[derive(Clone, Copy, Debug)]
pub enum Failure {
    /// Any transport failure but a refusal or a timeout, while opening a connection or on an open one.
    Connect,
    Refused,
    Timeout,
    /// A status other than success.
    Status,
    /// A successful status with a reply that differs from the expected one.
    WrongReply,
}

impl Failure {
    pub fn of_io(error: &io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::ConnectionRefused => Self::Refused,
            io::ErrorKind::TimedOut => Self::Timeout,
            _ => Self::Connect,
        }
    }

    /// The class of the first I/O error in `error`'s chain of sources, `Connect` without one.
    pub fn of(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut source = Some(error);
        while let Some(error) = source {
            if let Some(error) = error.downcast_ref::<io::Error>() {
                return Self::of_io(error);
            }
            source = error.source();
        }
        Self::Connect
    }
}

/// Failed calls by `Failure`.
#[derive(Clone, Copy, Default, serde::Serialize)]
pub struct Failures {
    pub connect: u64,
    pub refused: u64,
    pub timeout: u64,
    pub status: u64,
    pub wrong_reply: u64,
}

impl Failures {
    pub fn count(&mut self, failure: Failure) {
        *match failure {
            Failure::Connect => &mut self.connect,
            Failure::Refused => &mut self.refused,
            Failure::Timeout => &mut self.timeout,
            Failure::Status => &mut self.status,
            Failure::WrongReply => &mut self.wrong_reply,
        } += 1;
    }

    pub fn total(&self) -> u64 {
        self.connect + self.refused + self.timeout + self.status + self.wrong_reply
    }

    pub fn merge(&mut self, other: &Self) {
        self.connect += other.connect;
        self.refused += other.refused;
        self.timeout += other.timeout;
        self.status += other.status;
        self.wrong_reply += other.wrong_reply;
    }
}

/// What a set of calls returned.
#[derive(Clone, Default)]
pub struct Tally {
    pub latencies: Histogram,
    /// How late each measured call left against its schedule.
    pub lateness: Histogram,
    /// Valid replies inside the measured window.
    pub calls: u64,
    /// Failed or mismatched replies inside the measured window.
    pub failures: Failures,
    /// Measured calls the schedule owed that the window closed on before they left.
    pub unsent: u64,
    /// Every call, warmup included, which the server's CPU time covers.
    pub all_calls: u64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
}

impl Tally {
    pub fn merge(&mut self, other: &Self) {
        self.latencies.merge(&other.latencies);
        self.lateness.merge(&other.lateness);
        self.calls += other.calls;
        self.failures.merge(&other.failures);
        self.unsent += other.unsent;
        self.all_calls += other.all_calls;
        self.request_body_bytes += other.request_body_bytes;
        self.response_body_bytes += other.response_body_bytes;
    }
}

/// When calls go out: a warmup, then the measured window, closed-loop or on one global arrival schedule.
#[derive(Clone, Copy)]
pub struct Window {
    pub begin: Instant,
    pub measured_from: Instant,
    pub until: Instant,
    pub calls_per_second: Option<usize>,
    /// Slots sharing the schedule, each with one call outstanding.
    pub slots: usize,
}

impl Window {
    pub fn new(
        begin: Instant,
        warmup: Duration,
        time_limit: Duration,
        calls_per_second: Option<usize>,
        slots: usize,
    ) -> Self {
        let measured_from = begin + warmup;
        Self {
            begin,
            measured_from,
            until: measured_from + time_limit,
            calls_per_second,
            slots,
        }
    }

    pub fn seconds(&self) -> f64 {
        (self.until - self.measured_from).as_secs_f64()
    }

    /// The measured window's first and last moment in Unix milliseconds, to find it in other recordings.
    pub fn unix_ms(&self) -> [u64; 2] {
        let (unix, now) = (SystemTime::now(), Instant::now());
        [self.measured_from, self.until].map(|moment| {
            let wall = match moment.checked_duration_since(now) {
                Some(ahead) => unix + ahead,
                None => unix - (now - moment),
            };
            wall.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
        })
    }

    /// When open-loop call `sequence` is due, `None` in a closed loop.
    fn scheduled(&self, sequence: usize) -> Option<Instant> {
        let rate = self.calls_per_second?;
        Some(self.begin + Duration::from_secs_f64(sequence as f64 / rate as f64))
    }
}

/// One call outstanding at a time on the window's schedule: `next` waits until a call is due, `record` keeps its
/// outcome. Open-loop latency counts from the intended send, so a stalled server cannot hide its queue, and the calls
/// a late slot never sent count as unsent, so a slow client cannot pass for a fast server.
pub struct Slot {
    window: Window,
    slot: usize,
    arrival: usize,
    pub tally: Tally,
}

impl Slot {
    pub fn new(window: Window, slot: usize) -> Self {
        Self {
            window,
            slot,
            arrival: 0,
            tally: Tally::default(),
        }
    }

    /// The intended send time and sequence number of the next call, or `None` once the window closed.
    pub async fn next(&mut self) -> Option<(Instant, usize)> {
        let window = self.window;
        if self.arrival == 0 {
            tokio::time::sleep_until(window.begin.into()).await;
        }
        let sequence = self.slot + self.arrival * window.slots;
        self.arrival += 1;
        let intended = window.scheduled(sequence).unwrap_or_else(Instant::now);
        if intended >= window.until || Instant::now() >= window.until {
            let owed = (sequence..)
                .step_by(window.slots)
                .map_while(|late| window.scheduled(late));
            let owed = owed.take_while(|&late| late < window.until);
            self.tally.unsent += owed.filter(|&late| late >= window.measured_from).count() as u64;
            return None;
        }
        if window.calls_per_second.is_some() {
            tokio::time::sleep_until(intended.into()).await;
        }
        if intended >= window.measured_from {
            self.tally.lateness.record(intended.elapsed());
        }
        Some((intended, sequence))
    }

    /// Keeps a call's outcome: the body bytes sent and received, or why it failed.
    pub fn record(&mut self, intended: Instant, outcome: Result<(u64, u64), Failure>) {
        let tally = &mut self.tally;
        tally.all_calls += 1;
        if intended < self.window.measured_from {
            return;
        }
        match outcome {
            Ok((sent, received)) => {
                tally.calls += 1;
                tally.request_body_bytes += sent;
                tally.response_body_bytes += received;
                tally.latencies.record(intended.elapsed());
            }
            Err(failure) => tally.failures.count(failure),
        }
    }
}

/// The `/stats` reply: this process's cgroup v2 counters, which cover every process of its systemd unit and the
/// kernel's socket memory, with the unit's cores and the box's `tcp_active_opens`. Each server runs in its own unit,
/// so a reply covers one server. A counter is `None` where its file is missing, as outside a cgroup v2 hierarchy, and
/// a field a server leaves out reads as missing.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct UnitStats {
    /// The cgroup's `usage_usec` from `cpu.stat`.
    pub cpu_microseconds: Option<u64>,
    /// The cgroup's `memory.current`.
    pub memory_current_bytes: Option<u64>,
    /// The cgroup's `memory.peak` since the previous `/stats`, as `MemoryPeak` reads it.
    pub memory_peak_bytes: Option<u64>,
    /// The smallest `memory.max` of the cgroup and its ancestors.
    pub memory_limit: Option<u64>,
    /// Cores in the `sched_getaffinity` mask.
    pub cores: u64,
    /// The `sched_getaffinity` mask as a cpuset list, like `8-9`.
    pub cpu_list: String,
    /// The cgroup's last path component, like `serveval-server-axum-8545-0.service`.
    pub unit: Option<String>,
    /// The box's `tcp_active_opens`, not the unit's.
    pub tcp_active_opens: Option<u64>,
    /// The serving library's own counters summed over its event loops, by name, like UCall's `kernel_enters`, for
    /// servers whose library keeps them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_loop_counters: Option<BTreeMap<String, u64>>,
}

/// The `/stats` replies of every target as one server's.
#[derive(Clone, Default)]
pub struct ServerStats {
    pub cpu_microseconds: Option<u64>,
    pub memory_current_bytes: Option<u64>,
    pub memory_peak_bytes: Option<u64>,
    pub memory_limit: Option<u64>,
    pub cores: u64,
    /// The union of the units' cpuset lists.
    pub cpu_list: String,
    /// Each unit once, in target order.
    pub units: Vec<String>,
    pub tcp_active_opens: Option<u64>,
    /// The counted units' event-loop counters summed by name, missing unless every counted unit keeps them.
    pub event_loop_counters: Option<BTreeMap<String, u64>>,
}

impl UnitStats {
    /// The replies of every target as one server's: counters summed, `cpu_list` unioned, and `units` listing each
    /// unit once. A unit serving several targets, like UCall's one server on both network interfaces, answers each
    /// alike, so only its first reply counts: a second would count its cores twice and see its memory peak just
    /// reset. A counter missing from any counted reply is missing, and `tcp_active_opens`, the box's, is the largest.
    pub fn sum(all: &[UnitStats]) -> ServerStats {
        let mut counted: Vec<&UnitStats> = Vec::new();
        let mut units: Vec<String> = Vec::new();
        for reply in all {
            if let Some(unit) = &reply.unit {
                if units.contains(unit) {
                    continue;
                }
                units.push(unit.clone());
            }
            counted.push(reply);
        }
        let total =
            |field: fn(&UnitStats) -> Option<u64>| counted.iter().map(|stats| field(stats)).sum::<Option<u64>>();
        let cpus = counted.iter().flat_map(|stats| parse_cpus(&stats.cpu_list));
        ServerStats {
            cpu_microseconds: total(|stats| stats.cpu_microseconds),
            memory_current_bytes: total(|stats| stats.memory_current_bytes),
            memory_peak_bytes: total(|stats| stats.memory_peak_bytes),
            memory_limit: total(|stats| stats.memory_limit),
            cores: counted.iter().map(|stats| stats.cores).sum(),
            cpu_list: spell_cpus(cpus.collect()),
            units,
            tcp_active_opens: all
                .iter()
                .map(|stats| stats.tcp_active_opens)
                .collect::<Option<Vec<u64>>>()
                .and_then(|opens| opens.into_iter().max()),
            event_loop_counters: counted.iter().try_fold(BTreeMap::new(), |mut sums, stats| {
                for (name, value) in stats.event_loop_counters.as_ref()? {
                    *sums.entry(name.clone()).or_insert(0) += value;
                }
                Some(sums)
            }),
        }
    }
}

/// Each event-loop counter's growth from `before` to `after`, the counters of one server sampled around a window.
pub fn counter_growth(
    before: &Option<BTreeMap<String, u64>>,
    after: &Option<BTreeMap<String, u64>>,
) -> Option<BTreeMap<String, u64>> {
    let (before, after) = (before.as_ref()?, after.as_ref()?);
    let growth = |(name, value): (&String, &u64)| {
        let earlier = before.get(name).copied().unwrap_or(0);
        (name.clone(), value.saturating_sub(earlier))
    };
    Some(after.iter().map(growth).collect())
}

/// Each nonzero counter of `growth` per request the loops parsed over the same span; missing without requests.
pub fn per_request(growth: &Option<BTreeMap<String, u64>>) -> Option<BTreeMap<String, f64>> {
    let growth = growth.as_ref()?;
    let requests = growth.get("requests").copied().filter(|&requests| requests > 0)? as f64;
    let nonzero = growth.iter().filter(|(_, &value)| value > 0);
    Some(
        nonzero
            .map(|(name, &value)| (name.clone(), value as f64 / requests))
            .collect(),
    )
}

/// The `/stats` fields `serveval.proto`'s `StatsReply` has no field for, which the gRPC pairs send as response
/// metadata, each under its name with dashes for underscores.
#[cfg(feature = "tonic")]
const METADATA_FIELDS: [&str; 3] = ["memory-peak-bytes", "cpu-list", "unit"];

#[cfg(feature = "tonic")]
impl UnitStats {
    /// The gRPC `Stats` reply: `StatsReply`, whose `memory_bytes` carries `memory_current_bytes`, and the fields it
    /// lacks as metadata.
    pub fn into_response(self) -> tonic::Response<crate::proto::StatsReply> {
        let mut response = tonic::Response::new(crate::proto::StatsReply {
            cpu_microseconds: self.cpu_microseconds,
            memory_bytes: self.memory_current_bytes,
            cores: self.cores,
            memory_limit: self.memory_limit,
            tcp_active_opens: self.tcp_active_opens,
        });
        let values = [
            self.memory_peak_bytes.map(|peak| peak.to_string()),
            Some(self.cpu_list),
            self.unit,
        ];
        for (name, value) in METADATA_FIELDS.into_iter().zip(values) {
            if let Some(value) = value.and_then(|value| value.parse().ok()) {
                response.metadata_mut().insert(name, value);
            }
        }
        response
    }

    /// Reads a reply `into_response` made; a field missing from it reads as missing.
    pub fn from_response(response: tonic::Response<crate::proto::StatsReply>) -> Self {
        let text = |name: &str| {
            let value = response.metadata().get(name)?;
            value.to_str().ok().map(str::to_owned)
        };
        let [peak, cpu_list, unit] = METADATA_FIELDS.map(text);
        let reply = response.get_ref();
        Self {
            cpu_microseconds: reply.cpu_microseconds,
            memory_current_bytes: reply.memory_bytes,
            memory_peak_bytes: peak.and_then(|peak| peak.parse().ok()),
            memory_limit: reply.memory_limit,
            cores: reply.cores,
            cpu_list: cpu_list.unwrap_or_default(),
            unit,
            tcp_active_opens: reply.tcp_active_opens,
            event_loop_counters: None,
        }
    }
}

/// The cores of a cpuset list like `8-9,104-105`, in order; a malformed range is skipped.
pub fn parse_cpus(text: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in text.trim().split(',').filter(|part| !part.is_empty()) {
        let (first, last) = part.split_once('-').unwrap_or((part, part));
        if let (Ok(first), Ok(last)) = (first.parse::<usize>(), last.parse::<usize>()) {
            cpus.extend(first..=last);
        }
    }
    cpus
}

/// Spells cores as a cpuset list like `8-9,104-105`, the way `parse_cpus` reads it.
pub fn spell_cpus(mut cpus: Vec<usize>) -> String {
    cpus.sort_unstable();
    cpus.dedup();
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for cpu in cpus {
        match ranges.last_mut() {
            Some((_, last)) if *last + 1 == cpu => *last = cpu,
            _ => ranges.push((cpu, cpu)),
        }
    }
    let spelled = ranges.iter().map(|&(first, last)| match first == last {
        true => first.to_string(),
        false => format!("{first}-{last}"),
    });
    spelled.collect::<Vec<_>>().join(",")
}

/// The cores this process may run on: its `sched_getaffinity` mask, or every core where the platform has none.
pub fn allowed_cpus() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    {
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let read = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
        if read == 0 {
            let count = 8 * std::mem::size_of::<libc::cpu_set_t>();
            return (0..count)
                .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
                .collect();
        }
    }
    (0..std::thread::available_parallelism().map_or(1, |cores| cores.get())).collect()
}

/// This process's systemd unit, the last component of its cgroup v2 path like
/// `serveval-server-axum-8545-0.service`, or `None` outside a cgroup v2 hierarchy or at its root.
pub fn unit_name() -> Option<String> {
    let directory = cgroup_directory()?;
    if directory == Path::new("/sys/fs/cgroup") {
        return None;
    }
    Some(directory.file_name()?.to_string_lossy().into_owned())
}

/// The cgroup's `memory.peak` since the previous read, through one descriptor this process holds open.
///
/// Writing to an open `memory.peak` resets the peak only that descriptor reports, to the current usage, which Linux
/// allows since 6.12; so a client's `/stats` before and after its window bracket that window's peak. The descriptor
/// opens on the first read and again on every read until it opens: `memory.peak` belongs to root unless the launcher
/// hands it to the server's user, and a peak that cannot be reset would cover the unit's whole life, so it reads as
/// `None` instead.
pub struct MemoryPeak {
    // Two `/stats` replies served at once must not interleave one's read with the other's reset.
    file: Mutex<Option<File>>,
}

impl MemoryPeak {
    const fn new() -> Self {
        Self { file: Mutex::new(None) }
    }

    /// The peak since the previous `take`, or since the cgroup started for the first, then resets it.
    pub fn take(&self) -> Option<u64> {
        use std::os::unix::fs::FileExt;

        let mut file = self.file.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if file.is_none() {
            let path = cgroup_directory()?.join("memory.peak");
            *file = Some(File::options().read(true).write(true).open(path).ok()?);
        }
        let file = file.as_ref()?;
        let mut text = [0u8; 32];
        let read = file.read_at(&mut text, 0).ok()?;
        let peak = std::str::from_utf8(&text[..read]).ok()?.trim().parse().ok()?;
        // The kernel takes any non-empty write as a reset and ignores the offset.
        file.write_at(b"reset\n", 0).ok()?;
        Some(peak)
    }
}

/// The one `MemoryPeak` of this process, which every `/stats` reply reads.
pub static MEMORY_PEAK: MemoryPeak = MemoryPeak::new();

/// The `/stats` reply of this process, as `UnitStats` describes its fields.
pub fn unit_stats() -> UnitStats {
    let cpus = allowed_cpus();
    let mut stats = UnitStats {
        memory_peak_bytes: MEMORY_PEAK.take(),
        cores: cpus.len() as u64,
        cpu_list: spell_cpus(cpus),
        unit: unit_name(),
        tcp_active_opens: tcp_active_opens(),
        ..UnitStats::default()
    };
    let Some(directory) = cgroup_directory() else {
        return stats;
    };
    let read = |name: &str| std::fs::read_to_string(directory.join(name)).ok();
    stats.cpu_microseconds = read("cpu.stat").and_then(|stat| {
        stat.lines()
            .find_map(|line| line.strip_prefix("usage_usec "))
            .and_then(|value| value.trim().parse().ok())
    });
    stats.memory_current_bytes = read("memory.current").and_then(|value| value.trim().parse().ok());
    stats.memory_limit = directory
        .ancestors()
        .take_while(|ancestor| ancestor.starts_with("/sys/fs/cgroup"))
        .filter_map(|ancestor| {
            std::fs::read_to_string(ancestor.join("memory.max"))
                .ok()?
                .trim()
                .parse()
                .ok()
        })
        .min();
    stats
}

/// TCP connections this box opened since boot, every process together, from `/proc/net/snmp`.
pub fn tcp_active_opens() -> Option<u64> {
    let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut tcp = snmp.lines().filter_map(|line| line.strip_prefix("Tcp:"));
    let (names, values) = (tcp.next()?, tcp.next()?);
    let position = names.split_whitespace().position(|name| name == "ActiveOpens")?;
    values.split_whitespace().nth(position)?.parse().ok()
}

/// The box's TCP sockets in use and orphaned, from `/proc/net/sockstat`.
pub fn sockets_in_use() -> Option<[u64; 2]> {
    let sockstat = std::fs::read_to_string("/proc/net/sockstat").ok()?;
    let fields: Vec<&str> = sockstat
        .lines()
        .find_map(|line| line.strip_prefix("TCP:"))?
        .split_whitespace()
        .collect();
    let read = |name: &str| {
        fields
            .iter()
            .position(|&field| field == name)
            .and_then(|at| fields.get(at + 1)?.parse().ok())
    };
    Some([read("inuse")?, read("orphan")?])
}

/// The box's sockets and a server's memory, sampled until closed connections are released.
pub type Release = (Option<[u64; 2]>, Option<u64>);
/// Pause between two `Release` samples.
pub const SETTLE_INTERVAL: Duration = Duration::from_millis(100);
/// Most `Release` samples one reconnect waits for.
pub const SETTLE_ROUNDS: usize = 100;

/// Whether nothing moved between two samples: the same sockets, and memory within 1%.
pub fn settled(earlier: Release, later: Release) -> bool {
    let memory = match (earlier.1, later.1) {
        (Some(earlier), Some(later)) => earlier.abs_diff(later) * 100 <= earlier,
        (earlier, later) => earlier == later,
    };
    earlier.0 == later.0 && memory
}

/// The `/stats` body.
pub fn unit_json() -> String {
    serde_json::to_string(&unit_stats()).unwrap_or_default()
}

fn cgroup_directory() -> Option<PathBuf> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = membership.lines().find_map(|line| line.strip_prefix("0::"))?;
    Some(PathBuf::from("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
}

/// User and system CPU time this process has used.
pub fn process_cpu() -> Duration {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let time = |value: libc::timeval| Duration::new(value.tv_sec as u64, value.tv_usec as u32 * 1000);
    time(usage.ru_utime) + time(usage.ru_stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_cover_every_latency() {
        assert_eq!(Histogram::bin_of(31), 31);
        assert_eq!(Histogram::bin_of(32), 32);
        assert_eq!(Histogram::bin_of(u64::MAX), BINS - 1);
        assert_eq!(Histogram::upper_edge(BINS - 1), u64::MAX);
        for nanoseconds in [1, 100, 1_000_000, 1 << 40] {
            let edge = Histogram::upper_edge(Histogram::bin_of(nanoseconds));
            assert!(edge >= nanoseconds && edge - nanoseconds <= nanoseconds / 16);
        }
    }

    #[test]
    fn one_unit_serving_two_targets_counts_once() {
        assert_eq!(spell_cpus(parse_cpus("104-105,8,9")), "8-9,104-105");
        let unit = |name: &str, cpu_list: &str| UnitStats {
            cpu_microseconds: Some(10),
            cores: 2,
            cpu_list: cpu_list.into(),
            unit: Some(name.into()),
            ..UnitStats::default()
        };
        let one = UnitStats::sum(&[unit("a.service", "8-9"), unit("a.service", "8-9")]);
        assert_eq!((one.cores, one.cpu_microseconds, one.units.len()), (2, Some(10), 1));
        let two = UnitStats::sum(&[unit("a.service", "8-9"), unit("b.service", "104-105")]);
        assert_eq!((two.cores, two.cpu_list.as_str()), (4, "8-9,104-105"));
    }

    #[cfg(feature = "tonic")]
    #[test]
    fn grpc_replies_carry_every_field() {
        let sent = UnitStats {
            memory_current_bytes: Some(3),
            memory_peak_bytes: Some(5),
            cores: 2,
            cpu_list: "8-9".into(),
            unit: Some("serveval-server-tonic-8545-0.service".into()),
            ..UnitStats::default()
        };
        let read = UnitStats::from_response(sent.clone().into_response());
        assert_eq!(serde_json::to_value(read).unwrap(), serde_json::to_value(sent).unwrap());
    }

    #[tokio::test]
    async fn late_slots_count_the_measured_calls_they_owed() {
        let begin = Instant::now() - Duration::from_secs(1);
        let window = Window::new(
            begin,
            Duration::from_millis(20),
            Duration::from_millis(80),
            Some(100),
            2,
        );
        let mut slot = Slot::new(window, 1);
        assert!(slot.next().await.is_none());
        assert_eq!(slot.tally.unsent, 4, "calls due at 30, 50, 70 and 90ms");
    }
}
