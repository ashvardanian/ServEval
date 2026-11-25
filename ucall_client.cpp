/**
 *  @file ucall_client.cpp
 *  @brief UCall's C++ client: ForkUnion pools spawned near each interface's memory domain, one UCall client per
 *      worker, living across configurations so its connections can stay open under `--between-runs keep`.
 *
 *  Each worker opens its share of the connections up front by concurrent health checks, then drives its slots on the
 *  schedule `stats.rs` keeps, waiting on the network exactly until the next call is due. Reports carry the fields
 *  `results.rs` writes, so `launch.py` and `scripts/plot.py` read them alike. UCall's client binds neither a device
 *  nor an address, so the kernel routes each target by its subnet.
 */
#include <cmath>   // `std::ceil`
#include <cstdio>  // `std::FILE`, `std::fopen`, `std::fwrite`, `std::fflush`, `std::rename`, `popen`, `pclose`
#include <cstdlib> // `std::getenv`

#include <sys/resource.h> // `getrusage`, `RUSAGE_SELF`
#include <sys/utsname.h>  // `uname`
#include <unistd.h>       // `sysconf`, `fsync`, `fileno`, `getpid`

#include <algorithm>   // `std::find`, `std::max`, `std::min`
#include <bit>         // `std::bit_width`
#include <chrono>      // `std::chrono::steady_clock`, `std::chrono::system_clock`
#include <filesystem>  // `std::filesystem::directory_iterator`, `std::filesystem::create_directories`
#include <functional>  // `std::greater`
#include <optional>    // `std::optional`
#include <queue>       // `std::priority_queue`
#include <set>         // `std::set`
#include <string>      // `std::string`
#include <string_view> // `std::string_view`
#include <thread>      // `std::this_thread::sleep_until`
#include <vector>      // `std::vector`

#include <yyjson.h> // `yyjson_read`, `yyjson_obj_get`, `yyjson_doc_free`

#include <forkunion/forkunion.h> // `fu_pool_new`, `fu_pool_spawn_near_memory_domain`, `fu_pool_unsafe_for_threads`

#include "ucall/client.hpp" // `uc::client_t`, `uc::client_call_t`, `uc::response_t`
#include "serveval.hpp"     // `command_line_t`, `tool_parts_t`, `tool_reply`, `share`

namespace uc = unum::ucall;

namespace serveval {

using steady_t = std::chrono::steady_clock;
using time_point_t = steady_t::time_point;

flag_t const flags_k[] {
    {"--threads", "0", "Workers, 0 for every core in the affinity mask."},
    {"--interfaces", "127.0.0.1", "Local interfaces as DEVICE=ADDRESS or ADDRESS, extra addresses joined by +."},
    {"--port", "8545", "Port every server listens on."},
    {"--security", "none", "none, noise-xx or noise-ik."},
    {"--data-dir", "data", "Directory holding the noise keys that keys.sh writes."},
    {"--seed", "42", "Seed of every payload, or random."},
    {"--targets", "127.0.0.1", "Server addresses, paired with --interfaces by position."},
    {"--connections", "64", "Connection counts to sweep, like 64,10K,1M."},
    {"--workloads", "tool,context,fanout", "Workloads: tool, context and fanout."},
    {"--variants", "plain,batched", "Variants: plain and batched."},
    {"--phases", "steady,handshake", "Phases: steady and handshake."},
    {"--parsing", "body", "Tool call shapes: raw, body and full."},
    {"--body-bytes", "256KB", "Context upload sizes to sweep, like 256KB,4MB,64MB."},
    {"--in-flight", "", "Calls outstanding at once, one per connection when unset."},
    {"--calls-per-second", "", "Scheduled calls per second across this process; unset runs a closed loop."},
    {"--width", "8", "Upstream calls per fan-out."},
    {"--time-limit", "10s", "Measured time per configuration."},
    {"--warmup", "1s", "Unmeasured calls before each measured window."},
    {"--memory-limit", "", "Client memory budget, at most the cgroup's."},
    {"--between-runs", "reconnect", "keep or reconnect."},
    {"--output", "results", "Directory for <framework>-<config-hash>.json reports."},
};

std::string_view const workload_names_k[] {"tool", "context", "fanout"};
std::string_view const variant_names_k[] {"plain", "batched"};
std::string_view const phase_names_k[] {"steady", "handshake"};
std::string_view const parsing_names_k[] {"raw", "body", "full"};
std::string_view const between_runs_names_k[] {"keep", "reconnect"};
enum workload_t : std::size_t { tool_k, context_k, fanout_k };
enum variant_t : std::size_t { plain_k, batched_k };
enum phase_t : std::size_t { steady_k, handshake_k };
enum parsing_t : std::size_t { raw_k, body_k, full_k };
enum between_runs_t : std::size_t { keep_k, reconnect_k };
/** UCall's default call budget, which bounds every wait for a completion. */
constexpr std::chrono::microseconds call_timeout_k = std::chrono::seconds(30);
/** Pause between two samples of the box's sockets and the servers' memory, and the most samples one reconnect waits
 *  for, as `stats.rs` settles. */
constexpr std::chrono::milliseconds settle_interval_k {100};
constexpr std::size_t settle_rounds_k = 100;

std::optional<std::size_t> parse_workload(std::string_view text) noexcept {
    return parse_choice(text, workload_names_k);
}
std::optional<std::size_t> parse_variant(std::string_view text) noexcept { return parse_choice(text, variant_names_k); }
std::optional<std::size_t> parse_phase(std::string_view text) noexcept { return parse_choice(text, phase_names_k); }
std::optional<std::size_t> parse_parsing(std::string_view text) noexcept { return parse_choice(text, parsing_names_k); }
std::optional<std::size_t> parse_between_runs(std::string_view text) noexcept {
    return parse_choice(text, between_runs_names_k);
}

/** Every flag the client reads, resolved. */
struct settings_t {
    std::size_t threads = 0;
    std::vector<interface_t> interfaces;
    std::uint16_t port = 0;
    std::size_t security_choice = 0;
    ucall_security_t security = ucall_security_none_k;
    std::string data_dir;
    std::uint32_t seed = 0;
    std::vector<std::string> targets;
    std::vector<std::size_t> connections, workloads, variants, phases, parsing;
    std::vector<std::uint64_t> body_bytes;
    std::optional<std::size_t> in_flight, calls_per_second;
    std::size_t width = 0;
    std::chrono::milliseconds time_limit {}, warmup {};
    std::optional<std::uint64_t> memory_limit;
    std::size_t between_runs = 0;
    std::string output;

    explicit settings_t(command_line_t const& line) noexcept {
        std::string_view const counts = "a positive count like 1000 or 10M";
        threads = line.read("--threads", parse_threads, "a count, 0 for all cores");
        interfaces = line.read_list<interface_t>("--interfaces", parse_interface, "DEVICE=ADDRESS or ADDRESS");
        port = line.read("--port", parse_port, "a port from 1 to 65535");
        security_choice = line.read("--security", parse_security, "one of none, noise-xx, noise-ik, tls");
        security = security_of(security_choice);
        data_dir = line.text("--data-dir");
        seed = line.read("--seed", parse_seed, "an unsigned integer or random");
        targets = line.read_list<std::string>("--targets", parse_address, "an IP address");
        connections = line.read_list<std::size_t>("--connections", parse_count, counts);
        workloads = line.read_list<std::size_t>("--workloads", parse_workload, "one of tool, context, fanout");
        variants = line.read_list<std::size_t>("--variants", parse_variant, "one of plain, batched");
        phases = line.read_list<std::size_t>("--phases", parse_phase, "one of steady, handshake");
        parsing = line.read_list<std::size_t>("--parsing", parse_parsing, "one of raw, body, full");
        body_bytes = line.read_list<std::uint64_t>("--body-bytes", parse_size, "a size like 4096, 64KB or 16GB");
        if (!line.text("--in-flight").empty()) in_flight = line.read("--in-flight", parse_count, counts);
        if (!line.text("--calls-per-second").empty())
            calls_per_second = line.read("--calls-per-second", parse_count, counts);
        width = line.read("--width", parse_count, counts);
        time_limit = line.read("--time-limit", parse_duration, "a duration like 200ms or 10s");
        warmup = line.read("--warmup", parse_duration, "a duration like 200ms or 10s");
        if (!line.text("--memory-limit").empty())
            memory_limit = line.read("--memory-limit", parse_size, "a size like 4096, 64KB or 16GB");
        between_runs = line.read("--between-runs", parse_between_runs, "one of keep, reconnect");
        output = line.text("--output");
    }
};

/** One measured configuration, as `ClientArgs::runs` sweeps them. */
struct run_t {
    workload_t workload = tool_k;
    variant_t variant = plain_k;
    parsing_t parsing = body_k;
    phase_t phase = steady_k;
    std::size_t connections = 0;
    std::size_t in_flight = 0;
    std::uint64_t body_bytes = 0;
};

/** Every configuration; batched variants only where the server batches, and never for uploads. */
std::vector<run_t> runs_of(settings_t const& settings, bool batching) noexcept {
    std::vector<run_t> runs;
    for (std::size_t workload : settings.workloads)
        for (std::size_t variant : settings.variants) {
            if (variant == batched_k && (!batching || workload == context_k)) continue;
            bool const swept = workload == tool_k && variant == plain_k;
            std::vector<std::size_t> const shapes = swept ? settings.parsing : std::vector<std::size_t> {body_k};
            std::vector<std::uint64_t> const sizes = workload == context_k ? settings.body_bytes
                                                                           : std::vector<std::uint64_t> {0};
            for (std::size_t parsing : shapes)
                for (std::size_t phase : settings.phases)
                    for (std::size_t connections : settings.connections)
                        for (std::uint64_t body_bytes : sizes)
                            runs.push_back({static_cast<workload_t>(workload), static_cast<variant_t>(variant),
                                            static_cast<parsing_t>(parsing), static_cast<phase_t>(phase), connections,
                                            std::min(settings.in_flight.value_or(connections), connections),
                                            body_bytes});
        }
    return runs;
}

/** Nanosecond latencies in 16 linear bins per power of two, the bins `stats.rs` and `stats.py` keep. */
struct histogram_t {
    static constexpr std::size_t sub_bins_k = 16;
    static constexpr unsigned exact_bits_k = 5;
    static constexpr std::size_t bins_k = (64 - exact_bits_k + 2) * sub_bins_k;
    std::vector<std::uint64_t> bins = std::vector<std::uint64_t>(bins_k);

    static std::size_t bin_of(std::uint64_t nanoseconds) noexcept {
        if (nanoseconds < 2 * sub_bins_k) return nanoseconds;
        unsigned const shift = static_cast<unsigned>(std::bit_width(nanoseconds)) - exact_bits_k;
        return shift * sub_bins_k + (nanoseconds >> shift);
    }

    static std::uint64_t upper_edge(std::size_t bin) noexcept {
        if (bin < 2 * sub_bins_k) return bin;
        std::size_t const shift = bin / sub_bins_k - 1;
        return ((bin % sub_bins_k + sub_bins_k + 1) << shift) - 1;
    }

    void record(steady_t::duration latency) noexcept {
        long long const nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(latency).count();
        ++bins[std::min(bin_of(static_cast<std::uint64_t>(std::max(nanoseconds, 0ll))), bins_k - 1)];
    }

    void merge(histogram_t const& other) noexcept {
        for (std::size_t bin = 0; bin != bins_k; ++bin) bins[bin] += other.bins[bin];
    }

    /** The upper edge in microseconds of the bin holding the nearest-rank @p fraction. */
    double percentile(double fraction) const noexcept {
        std::uint64_t total = 0;
        for (std::uint64_t count : bins) total += count;
        if (!total) return 0;
        std::uint64_t const rank = std::max<std::uint64_t>(static_cast<std::uint64_t>(std::ceil(fraction * total)), 1);
        std::uint64_t cumulative = 0;
        for (std::size_t bin = 0; bin != bins_k; ++bin)
            if ((cumulative += bins[bin]) >= rank) return upper_edge(bin) / 1000.0;
        return upper_edge(bins_k - 1) / 1000.0;
    }

    std::string percentiles_json() const noexcept {
        return std::format(R"({{"p50":{},"p90":{},"p99":{},"p999":{},"max":{}}})", percentile(0.5), percentile(0.9),
                           percentile(0.99), percentile(0.999), percentile(1.0));
    }

    /** The non-empty bins as `[bin, count]` pairs. */
    std::string sparse_json() const noexcept {
        std::string pairs;
        for (std::size_t bin = 0; bin != bins_k; ++bin)
            if (bins[bin]) pairs += std::format("{}[{},{}]", pairs.empty() ? "" : ",", bin, bins[bin]);
        return std::format("[{}]", pairs);
    }
};

/** Failed calls by why they failed, as `stats.rs` classes them. */
struct failures_t {
    std::uint64_t connect = 0, refused = 0, timeout = 0, status = 0, wrong_reply = 0;

    std::uint64_t total() const noexcept { return connect + refused + timeout + status + wrong_reply; }
    void merge(failures_t const& other) noexcept {
        connect += other.connect, refused += other.refused, timeout += other.timeout;
        status += other.status, wrong_reply += other.wrong_reply;
    }
};

/** What a set of calls returned. */
struct tally_t {
    histogram_t latencies, lateness;
    std::uint64_t calls = 0, unsent = 0, all_calls = 0, request_body_bytes = 0, response_body_bytes = 0;
    failures_t failures;

    void merge(tally_t const& other) noexcept {
        latencies.merge(other.latencies), lateness.merge(other.lateness);
        calls += other.calls, unsent += other.unsent, all_calls += other.all_calls;
        request_body_bytes += other.request_body_bytes, response_body_bytes += other.response_body_bytes;
        failures.merge(other.failures);
    }
};

/** A warmup, then the measured window, closed-loop or on one global arrival schedule. */
struct window_t {
    time_point_t begin, measured_from, until;
    std::optional<std::size_t> calls_per_second;
    std::size_t slots = 0;

    std::optional<time_point_t> scheduled(std::size_t sequence) const noexcept {
        if (!calls_per_second) return {};
        double const seconds = static_cast<double>(sequence) / static_cast<double>(*calls_per_second);
        return begin + std::chrono::duration_cast<steady_t::duration>(std::chrono::duration<double>(seconds));
    }
};

/** One call as every worker sends it to one target, and the reply it must get back byte for byte. */
struct exchange_t {
    ucall_verb_t verb = ucall_verb_post_k;
    std::string url;
    std::vector<std::string> header_lines;
    std::vector<ucall_bytes_view_t> headers;
    std::string_view body;
    std::string reply;
};

/** Everything the workers share. */
struct shared_t {
    settings_t const& settings;
    std::vector<std::string> origins;
    std::vector<tool_parts_t> ring;
    std::vector<std::string> contexts;
    ucall_noise_key_t client_key {}, server_public {};
    uc::client_call_options_t options {};

    struct known_server_t {
        ucall_noise_key_t const* key;
        bool operator()(ucall_noise_key_t const& peer) const noexcept { return uc::noise_keys_equal(peer, *key); }
    } known_server {&server_public};

    uc::client_t client(std::size_t connections) noexcept {
        bool const noise = settings.security != ucall_security_none_k;
        uc::client_options_t options {.max_concurrent_connections = connections, .call_timeout = call_timeout_k};
        if (noise) {
            options.noise_private_key = &client_key;
            options.peer_authorizer = uc::peer_authorizer_t {known_server};
        }
        auto [made, made_status] = uc::client_t::make(options);
        if (made_status.failed()) exit_with(std::format("UCall client: {}", uc::status_name(made_status.code)));
        return std::move(made);
    }
};

/** One configuration's calls: per target, one exchange per ring entry, or the one context upload. */
struct load_t {
    run_t run;
    std::vector<std::string> bodies;
    std::vector<std::vector<exchange_t>> exchanges;

    load_t(shared_t const& shared, run_t const& configuration) noexcept : run(configuration) {
        settings_t const& settings = shared.settings;
        std::string_view const batch = run.variant == batched_k ? "/batch" : "";
        if (run.workload != context_k)
            for (tool_parts_t const& parts : shared.ring)
                bodies.push_back(run.workload == tool_k && run.parsing == full_k ? parts.full_body() : parts.body());
        for (std::string const& origin : shared.origins) {
            std::vector<exchange_t>& calls = exchanges.emplace_back();
            auto add = [&](ucall_verb_t verb, std::string path, std::string_view body, std::string reply) noexcept {
                exchange_t& call = calls.emplace_back();
                call.verb = verb, call.url = std::format("{}{}", origin, path), call.body = body, call.reply = reply;
                call.header_lines = {"content-type: application/json", "accept-encoding: identity"};
                if (run.phase == handshake_k) call.header_lines.push_back("connection: close");
                return &call;
            };
            if (run.workload == context_k) {
                std::size_t const size = std::find(settings.body_bytes.begin(), settings.body_bytes.end(),
                                                   run.body_bytes) -
                                         settings.body_bytes.begin();
                std::string_view const body = shared.contexts[size];
                add(ucall_verb_put_k, "/context", body, std::string {count_reply(body.size()).view()});
            }
            for (std::size_t index = 0; run.workload != context_k && index != shared.ring.size(); ++index) {
                tool_parts_t const& parts = shared.ring[index];
                std::string_view const body = bodies[index];
                if (run.workload == fanout_k)
                    add(ucall_verb_post_k, std::format("/fanout{}?width={}", batch, settings.width), body,
                        std::string {fanout_reply(parts.id, settings.width).view()});
                else if (run.parsing == raw_k)
                    add(ucall_verb_post_k, "/raw", body, std::string {body.substr(0, reply_bytes_k)});
                else if (run.parsing == body_k)
                    add(ucall_verb_post_k, std::format("/tool{}", batch), body,
                        std::string {tool_reply(parts.id, parts.check()).view()});
                else
                    add(ucall_verb_post_k, parts.full_path(), body,
                        std::string {full_reply(parts.id, tool_name_k, parts.trace_id(), lang_k, parts.check()).view()})
                        ->header_lines.insert(calls.back().header_lines.end(),
                                              {std::format("authorization: {}", authorization_k),
                                               std::format("traceparent: {}", parts.traceparent())});
            }
            for (exchange_t& call : calls)
                for (std::string const& line : call.header_lines) call.headers.push_back(uc::bytes_view(line));
        }
    }

    uc::client_call_t call(shared_t const& shared, exchange_t const& exchange) const noexcept {
        return {.verb = exchange.verb,
                .url = exchange.url,
                .headers = exchange.headers,
                .body = exchange.body,
                .options = shared.options};
    }
};

/** A call of one slot that the client holds. */
struct flight_t {
    time_point_t intended {};
    std::size_t sequence = 0;
};

/** One worker of the pools: its client across configurations, and its share of each one's slots and connections. */
struct worker_t {
    std::size_t interface = 0;
    std::size_t rank = 0;
    std::size_t peers = 0;
    uc::client_t client;
    std::size_t connections = 0;
    tally_t tally;

    /** Whether target @p target is paired with this worker's interface. */
    bool serves(shared_t const& shared, std::size_t target) const noexcept {
        return target % shared.settings.interfaces.size() == interface;
    }

    /** Opens this worker's share of the connections, reusing its client when the policy and share allow. */
    void prepare(shared_t& shared, load_t const& load) noexcept {
        std::size_t const targets = shared.origins.size();
        std::size_t wanted = 0;
        for (std::size_t target = 0; target != targets; ++target)
            if (serves(shared, target)) wanted += share(share(load.run.connections, target, targets), rank, peers);
        wanted = std::max<std::size_t>(wanted, 1);
        if (!client.handle() || connections != wanted) {
            client = shared.client(wanted);
            connections = wanted;
        }
        if (load.run.phase != steady_k) return;
        ucall_bytes_view_t const headers[] {uc::bytes_view("accept-encoding: identity")};
        std::vector<std::string> urls;
        for (std::size_t target = 0; target != targets; ++target)
            if (serves(shared, target)) urls.push_back(std::format("{}/health", shared.origins[target]));
        std::size_t submitted = 0;
        for (std::size_t target = 0, served = 0; target != targets; ++target) {
            if (!serves(shared, target)) continue;
            std::size_t const count = share(share(load.run.connections, target, targets), rank, peers);
            uc::client_call_t const call {.verb = ucall_verb_get_k,
                                          .url = urls[served++],
                                          .headers = headers,
                                          .options = shared.options};
            for (std::size_t index = 0; index != count; ++index) submitted += client.submit(call, nullptr).succeeded();
        }
        std::vector<ucall_completed_call_t> storage(std::max<std::size_t>(submitted, 1));
        for (std::size_t answered = 0; answered < submitted;) {
            auto [finished, finished_status] = client.take_completed(storage, call_timeout_k);
            if (finished_status.failed()) break;
            answered += finished.size();
        }
    }

    /** Drives this worker's slots of the window, each with one call outstanding, as `stats::Slot` schedules them. */
    void measure(shared_t const& shared, load_t const& load, window_t const& window, std::size_t interfaces) noexcept {
        tally = {};
        std::size_t const targets = shared.origins.size();
        std::vector<std::size_t> slots;
        for (std::size_t slot = 0; slot != window.slots; ++slot) {
            std::size_t const target = slot % targets;
            if (target % interfaces == interface && slot / targets % peers == rank) slots.push_back(slot);
        }
        std::vector<std::size_t> arrivals(slots.size());
        std::vector<flight_t> flights(slots.size());
        struct due_t {
            time_point_t intended;
            std::size_t position, sequence;
            bool operator>(due_t const& other) const noexcept { return intended > other.intended; }
        };
        std::priority_queue<due_t, std::vector<due_t>, std::greater<due_t>> due;
        auto queue = [&](std::size_t position) noexcept {
            std::size_t const sequence = slots[position] + arrivals[position]++ * window.slots;
            time_point_t const intended = window.scheduled(sequence).value_or(steady_t::now());
            if (intended < window.until && steady_t::now() < window.until)
                return due.push({intended, position, sequence});
            for (std::size_t late = sequence;; late += window.slots) {
                std::optional<time_point_t> const owed = window.scheduled(late);
                if (!owed || *owed >= window.until) break;
                tally.unsent += *owed >= window.measured_from;
            }
        };
        auto exchange_of = [&](std::size_t position, std::size_t sequence) noexcept -> exchange_t const& {
            std::vector<exchange_t> const& calls = load.exchanges[slots[position] % targets];
            return calls[sequence % calls.size()];
        };
        auto record = [&](std::size_t position, std::optional<std::uint64_t> received,
                          std::uint64_t failures_t::* failure) noexcept {
            flight_t const& flight = flights[position];
            ++tally.all_calls;
            if (flight.intended >= window.measured_from && received) {
                ++tally.calls;
                tally.request_body_bytes += exchange_of(position, flight.sequence).body.size();
                tally.response_body_bytes += *received;
                tally.latencies.record(steady_t::now() - flight.intended);
            }
            else if (flight.intended >= window.measured_from) ++(tally.failures.*failure);
            queue(position);
        };
        std::this_thread::sleep_until(window.begin);
        for (std::size_t position = 0; position != slots.size(); ++position) queue(position);
        std::vector<ucall_completed_call_t> storage(std::max<std::size_t>(slots.size(), 1));
        std::size_t outstanding = 0;
        while (outstanding || !due.empty()) {
            time_point_t const now = steady_t::now();
            while (!due.empty() && due.top().intended <= now) {
                due_t const next = due.top();
                due.pop();
                if (next.intended >= window.measured_from) tally.lateness.record(now - next.intended);
                flights[next.position] = {next.intended, next.sequence};
                uc::client_call_t const call = load.call(shared, exchange_of(next.position, next.sequence));
                uc::status_t const submitted = client.submit(call, &flights[next.position]);
                if (submitted.succeeded()) ++outstanding;
                else
                    record(next.position, std::nullopt,
                           submitted == uc::status_code_t::timeout_k ? &failures_t::timeout : &failures_t::connect);
            }
            std::chrono::microseconds const wait = due.empty() ? call_timeout_k
                                                               : std::chrono::ceil<std::chrono::microseconds>(std::max(
                                                                     due.top().intended - now, steady_t::duration {}));
            if (!outstanding) {
                std::this_thread::sleep_for(wait);
                continue;
            }
            auto [finished, finished_status] = client.take_completed(storage, wait);
            if (finished_status.failed()) continue;
            for (uc::completed_t completed : finished) {
                std::size_t const position = static_cast<std::size_t>(static_cast<flight_t*>(completed.context) -
                                                                      flights.data());
                exchange_t const& exchange = exchange_of(position, flights[position].sequence);
                --outstanding;
                auto& [response, response_status] = completed.response;
                if (response_status.failed()) {
                    record(
                        position, std::nullopt,
                        response_status == uc::status_code_t::timeout_k ? &failures_t::timeout : &failures_t::connect);
                    continue;
                }
                auto const [body, body_status] = response.body();
                if (response.status_code() != ucall_http_status_ok_k)
                    record(position, std::nullopt, &failures_t::status);
                else if (body_status.failed() || body != exchange.reply)
                    record(position, std::nullopt, &failures_t::wrong_reply);
                else record(position, body.size(), nullptr);
            }
        }
    }
};

/** The pools near each memory domain and the workers they run, in the order of their threads. */
struct pools_t {
    shared_t& shared;
    std::vector<worker_t> workers;
    std::vector<fu_pool_t> pools;
    std::vector<std::vector<std::size_t>> members;
    load_t const* load = nullptr;
    window_t window;

    static void prepare(void* context, std::size_t thread, std::size_t) noexcept {
        pools_t& pools = *static_cast<pools_t*>(context);
        pools.workers[thread].prepare(pools.shared, *pools.load);
    }
    static void measure(void* context, std::size_t thread, std::size_t) noexcept {
        pools_t& pools = *static_cast<pools_t*>(context);
        pools.workers[thread].measure(pools.shared, *pools.load, pools.window, pools.shared.settings.interfaces.size());
    }

    /** Runs @p callback on every worker of every pool at once, each pool's threads indexing its own members. */
    void dispatch(fu_for_threads_t callback) noexcept {
        struct pool_context_t {
            pools_t* pools;
            std::vector<std::size_t> const* members;
            fu_for_threads_t callback;
            static void run(void* context, std::size_t thread, std::size_t domain) noexcept {
                pool_context_t& pool = *static_cast<pool_context_t*>(context);
                pool.callback(pool.pools, (*pool.members)[thread], domain);
            }
        };
        std::vector<pool_context_t> contexts;
        for (std::vector<std::size_t> const& group : members) contexts.push_back({this, &group, callback});
        std::vector<fu_generation_t> generations(pools.size());
        for (std::size_t index = 0; index != pools.size(); ++index)
            if (fu_pool_unsafe_for_threads(pools[index], &pool_context_t::run, &contexts[index], &generations[index]) !=
                fu_success_k)
                exit_with("a ForkUnion pool refused the dispatch");
        for (std::size_t index = 0; index != pools.size(); ++index)
            fu_pool_unsafe_join(pools[index], generations[index]);
    }

    ~pools_t() noexcept {
        for (fu_pool_t pool : pools) fu_pool_delete(pool);
    }
};

/** The memory domain UCall reports for each interface's device, -1 for an address alone. */
std::vector<int> memory_domains(std::vector<interface_t> const& interfaces) noexcept {
    std::vector<std::string> addresses;
    for (interface_t const& interface : interfaces) addresses.push_back(host_port(interface.addresses[0], 0));
    std::vector<ucall_network_interface_t> listeners;
    for (std::size_t index = 0; index != interfaces.size(); ++index)
        listeners.push_back({.address = uc::bytes_view(addresses[index]),
                             .device_name = uc::bytes_view(interfaces[index].device),
                             .memory_domain = -1,
                             .event_loops = 1,
                             .max_concurrent_connections = 1});
    ucall_server_options_t const options {.interfaces = listeners.data(), .interfaces_count = listeners.size()};
    ucall_server_options_t resolved {};
    ucall_server_t server = nullptr;
    ucall_outcome_t outcome {};
    ucall_server_init_t init {.server = &server, .outcome = &outcome, .options = &options, .resolved = &resolved};
    ucall_server_init(&init);
    std::vector<int> domains(interfaces.size(), -1);
    for (std::size_t index = 0; outcome.status == ucall_status_success_k && index != domains.size(); ++index)
        domains[index] = resolved.interfaces[index].memory_domain;
    ucall_server_free(server);
    return domains;
}

/** This process's place among the clients launch.py starts, from `SERVEVAL_PROCESS` as `INDEX/COUNT`. */
struct process_t {
    std::size_t index = 0, count = 1;

    process_t() noexcept {
        char const* set = std::getenv("SERVEVAL_PROCESS");
        if (!set || !*set) return;
        std::string_view const text = set;
        std::size_t const slash = text.find('/');
        std::optional<std::size_t> const parsed_index = parse_digits<std::size_t>(text.substr(0, slash));
        std::optional<std::size_t> const parsed_count = slash == std::string_view::npos
                                                            ? std::nullopt
                                                            : parse_digits<std::size_t>(text.substr(slash + 1));
        if (!parsed_index || !parsed_count || *parsed_index >= *parsed_count)
            exit_with(std::format("SERVEVAL_PROCESS=\"{}\" does not parse, expected INDEX/COUNT like 0/2", text));
        index = *parsed_index, count = *parsed_count;
    }
};

std::uint64_t unix_nanoseconds() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

/** Run @p run starts 100ms after the last launched client finished connecting for it, as `arguments::start_at`. */
time_point_t start_at(std::size_t run, process_t const& process) noexcept {
    char const* directory = std::getenv("SERVEVAL_BARRIER");
    if (!directory) return steady_t::now();
    std::string const mine = std::format("{}/{}-{}", directory, run, process.index), staged = mine + ".tmp";
    std::FILE* file = std::fopen(staged.c_str(), "wb");
    std::string const stamp = std::format("{}", unix_nanoseconds());
    bool const written = file && std::fwrite(stamp.data(), 1, stamp.size(), file) == stamp.size();
    if (file) std::fclose(file);
    if (!written || std::rename(staged.c_str(), mine.c_str()) != 0)
        exit_with(std::format("SERVEVAL_BARRIER=\"{}\" is not writable", directory));
    std::uint64_t latest = 0;
    for (std::size_t other = 0; other != process.count;) {
        std::optional<std::string> const text = read_file(std::format("{}/{}-{}", directory, run, other));
        std::optional<std::uint64_t> const nanoseconds = text ? parse_digits<std::uint64_t>(*text) : std::nullopt;
        if (!nanoseconds) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        latest = std::max(latest, *nanoseconds), ++other;
    }
    std::chrono::nanoseconds const delay {
        static_cast<std::int64_t>(latest + 100'000'000 - std::min(latest + 100'000'000, unix_nanoseconds()))};
    return steady_t::now() + std::chrono::duration_cast<steady_t::duration>(delay);
}

/** User and system CPU time this process used. */
std::chrono::microseconds process_cpu() noexcept {
    rusage usage {};
    getrusage(RUSAGE_SELF, &usage);
    auto time = [](timeval value) noexcept {
        return std::chrono::seconds(value.tv_sec) + std::chrono::microseconds(value.tv_usec);
    };
    return time(usage.ru_utime) + time(usage.ru_stime);
}

/** A server's cgroup counters from `/stats`, missing where it reports none. */
struct unit_stats_t {
    std::optional<std::uint64_t> cpu_microseconds, memory_bytes, memory_limit, tcp_active_opens;
    std::uint64_t cores = 0;
};

std::optional<std::uint64_t> json_count(yyjson_val* object, char const* key) noexcept {
    yyjson_val* value = yyjson_obj_get(object, key);
    if (!yyjson_is_uint(value)) return {};
    return yyjson_get_uint(value);
}

/** The JSON text of an optional count. */
std::string spell(std::optional<std::uint64_t> value) noexcept { return value ? std::format("{}", *value) : "null"; }
std::string spell(std::optional<std::int64_t> value) noexcept { return value ? std::format("{}", *value) : "null"; }
std::string spell(std::optional<double> value) noexcept { return value ? std::format("{}", *value) : "null"; }

/** @p text as a JSON string. */
std::string json_string(std::string_view text) noexcept {
    std::string escaped {"\""};
    for (char character : text) {
        if (character == '"' || character == '\\') escaped += '\\';
        escaped += character;
    }
    return escaped += '"';
}

/** The output of a shell command, trimmed, for what only the platform's tools report. */
std::optional<std::string> command_output(char const* command) noexcept {
    std::FILE* pipe = popen(command, "r");
    if (!pipe) return {};
    std::string text;
    char chunk[256];
    for (std::size_t read; (read = std::fread(chunk, 1, sizeof(chunk), pipe)) != 0;) text.append(chunk, read);
    pclose(pipe);
    while (!text.empty() && (text.back() == '\n' || text.back() == ' ')) text.pop_back();
    if (text.empty()) return {};
    return text;
}

/** The host's CPU, memory and OS with this client's budget, as `results.rs` reports them, its JSON spelled once. */
struct machine_t {
    std::string cpu_model;
    std::size_t cores = 0;
    std::string cpus;
    std::uint64_t memory_limit = 0;
    std::string json;

    explicit machine_t(std::optional<std::uint64_t> wanted_memory) noexcept {
        cpus = allowed_cpus(), cores = count_cpus(cpus);
        std::uint64_t const ram = static_cast<std::uint64_t>(sysconf(_SC_PHYS_PAGES)) * sysconf(_SC_PAGE_SIZE);
        std::optional<std::string> const membership = read_file("/proc/self/cgroup");
        std::optional<std::string_view> const relative = membership ? line_value(*membership, "0::") : std::nullopt;
        std::optional<std::uint64_t> cgroup_limit;
        for (std::string directory = relative ? std::format("/sys/fs/cgroup{}", *relative) : "";
             directory.starts_with("/sys/fs/cgroup"); directory.resize(directory.rfind('/'))) {
            std::optional<std::string> const maximum = read_file(directory + "/memory.max");
            std::optional<std::string_view> const value = maximum ? line_value(*maximum, "") : std::nullopt;
            std::optional<std::uint64_t> const bound = value ? parse_digits<std::uint64_t>(*value) : std::nullopt;
            if (bound && (!cgroup_limit || *bound < *cgroup_limit)) cgroup_limit = bound;
        }
        std::uint64_t const environment = cgroup_limit.value_or(ram);
        if (wanted_memory && *wanted_memory > environment)
            exit_with(std::format("--memory-limit {} exceeds the {} bytes this process may use", *wanted_memory,
                                  environment));
        memory_limit = wanted_memory.value_or(environment);

        std::optional<std::string> const cpuinfo = read_file("/proc/cpuinfo");
        std::optional<std::string_view> const model = cpuinfo ? line_value(*cpuinfo, "model name") : std::nullopt;
        std::optional<std::string_view> const implementer = cpuinfo ? line_value(*cpuinfo, "CPU implementer")
                                                                    : std::nullopt;
        std::optional<std::string_view> const part = cpuinfo ? line_value(*cpuinfo, "CPU part") : std::nullopt;
        struct arm_part_t {
            std::string_view implementer, part, name;
        };
        arm_part_t const arm_parts[] {{": 0x41", ": 0xd0c", "Neoverse-N1"}, {": 0x41", ": 0xd40", "Neoverse-V1"},
                                      {": 0x41", ": 0xd49", "Neoverse-N2"}, {": 0x41", ": 0xd4f", "Neoverse-V2"},
                                      {": 0x41", ": 0xd84", "Neoverse-V3"}, {": 0x41", ": 0xd8e", "Neoverse-N3"}};
        for (arm_part_t const& known : arm_parts)
            if (implementer == known.implementer.substr(2) && part == known.part.substr(2)) cpu_model = known.name;
        if (cpu_model.empty() && model) cpu_model = model->substr(model->find_first_not_of(": \t"));
        if (cpu_model.empty()) cpu_model = command_output("sysctl -n machdep.cpu.brand_string").value_or("");

        std::set<std::string> siblings;
        std::error_code missing;
        for (std::filesystem::directory_entry const& entry :
             std::filesystem::directory_iterator("/sys/devices/system/cpu", missing))
            if (std::optional<std::string> list = read_file(entry.path().string() + "/topology/core_cpus_list"))
                siblings.insert(*list);
        std::optional<std::uint64_t> physical;
        if (!siblings.empty()) physical = siblings.size();
        else if (std::optional<std::string> counted = command_output("sysctl -n hw.physicalcpu"))
            physical = parse_digits<std::uint64_t>(*counted);

        utsname names {};
        uname(&names);
        std::string_view const machine = names.machine;
        std::optional<std::string> const release = read_file("/etc/os-release");
        std::optional<std::string_view> const pretty = release ? line_value(*release, "PRETTY_NAME=") : std::nullopt;
        std::string os = pretty ? std::string {pretty->substr(1, pretty->size() - 2)} : std::string {};
        if (os.empty())
            if (std::optional<std::string> version = command_output("sw_vers -productVersion"))
                os = std::format("macOS {}", *version);

        std::string_view const kernel_limits[] {"fs.nr_open",
                                                "net.core.somaxconn",
                                                "net.ipv4.ip_local_port_range",
                                                "net.ipv4.tcp_max_syn_backlog",
                                                "net.ipv4.tcp_max_tw_buckets",
                                                "net.ipv4.tcp_rmem",
                                                "net.ipv4.tcp_tw_reuse",
                                                "net.ipv4.tcp_wmem",
                                                "net.netfilter.nf_conntrack_max"};
        std::string limits;
        for (std::string_view name : kernel_limits) {
            std::string path {"/proc/sys/"};
            for (char character : name) path += character == '.' ? '/' : character;
            std::optional<std::string> const value = read_file(path);
            std::string words;
            for (std::size_t first = value ? value->find_first_not_of(" \t\n") : std::string::npos;
                 first != std::string::npos;) {
                std::size_t const end = std::min(value->find_first_of(" \t\n", first), value->size());
                words += std::format("{}{}", words.empty() ? "" : " ",
                                     std::string_view {*value}.substr(first, end - first));
                first = value->find_first_not_of(" \t\n", end);
            }
            limits += std::format("{}{}:{}", limits.empty() ? "" : ",", json_string(name),
                                  value ? json_string(words) : "null");
        }
        json = std::format(
            R"({{"cpu_model":{},"logical_cores":{},"physical_cores":{},"ram_bytes":{},"kernel":{},"os":{},"architecture":{},"kernel_limits":{{{}}},"budget":{{"cores":{},"cpus":{},"memory_limit":{}}}}})",
            json_string(cpu_model), std::thread::hardware_concurrency(), spell(physical), ram,
            json_string(names.release), os.empty() ? "null" : json_string(os),
            json_string(machine == "arm64" ? "aarch64" : machine), limits, cores, json_string(cpus), memory_limit);
    }
};

/** The FNV-1a hash of @p text, as 16 hexadecimal digits. */
std::string fnv1a(std::string_view text) noexcept {
    std::uint64_t hash = 0xCBF29CE484222325ull;
    for (char character : text) hash = (hash ^ static_cast<std::uint8_t>(character)) * 0x100000001B3ull;
    return std::format("{:016x}", hash);
}

/** Writes @p json as `<directory>/<framework>-<config-hash>.json`, flushed to the drive before it replaces an older
 *  one. */
void write_report(std::string const& directory, std::string_view framework, std::string_view config,
                  std::string_view json) noexcept {
    std::filesystem::create_directories(directory);
    std::string const path = std::format("{}/{}-{}.json", directory, framework, fnv1a(config));
    std::string const staged = std::format("{}.{}.tmp", path, getpid());
    std::FILE* file = std::fopen(staged.c_str(), "wb");
    bool const written = file && std::fwrite(json.data(), 1, json.size(), file) == json.size() &&
                         std::fflush(file) == 0 && fsync(fileno(file)) == 0;
    if (file) std::fclose(file);
    if (!written || std::rename(staged.c_str(), path.c_str()) != 0)
        exit_with(std::format("--output=\"{}\" is not writable", directory));
}

/** The servers' counters summed over the targets; a counter missing from any of them is missing. */
unit_stats_t server_stats(shared_t const& shared, uc::client_t& control) noexcept {
    unit_stats_t total {.cpu_microseconds = 0, .memory_bytes = 0, .memory_limit = 0};
    ucall_bytes_view_t const headers[] {uc::bytes_view("accept-encoding: identity"),
                                        uc::bytes_view("connection: close")};
    for (std::string const& origin : shared.origins) {
        std::string const url = std::format("{}/stats", origin);
        auto [response, response_status] = control.call(
            {.verb = ucall_verb_get_k, .url = url, .headers = headers, .options = shared.options});
        auto const [body, body_status] = response_status.failed() ? uc::expected<std::string_view> {{}, response_status}
                                                                  : response.body();
        yyjson_doc* document = yyjson_read(body.data(), body_status.failed() ? 0 : body.size(), 0);
        yyjson_val* root = yyjson_doc_get_root(document);
        auto add = [&](std::optional<std::uint64_t>& sum, char const* key) noexcept {
            std::optional<std::uint64_t> const value = json_count(root, key);
            sum = sum && value ? std::optional {*sum + *value} : std::nullopt;
        };
        add(total.cpu_microseconds, "cpu_microseconds"), add(total.memory_bytes, "memory_bytes");
        add(total.memory_limit, "memory_limit");
        total.cores += json_count(root, "cores").value_or(0);
        std::optional<std::uint64_t> const opens = json_count(root, "tcp_active_opens");
        if (opens) total.tcp_active_opens = std::max(total.tcp_active_opens.value_or(0), *opens);
        yyjson_doc_free(document);
    }
    return total;
}

/** @p values joined by commas, each spelled by @p spell_one. */
template <typename value_type_, typename spell_type_>
std::string joined(std::vector<value_type_> const& values, spell_type_ spell_one) noexcept {
    std::string text;
    for (value_type_ const& value : values) text += std::format("{}{}", text.empty() ? "" : ",", spell_one(value));
    return text;
}

template <typename value_type_>
std::string spell_plain(value_type_ const& value) noexcept {
    return std::format("{}", value);
}

std::string spell_interface(interface_t const& interface) noexcept {
    std::string addresses;
    for (std::string const& address : interface.addresses)
        addresses += std::format("{}{}", addresses.empty() ? "" : "+", address);
    return interface.device.empty() ? addresses : std::format("{}={}", interface.device, addresses);
}

/** Spells a size the way `parse_size` reads it: `16GB`, `1000`. */
std::string spell_size(std::uint64_t bytes) noexcept {
    std::string_view const units[] {"", "KB", "MB", "GB", "TB"};
    std::size_t level = 0;
    for (; level + 1 != std::size(units) && bytes && bytes % 1024 == 0; ++level) bytes /= 1024;
    return std::format("{}{}", bytes, units[level]);
}

/** Spells a duration the way `parse_duration` reads it: `1s`, `1500ms`. */
std::string spell_duration(std::chrono::milliseconds duration) noexcept {
    if (duration.count() % 1000) return std::format("{}ms", duration.count());
    return std::format("{}s", duration.count() / 1000);
}

std::string spell_count(std::optional<std::size_t> count) noexcept {
    return count ? std::format("{}", *count) : std::string {"none"};
}

/** The choices @p indices select from @p names, joined by commas. */
std::string spell_names(std::vector<std::size_t> const& indices, std::string_view const* names) noexcept {
    std::string text;
    for (std::size_t index : indices) text += std::format("{}{}", text.empty() ? "" : ",", names[index]);
    return text;
}

std::string spell_cell(std::optional<double> value) noexcept {
    return value ? std::format("{:.1f}", *value) : std::string {"n/a"};
}

/** How much a counter grew, missing where either sample is. */
std::optional<std::uint64_t> growth(std::optional<std::uint64_t> before, std::optional<std::uint64_t> after) noexcept {
    if (!before || !after) return {};
    return *after - std::min(*after, *before);
}

/** A steady-clock moment in Unix milliseconds, to find a window in other recordings. */
std::int64_t unix_milliseconds(time_point_t moment) noexcept {
    std::chrono::nanoseconds const offset = std::chrono::system_clock::now().time_since_epoch() -
                                            steady_t::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(moment.time_since_epoch() + offset).count();
}

} // namespace serveval

int main(int argc, char** argv) {
    using namespace serveval;
    command_line_t const line {argc, argv, flags_k,
                               "UCall driving a UCall server through the tool, context and fan-out workloads."};
    settings_t const settings {line};
    process_t const process;
    shared_t shared {.settings = settings};
    std::string_view const scheme = settings.security == ucall_security_none_k ? "http" : "http+noise";
    for (std::string const& target : settings.targets)
        shared.origins.push_back(std::format("{}://{}", scheme, host_port(target, settings.port)));
    for (std::size_t index = 0; index != ring_k; ++index) shared.ring.emplace_back(settings.seed, index);
    if (std::find(settings.workloads.begin(), settings.workloads.end(), context_k) != settings.workloads.end())
        for (std::uint64_t bytes : settings.body_bytes) shared.contexts.push_back(context_body(settings.seed, bytes));
    if (settings.security != ucall_security_none_k) {
        read_key(settings.data_dir, "client.key", shared.client_key);
        read_key(settings.data_dir, "server.pub", shared.server_public);
        shared.options = {.security = settings.security, .noise_peer_key = &shared.server_public};
    }
    uc::client_t control = shared.client(shared.origins.size());
    ucall_bytes_view_t const get_headers[] {uc::bytes_view("accept-encoding: identity"),
                                            uc::bytes_view("connection: close")};
    std::string const health_url = std::format("{}/health", shared.origins[0]);
    auto [health, health_status] = control.call(
        {.verb = ucall_verb_get_k, .url = health_url, .headers = get_headers, .options = shared.options});
    auto const [health_body, health_body_status] = health_status.failed()
                                                       ? uc::expected<std::string_view> {{}, health_status}
                                                       : health.body();
    yyjson_doc* health_document = yyjson_read(health_body.data(), health_body_status.failed() ? 0 : health_body.size(),
                                              0);
    yyjson_val* health_root = yyjson_doc_get_root(health_document);
    yyjson_val* framework_value = yyjson_obj_get(health_root, "framework");
    yyjson_val* client_value = yyjson_obj_get(health_root, "client");
    if (!yyjson_is_str(framework_value) || !yyjson_is_str(client_value)) exit_with("no /health reply");
    std::string const framework = yyjson_get_str(framework_value), client_name = yyjson_get_str(client_value);
    bool const batching = yyjson_is_true(yyjson_obj_get(health_root, "batching"));
    yyjson_doc_free(health_document);

    machine_t const machine {settings.memory_limit};
    std::string const settings_lines[] {
        std::format("- Framework: {}", framework),
        std::format("- Client: {}", client_name),
        std::format("- Threads: {}", settings.threads),
        std::format("- Interfaces: {}", joined(settings.interfaces, spell_interface)),
        std::format("- Port: {}", settings.port),
        std::format("- Security: {}", security_names_k[settings.security_choice]),
        std::format("- Data dir: {}", settings.data_dir),
        std::format("- Seed: {}", settings.seed),
        std::format("- Targets: {}", joined(settings.targets, spell_plain<std::string>)),
        std::format("- Connections: {}", joined(settings.connections, spell_plain<std::size_t>)),
        std::format("- Workloads: {}", spell_names(settings.workloads, workload_names_k)),
        std::format("- Variants: {}", spell_names(settings.variants, variant_names_k)),
        std::format("- Phases: {}", spell_names(settings.phases, phase_names_k)),
        std::format("- Parsing: {}", spell_names(settings.parsing, parsing_names_k)),
        std::format("- Body bytes: {}", joined(settings.body_bytes, spell_size)),
        std::format("- In flight: {}", spell_count(settings.in_flight)),
        std::format("- Calls per second: {}", spell_count(settings.calls_per_second)),
        std::format("- Width: {}", settings.width),
        std::format("- Time limit: {}", spell_duration(settings.time_limit)),
        std::format("- Warmup: {}", spell_duration(settings.warmup)),
        std::format("- Between runs: {}", between_runs_names_k[settings.between_runs]),
        std::format("- Output: {}", settings.output),
        std::format("- Cores: {} ({})", machine.cores, machine.cpus),
        std::format("- Memory limit: {}", spell_size(machine.memory_limit)),
    };
    for (std::string const& setting : settings_lines) print_line(stderr, setting);

    std::vector<int> const domains = memory_domains(settings.interfaces);
    pools_t pools {.shared = shared};
    std::vector<int> pool_domains;
    for (std::size_t interface = 0; interface != settings.interfaces.size(); ++interface) {
        std::size_t const peers = std::max<std::size_t>(share(settings.threads, interface, settings.interfaces.size()),
                                                        1);
        std::size_t const group = std::find(pool_domains.begin(), pool_domains.end(), domains[interface]) -
                                  pool_domains.begin();
        if (group == pool_domains.size()) pool_domains.push_back(domains[interface]), pools.members.emplace_back();
        for (std::size_t rank = 0; rank != peers; ++rank) {
            pools.members[group].push_back(pools.workers.size());
            pools.workers.push_back({.interface = interface, .rank = rank, .peers = peers});
        }
    }
    fu_topology_t topology = nullptr;
    if (fu_topology_new(&topology) != fu_success_k) exit_with("ForkUnion found no topology");
    for (std::size_t group = 0; group != pool_domains.size(); ++group) {
        fu_pool_t pool = nullptr;
        fu_status_t status = fu_pool_new("serveval-ucall", fu_capabilities_all_k, &pool);
        std::size_t const threads = pools.members[group].size();
        if (status == fu_success_k)
            status = pool_domains[group] < 0 ? fu_pool_spawn(topology, pool, threads, fu_caller_exclusive_k)
                                             : fu_pool_spawn_near_memory_domain(topology, pool, pool_domains[group],
                                                                                threads, fu_caller_exclusive_k);
        if (status != fu_success_k)
            exit_with(std::format("ForkUnion refused a pool of {} threads: {}", threads, static_cast<int>(status)));
        pools.pools.push_back(pool);
    }
    fu_topology_delete(topology);

    std::uint64_t const started = unix_nanoseconds() / 1'000'000'000;
    bool printed_header = false;
    std::vector<run_t> const runs = runs_of(settings, batching);
    for (std::size_t number = 0; number != runs.size(); ++number) {
        run_t const& run = runs[number];
        load_t const load {shared, run};
        auto [probe, probe_status] = control.call(load.call(shared, load.exchanges[0][0]));
        ucall_http_status_t const probed = probe_status.failed() ? ucall_http_status_ok_k : probe.status_code();
        if (probed == ucall_http_status_not_found_k || probed == ucall_http_status_method_not_allowed_k ||
            probed == ucall_http_status_not_implemented_k) {
            print_line(stderr,
                       std::format("| {} | {} | {} | {} | unsupported |", framework, workload_names_k[run.workload],
                                   variant_names_k[run.variant], parsing_names_k[run.parsing]));
            continue;
        }
        unit_stats_t const server_before = server_stats(shared, control);
        std::chrono::microseconds const cpu_before = process_cpu();
        pools.load = &load;
        pools.dispatch(&pools_t::prepare);
        time_point_t const begin = start_at(number, process);
        pools.window = {.begin = begin,
                        .measured_from = begin + settings.warmup,
                        .until = begin + settings.warmup + settings.time_limit,
                        .calls_per_second = settings.calls_per_second,
                        .slots = run.in_flight};
        std::optional<std::uint64_t> const opens_before = tcp_active_opens();
        pools.dispatch(&pools_t::measure);
        std::optional<std::uint64_t> const opens_after = tcp_active_opens();
        unit_stats_t const server_after = server_stats(shared, control);
        double const client_cpu_seconds = std::chrono::duration<double>(process_cpu() - cpu_before).count();
        tally_t tally;
        for (worker_t const& worker : pools.workers) tally.merge(worker.tally);

        window_t const& window = pools.window;
        double const seconds = std::chrono::duration<double>(window.until - window.measured_from).count();
        std::optional<std::uint64_t> const server_cpu = growth(server_before.cpu_microseconds,
                                                               server_after.cpu_microseconds);
        std::optional<std::int64_t> const server_memory =
            server_before.memory_bytes && server_after.memory_bytes
                ? std::optional {static_cast<std::int64_t>(*server_after.memory_bytes) -
                                 static_cast<std::int64_t>(*server_before.memory_bytes)}
                : std::nullopt;
        std::uint64_t const errors = tally.failures.total();
        double const latency_p50 = tally.latencies.percentile(0.5), lateness_p99 = tally.lateness.percentile(0.99);
        double const client_cpu_percent = 100.0 * client_cpu_seconds /
                                          std::chrono::duration<double>(settings.warmup + settings.time_limit).count();
        bool const offered_short = settings.calls_per_second &&
                                   (tally.calls + errors) / seconds <
                                       0.99 * static_cast<double>(*settings.calls_per_second);
        double const client_cpu_share = client_cpu_percent / 100.0 / machine.cores;
        bool const client_limited =
            offered_short || lateness_p99 > 0.05 * latency_p50 || client_cpu_share > 0.9;
        std::optional<double> const cpu_per_call =
            server_cpu ? std::optional {static_cast<double>(*server_cpu) / std::max<std::uint64_t>(tally.all_calls, 1)}
                       : std::nullopt;
        std::optional<double> const kb_per_connection = server_memory
                                                            ? std::optional {*server_memory / 1024.0 / run.connections}
                                                            : std::nullopt;
        std::string const config = std::format(
            R"({{"framework":{},"client":{},"language":"cpp","workload":"{}","variant":"{}","parsing":"{}","security":"{}","phase":"{}","process":{},"processes":{},"interfaces":{},"threads":{},"connections":{},"in_flight":{},"pipeline_depth":1,"body_bytes":{},"width":{},"offered_calls_per_second":{},"seed":{},"between_runs":"{}"}})",
            json_string(framework), json_string(client_name), workload_names_k[run.workload],
            variant_names_k[run.variant], parsing_names_k[run.parsing], security_names_k[settings.security_choice],
            phase_names_k[run.phase], process.index, process.count, settings.interfaces.size(), settings.threads,
            run.connections, run.in_flight, run.body_bytes, settings.width,
            settings.calls_per_second ? std::format("{}", *settings.calls_per_second) : "null", settings.seed,
            between_runs_names_k[settings.between_runs]);
        std::string const report = std::format(
            R"({{"schema_version":2,"started_unix_seconds":{},"machine":{},"config":{},"ucall_commit":null,"tls_suite":null,"domains":null,"seconds":{},"window_unix_ms":[{},{}],"calls":{},"errors":{},"error_classes":{{"connect":{},"refused":{},"timeout":{},"status":{},"wrong_reply":{}}},"unsent":{},"all_calls":{},"request_body_bytes":{},"response_body_bytes":{},"reconnects":{},"upstream_connections":{},"server_cpu_microseconds":{},"server_memory_bytes":{},"client_cpu_seconds":{},"calls_per_second":{},"body_gigabytes_per_second":{},"latency_us":{},"latency_buckets":{},"send_lateness_us":{},"send_lateness_buckets":{},"server_cpu_us_per_call":{},"server_kb_per_connection":{},"client_cpu_percent":{},"client_cpu_share":{},"client_limited":{},"server_budget":{{"cores":{},"memory_limit":{}}}}})",
            started, machine.json, config, seconds, unix_milliseconds(window.measured_from),
            unix_milliseconds(window.until), tally.calls, errors, tally.failures.connect, tally.failures.refused,
            tally.failures.timeout, tally.failures.status, tally.failures.wrong_reply, tally.unsent, tally.all_calls,
            tally.request_body_bytes, tally.response_body_bytes, spell(growth(opens_before, opens_after)),
            spell(growth(server_before.tcp_active_opens, server_after.tcp_active_opens)), spell(server_cpu),
            spell(server_memory), client_cpu_seconds, tally.calls / seconds,
            (tally.request_body_bytes + tally.response_body_bytes) / seconds / 1e9, tally.latencies.percentiles_json(),
            tally.latencies.sparse_json(), tally.lateness.percentiles_json(), tally.lateness.sparse_json(),
            spell(cpu_per_call), spell(kb_per_connection), client_cpu_percent,
            client_cpu_share, client_limited, server_after.cores,
            spell(server_after.memory_limit));
        if (!printed_header) {
            print_line(stderr,
                       "| Machine | Framework | Workload | Parsing | Security | Phase | Connections | calls/s | p50, "
                       "µs | p99, µs | Body GB/s | Server CPU, µs/call | KB per connection | Client CPU, % |");
            print_line(stderr,
                       "| :------ | :-------- | :------- | :------ | :------- | :---- | ----------: | ------: | "
                       "------: | ------: | --------: | ------------------: | ----------------: | ------------: |");
            printed_header = true;
        }
        print_line(
            stderr,
            std::format("| {} | {} | {} | {} | {} | {} | {} | {:.0f} | {:.1f} | {:.1f} | {:.3f} | {} | {} | {:.0f} |",
                        machine.cpu_model, framework, workload_names_k[run.workload], parsing_names_k[run.parsing],
                        security_names_k[settings.security_choice], phase_names_k[run.phase], run.connections,
                        tally.calls / seconds, latency_p50, tally.latencies.percentile(0.99),
                        (tally.request_body_bytes + tally.response_body_bytes) / seconds / 1e9,
                        spell_cell(cpu_per_call), spell_cell(kb_per_connection), client_cpu_percent));
        print_line(stdout, report);
        std::fflush(stdout);
        write_report(settings.output, framework, config, report);
        if (settings.between_runs != reconnect_k) continue;
        for (worker_t& worker : pools.workers) worker.client = {};
        std::optional<std::string> earlier_sockets = sockets_in_use();
        std::optional<std::uint64_t> earlier_memory = server_stats(shared, control).memory_bytes;
        for (std::size_t round = 0; round != settle_rounds_k; ++round) {
            std::this_thread::sleep_for(settle_interval_k);
            std::optional<std::string> const sockets = sockets_in_use();
            std::optional<std::uint64_t> const memory = server_stats(shared, control).memory_bytes;
            bool const memory_settled =
                earlier_memory && memory
                    ? (std::max(*memory, *earlier_memory) - std::min(*memory, *earlier_memory)) * 100 <= *earlier_memory
                    : earlier_memory == memory;
            if (sockets == earlier_sockets && memory_settled) break;
            earlier_sockets = sockets, earlier_memory = memory;
        }
    }
    return 0;
}
