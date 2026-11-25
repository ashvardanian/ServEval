/**
 *  @file ucall_server.cpp
 *  @brief UCall's C++ server: one server over every interface, whose event loops serve all of them, each polled by its
 *      own thread that also drives its loop's bound fan-out client and streamed uploads, with Noise IK or XX.
 *
 *  Tool calls parse their whole body with yyjson, the parser UCall's Python binding builds with.
 */
#include <csignal> // `std::signal`, `SIGINT`, `SIGTERM`, `sigset_t`, `sigemptyset`, `sigaddset`, `sigwait`
#include <cstdio>  // `std::fflush`, `stdout`

#include <pthread.h> // `pthread_sigmask`

#include <algorithm>   // `std::max_element`
#include <deque>       // `std::deque`
#include <functional>  // `std::ref`
#include <latch>       // `std::latch`
#include <string>      // `std::string`
#include <string_view> // `std::string_view`
#include <thread>      // `std::thread`, `std::this_thread::get_id`
#include <vector>      // `std::vector`

#include <yyjson.h> // `yyjson_read`, `yyjson_obj_get`, `yyjson_doc_free`

#include "ucall/client.hpp" // `uc::request_t`, `uc::batch_t`, `uc::response_t`, `uc::deferred`, `uc::bytes_view`
#include "serveval.hpp"     // `command_line_t`, `tool_reply`, `full_reply`, `fanout_reply`, `count_reply`

namespace uc = unum::ucall;

namespace serveval {

flag_t const flags_k[] {
    {"--threads", "0", "Event loops, 0 for every core in the affinity mask."},
    {"--interfaces", "127.0.0.1", "Local interfaces as DEVICE=ADDRESS or ADDRESS, extra addresses joined by +."},
    {"--port", "8545", "Port every interface listens on."},
    {"--security", "none", "none, noise-xx or noise-ik."},
    {"--data-dir", "data", "Directory holding the noise keys that keys.sh writes."},
    {"--seed", "42", "Seed of every payload, or random."},
    {"--connections", "10K", "Connection counts; the largest is the admission budget."},
    {"--upstreams", "", "Fan-out targets as ADDRESS:PORT."},
    {"--batch-size", "32", "Most calls one batch gathers."},
    {"--batch-wait", "10ms", "Longest a batch waits to fill."},
};

/** The longest one poll waits; an unbounded wait fails the loop with an I/O error on macOS's kqueue backend. */
inline constexpr std::size_t poll_slice_microseconds_k = 100'000;

/** A request body parsed whole, its fields typed, the strings viewing the parsed document. */
class parsed_call_t {
    yyjson_doc* document_ = nullptr;

    static bool string_(yyjson_val* value, std::string_view& text) noexcept {
        if (!yyjson_is_str(value)) return false;
        text = {yyjson_get_str(value), yyjson_get_len(value)};
        return true;
    }

  public:
    std::string_view id, query, trace, kind, name, lang;
    std::uint64_t top_k = 0, after = 0;

    explicit parsed_call_t(std::string_view body) noexcept : document_(yyjson_read(body.data(), body.size(), 0)) {}
    parsed_call_t(parsed_call_t const&) = delete;
    parsed_call_t& operator=(parsed_call_t const&) = delete;
    ~parsed_call_t() noexcept { yyjson_doc_free(document_); }

    /** Reads a tool call: an id, type, name and trace, and arguments with a query, `top_k` and filters. */
    bool tool() noexcept {
        yyjson_val* root = yyjson_doc_get_root(document_);
        yyjson_val* arguments = yyjson_obj_get(root, "arguments");
        yyjson_val* filters = yyjson_obj_get(arguments, "filters");
        yyjson_val* top_k_value = yyjson_obj_get(arguments, "top_k");
        yyjson_val* after_value = yyjson_obj_get(filters, "after");
        if (!yyjson_is_uint(top_k_value) || !yyjson_is_uint(after_value)) return false;
        top_k = yyjson_get_uint(top_k_value), after = yyjson_get_uint(after_value);
        return string_(yyjson_obj_get(root, "id"), id) && string_(yyjson_obj_get(root, "type"), kind) &&
               string_(yyjson_obj_get(root, "name"), name) && string_(yyjson_obj_get(root, "trace"), trace) &&
               string_(yyjson_obj_get(arguments, "query"), query) && string_(yyjson_obj_get(filters, "lang"), lang);
    }

    /** Reads the body of a `full` call: an id, query, `top_k` and trace. */
    bool full() noexcept {
        yyjson_val* root = yyjson_doc_get_root(document_);
        yyjson_val* top_k_value = yyjson_obj_get(root, "top_k");
        if (!yyjson_is_uint(top_k_value)) return false;
        top_k = yyjson_get_uint(top_k_value);
        return string_(yyjson_obj_get(root, "id"), id) && string_(yyjson_obj_get(root, "query"), query) &&
               string_(yyjson_obj_get(root, "trace"), trace);
    }
};

/** A streamed upload, counted pull by pull between the polls of its loop. */
struct upload_t {
    uc::request_t request;
    uc::deferred<std::string_view> pull;
    std::uint64_t bytes = 0;
};

/** A fan-out its loop forwards to the upstreams and answers once every reply arrived. */
struct fanout_t {
    uc::request_t request;
    std::string_view body;
    reply_t expected;
    reply_t reply;
    std::size_t width = 0;
    std::size_t unsent = 0;
    std::size_t unfinished = 0;
    bool failed = false;
};

constexpr std::string_view json_k = "content-type: application/json";

/** The upstream `/tool` URLs, and the headers and security of every fan-out call, kept while calls are in flight. */
struct upstreams_t {
    std::vector<std::string> urls;
    ucall_bytes_view_t headers[2] {uc::bytes_view(json_k), uc::bytes_view("accept-encoding: identity")};
    ucall_client_call_options_t options {};
    std::size_t connections = 0;
};

void reply(uc::request_t request, std::string_view header_line, std::string_view body) noexcept {
    ucall_bytes_view_t const header = uc::bytes_view(header_line);
    [[maybe_unused]] uc::status_t const replied = std::move(request).reply(ucall_http_status_ok_k, {&header, 1}, body);
}

void reject(uc::request_t request, ucall_http_status_t status) noexcept {
    [[maybe_unused]] uc::status_t const replied = std::move(request).reply_error(status, "");
}

/** One event loop's bound client, fan-outs and uploads, touched only by the thread polling it. */
struct event_loop_t {
    std::thread::id owner;
    ucall_client_t client = nullptr;
    std::vector<uc::request_t> arrived;
    std::deque<upload_t> uploads;
    std::vector<upload_t*> spare_uploads, active_uploads;
    std::deque<fanout_t> fanouts;
    std::vector<fanout_t*> spare_fanouts;
    std::deque<fanout_t*> waiting;
    std::vector<ucall_completed_call_t> completed;
    std::size_t pulls = 0;

    fanout_t& add_fanout() noexcept {
        if (spare_fanouts.empty()) spare_fanouts.push_back(&fanouts.emplace_back());
        fanout_t& fanout = *spare_fanouts.back();
        spare_fanouts.pop_back();
        waiting.push_back(&fanout);
        return fanout;
    }

    void settle(fanout_t& fanout) noexcept {
        if (fanout.unfinished) return;
        if (fanout.failed) reject(std::move(fanout.request), ucall_http_status_bad_gateway_k);
        else reply(std::move(fanout.request), json_k, fanout.reply.view());
        spare_fanouts.push_back(&fanout);
    }

    /** Admits the waiting calls in order until the client is full. */
    void submit(upstreams_t const& upstreams) noexcept {
        while (!waiting.empty()) {
            fanout_t& fanout = *waiting.front();
            std::string_view const url = upstreams.urls[(fanout.width - fanout.unsent) % upstreams.urls.size()];
            ucall_outcome_t outcome {};
            ucall_client_call_t call {.client = client,
                                      .outcome = &outcome,
                                      .url = uc::bytes_view(url),
                                      .verb = ucall_verb_post_k,
                                      .headers = upstreams.headers,
                                      .headers_count = std::size(upstreams.headers),
                                      .body = uc::bytes_view(fanout.body),
                                      .options = upstreams.options,
                                      .context = &fanout};
            ucall_client_call(&call);
            if (outcome.status == ucall_status_contended_k) return;
            if (outcome.status != ucall_status_success_k) {
                --fanout.unfinished;
                fanout.failed = true;
            }
            if (--fanout.unsent) continue;
            waiting.pop_front();
            settle(fanout);
        }
    }

    void finish(ucall_completed_call_t const& call) noexcept {
        fanout_t& fanout = *static_cast<fanout_t*>(call.context);
        uc::response_t const response {client, call.response};
        bool matches = call.outcome.status == ucall_status_success_k &&
                       response.status_code() == ucall_http_status_ok_k;
        if (matches) {
            auto const [body, body_status] = response.body();
            matches = body_status.succeeded() && body == fanout.expected.view();
        }
        fanout.failed |= !matches;
        --fanout.unfinished;
        settle(fanout);
    }

    void pull(upload_t& upload) noexcept {
        ++pulls;
        [[maybe_unused]] uc::status_t const pulled = upload.request.body_chunk(upload.pull);
    }

    /** Counts what the upload's finished pulls brought, pulling again until its body ends; answers whether it did. */
    bool count(upload_t& upload) noexcept {
        while (upload.pull.status() != uc::status_code_t::deferred_k) {
            auto const [chunk, chunk_status] = upload.pull.wait();
            if (chunk_status.failed()) {
                reject(std::move(upload.request), ucall_http_status_bad_request_k);
                return true;
            }
            if (chunk.empty()) {
                reply(std::move(upload.request), "content-type: text/plain", count_reply(upload.bytes).view());
                return true;
            }
            upload.bytes += chunk.size();
            pull(upload);
        }
        return false;
    }

    void pull_uploads() noexcept {
        for (std::size_t index = 0; index < active_uploads.size();) {
            if (!count(*active_uploads[index])) {
                ++index;
                continue;
            }
            spare_uploads.push_back(active_uploads[index]);
            active_uploads[index] = active_uploads.back();
            active_uploads.pop_back();
        }
    }

    /** Between two polls: starts the uploads that arrived, forwards fan-outs, and answers what finished. */
    void advance(upstreams_t const& upstreams) noexcept {
        for (uc::request_t& request : arrived) {
            if (spare_uploads.empty()) spare_uploads.push_back(&uploads.emplace_back());
            upload_t& upload = *spare_uploads.back();
            spare_uploads.pop_back();
            upload.request = std::move(request);
            upload.bytes = 0;
            pull(upload);
            active_uploads.push_back(&upload);
        }
        arrived.clear();
        submit(upstreams);
        for (std::size_t count = completed.size(); count == completed.size();) {
            ucall_outcome_t outcome {};
            ucall_client_take_completed_t take {.client = client,
                                                .outcome = &outcome,
                                                .calls = completed.data(),
                                                .capacity = completed.size(),
                                                .count = &count,
                                                .timeout_microseconds = 0};
            ucall_client_take_completed(&take);
            if (outcome.status != ucall_status_success_k) count = 0;
            for (std::size_t index = 0; index != count; ++index) finish(completed[index]);
            submit(upstreams);
        }
        pull_uploads();
    }
};

/** Everything the handlers reach through their context. */
struct server_state_t {
    ucall_server_t server = nullptr;
    std::vector<event_loop_t> loops;
    upstreams_t upstreams;

    /** The loop the calling thread polls, which handlers always run on. */
    event_loop_t& current() noexcept {
        std::thread::id const thread = std::this_thread::get_id();
        for (event_loop_t& loop : loops)
            if (loop.owner == thread) return loop;
        exit_with("a handler ran outside the polling threads");
    }
};

/** The `/stats` body: this process's cgroup v2 counters, every worker and the kernel's socket memory included. */
std::string unit_json() noexcept {
    auto spell = [](std::optional<std::uint64_t> value) noexcept {
        return value ? std::format("{}", *value) : std::string {"null"};
    };
    auto integer = [](std::optional<std::string_view> text) noexcept {
        return text ? parse_digits<std::uint64_t>(*text) : std::nullopt;
    };
    std::optional<std::uint64_t> cpu, memory, limit;
    std::optional<std::string> const membership = read_file("/proc/self/cgroup");
    std::optional<std::string_view> const relative = membership ? line_value(*membership, "0::") : std::nullopt;
    if (relative) {
        std::string directory = std::format("/sys/fs/cgroup{}", *relative);
        std::optional<std::string> const stat = read_file(directory + "/cpu.stat");
        std::optional<std::string> const current = read_file(directory + "/memory.current");
        cpu = integer(stat ? line_value(*stat, "usage_usec ") : std::nullopt);
        memory = integer(current ? line_value(*current, "") : std::nullopt);
        for (; directory.starts_with("/sys/fs/cgroup"); directory.resize(directory.rfind('/'))) {
            std::optional<std::string> const maximum = read_file(directory + "/memory.max");
            std::optional<std::uint64_t> const bound = integer(maximum ? line_value(*maximum, "") : std::nullopt);
            if (bound && (!limit || *bound < *limit)) limit = bound;
        }
    }
    return std::format(
        R"({{"cpu_microseconds":{},"memory_bytes":{},"cores":{},"memory_limit":{},"tcp_active_opens":{}}})", spell(cpu),
        spell(memory), all_cores(), spell(limit), spell(tcp_active_opens()));
}

void raw(ucall_server_t server, ucall_received_http_request_t handle, void*) noexcept {
    uc::request_t request {server, handle};
    auto const [body, body_status] = request.body();
    reply(std::move(request), "content-type: application/octet-stream", body.substr(0, reply_bytes_k));
}

void tool(uc::request_t request) noexcept {
    auto const [body, body_status] = request.body();
    parsed_call_t call {body};
    if (body_status.failed() || !call.tool()) return reject(std::move(request), ucall_http_status_bad_request_k);
    reply(std::move(request), json_k, tool_reply(call.id, call.after + call.top_k).view());
}

void tool_route(ucall_server_t server, ucall_received_http_request_t handle, void*) noexcept {
    tool(uc::request_t {server, handle});
}

void full(ucall_server_t server, ucall_received_http_request_t handle, void*) noexcept {
    uc::request_t request {server, handle};
    auto const [authorization, authorization_status] = request.header("authorization");
    if (authorization_status.failed() || authorization != authorization_k)
        return reject(std::move(request), ucall_http_status_unauthorized_k);
    auto const [traceparent, traceparent_status] = request.header("traceparent");
    auto const [name, name_status] = request.path_parameter("name");
    auto const [lang, lang_status] = request.query("lang");
    auto const [after, after_status] = request.query("after");
    auto const [body, body_status] = request.body();
    std::optional<std::string_view> const trace = trace_id(traceparent);
    std::optional<std::uint64_t> const after_value = parse_digits<std::uint64_t>(after);
    parsed_call_t call {body};
    if (!trace || !after_value || name_status.failed() || lang_status.failed() || body_status.failed() || !call.full())
        return reject(std::move(request), ucall_http_status_bad_request_k);
    reply(std::move(request), json_k, full_reply(call.id, name, *trace, lang, *after_value + call.top_k).view());
}

void context(ucall_server_t server, ucall_received_http_request_t handle, void* state) noexcept {
    static_cast<server_state_t*>(state)->current().arrived.emplace_back(server, handle);
}

void fanout(ucall_server_t server, ucall_received_http_request_t handle, void* context) noexcept {
    server_state_t& state = *static_cast<server_state_t*>(context);
    uc::request_t request {server, handle};
    auto const [body, body_status] = request.body();
    auto const [width_text, width_status] = request.query("width");
    std::optional<std::size_t> const width = parse_digits<std::size_t>(width_text);
    parsed_call_t call {body};
    if (body_status.failed() || !width || !*width || !call.tool())
        return reject(std::move(request), ucall_http_status_bad_request_k);
    if (state.upstreams.urls.empty()) return reject(std::move(request), ucall_http_status_service_unavailable_k);
    fanout_t& forwarded = state.current().add_fanout();
    forwarded.expected = tool_reply(call.id, call.after + call.top_k);
    forwarded.reply = fanout_reply(call.id, *width);
    forwarded.width = forwarded.unsent = forwarded.unfinished = *width;
    forwarded.failed = false;
    forwarded.body = body;
    forwarded.request = std::move(request);
}

void stats(ucall_server_t server, ucall_received_http_request_t handle, void*) noexcept {
    reply(uc::request_t {server, handle}, json_k, unit_json());
}

void health(ucall_server_t server, ucall_received_http_request_t handle, void*) noexcept {
    reply(uc::request_t {server, handle}, json_k, R"({"framework":"ucall","client":"ucall","batching":true})");
}

void batch(ucall_server_t server, ucall_received_http_request_t const* requests, std::size_t count, void*) noexcept {
    for (uc::request_t request : uc::batch_t {server, {requests, count}}) tool(std::move(request));
}

/** `ADDRESS:PORT`, the address in brackets when it is IPv6. */
std::optional<std::string> parse_upstream(std::string_view text) noexcept {
    std::size_t const colon = text.rfind(':');
    if (colon == std::string_view::npos) return {};
    std::string_view host = text.substr(0, colon);
    if (host.starts_with('[') && host.ends_with(']')) host = host.substr(1, host.size() - 2);
    std::optional<std::uint16_t> const port = parse_port(text.substr(colon + 1));
    std::optional<std::string> const address = parse_address(host);
    if (!port || !address) return {};
    return host_port(*address, *port);
}

void drain(ucall_server_t server, ucall_http_batch_endpoint_t endpoint) noexcept {
    ucall_outcome_t outcome {};
    ucall_server_take_batches_t take {.server = server, .outcome = &outcome, .endpoint = endpoint};
    ucall_server_take_batches(&take);
}

/** Polls @p index until the server stops, between polls advancing its loop's uploads and fan-outs. */
void poll(server_state_t& state, std::size_t index, ucall_client_options_t options, std::latch& registered) noexcept {
    event_loop_t& loop = state.loops[index];
    loop.owner = std::this_thread::get_id();
    options.event_loop = index;
    ucall_outcome_t outcome {};
    ucall_client_init_t init {.client = &loop.client, .outcome = &outcome, .options = &options, .resolved = nullptr};
    ucall_client_init(&init);
    if (outcome.status != ucall_status_success_k)
        exit_with(std::format("UCall client: {}", uc::status_name(static_cast<uc::status_code_t>(outcome.status))));
    loop.completed.resize(state.upstreams.connections);
    registered.arrive_and_wait();
    for (std::size_t pulls = loop.pulls, waited = loop.pulls;; waited = pulls, pulls = loop.pulls) {
        // A pull the loop answers from bytes it already holds finishes inside a poll without waking it, so the poll
        // after new pulls does not wait.
        ucall_server_take_call_t take {.server = state.server,
                                       .outcome = &outcome,
                                       .event_loop_index = index,
                                       .timeout_microseconds = pulls != waited ? 0 : poll_slice_microseconds_k};
        ucall_server_take_call(&take);
        if (outcome.status == ucall_status_connection_closed_k) break;
        if (outcome.status != ucall_status_success_k)
            exit_with(std::format("event loop {} failed: {}", index, outcome.message ? outcome.message : ""));
        loop.advance(state.upstreams);
    }
    ucall_client_free(loop.client);
}

} // namespace serveval

int main(int argc, char** argv) {
    using namespace serveval;
    command_line_t const line {argc, argv, flags_k, "UCall serving the tool, context and fan-out workloads."};
    std::size_t const threads = line.read("--threads", parse_threads, "a count, 0 for all cores");
    std::vector<interface_t> const interfaces = line.read_list<interface_t>("--interfaces", parse_interface,
                                                                            "DEVICE=ADDRESS or ADDRESS, joined by +");
    std::uint16_t const port = line.read("--port", parse_port, "a port from 1 to 65535");
    ucall_security_t const security = security_of(
        line.read("--security", parse_security, "one of none, noise-xx, noise-ik, tls"));
    std::string_view const data_dir = line.text("--data-dir");
    [[maybe_unused]] std::uint32_t const seed = line.read("--seed", parse_seed, "an unsigned integer or random");
    std::vector<std::size_t> const connections = line.read_list<std::size_t>("--connections", parse_count,
                                                                             "a positive count like 1000 or 10M");
    std::size_t const batch_size = line.read("--batch-size", parse_count, "a positive count like 1000 or 10M");
    std::chrono::milliseconds const batch_wait = line.read("--batch-wait", parse_duration,
                                                           "a duration like 200ms or 10s");
    std::size_t const admission = *std::max_element(connections.begin(), connections.end());
    std::vector<std::string> upstreams;
    if (!line.text("--upstreams").empty())
        upstreams = line.read_list<std::string>("--upstreams", parse_upstream, "ADDRESS:PORT");

    bool const noise = security != ucall_security_none_k;
    ucall_noise_key_t server_key {}, server_public {}, client_key {}, client_public {};
    if (noise) {
        read_key(data_dir, "server.key", server_key);
        read_key(data_dir, "server.pub", server_public);
        read_key(data_dir, "client.key", client_key);
        read_key(data_dir, "client.pub", client_public);
    }
    std::vector<std::string> addresses;
    for (interface_t const& interface : interfaces) addresses.push_back(host_port(interface.addresses[0], port));
    std::vector<ucall_network_interface_t> listeners;
    for (std::size_t index = 0; index != interfaces.size(); ++index)
        listeners.push_back({.address = uc::bytes_view(addresses[index]),
                             .listen_descriptor = nullptr,
                             .device_name = uc::bytes_view(interfaces[index].device),
                             .security = security,
                             .memory_domain = -1,
                             .noise_private_key = noise ? &server_key : nullptr,
                             .noise_peer_key = nullptr,
                             .peer_authorizer = noise ? &admit_known_peer : nullptr,
                             .peer_authorizer_context = &client_public,
                             .event_loops = threads,
                             .max_concurrent_connections = admission,
                             .max_pipelined_requests = 0,
                             .max_request_bytes = 0,
                             .reply_directories = nullptr,
                             .reply_directories_count = 0});
    ucall_server_options_t const options {.interfaces = listeners.data(), .interfaces_count = listeners.size()};
    ucall_server_options_t resolved {};
    server_state_t state;
    ucall_outcome_t outcome {};
    ucall_server_init_t init {.server = &state.server, .outcome = &outcome, .options = &options, .resolved = &resolved};
    ucall_server_init(&init);
    if (outcome.status != ucall_status_success_k)
        exit_with(std::format("UCall: {}", outcome.message ? outcome.message : ""));
    for (std::size_t index = 0; index != resolved.interfaces_count; ++index)
        if (resolved.interfaces[index].event_loops != threads)
            exit_with(
                std::format("UCall runs {} event loops, not {}", resolved.interfaces[index].event_loops, threads));

    std::string_view const scheme = noise ? "http+noise" : "http";
    for (std::string const& upstream : upstreams)
        state.upstreams.urls.push_back(std::format("{}://{}/tool", scheme, upstream));
    state.upstreams.options = {.security = security, .noise_peer_key = noise ? &server_public : nullptr};
    // Each loop shares the upstream connections among the downstream calls it may hold.
    state.upstreams.connections = (admission + threads - 1) / threads * std::max<std::size_t>(upstreams.size(), 1);
    state.loops = std::vector<event_loop_t>(threads);  // `resize` would instantiate the deleted copies

    struct route_t {
        std::string_view path;
        ucall_verb_t verb;
        ucall_received_http_request_callback_t callback;
        ucall_body_delivery_t delivery;
    };
    route_t const routes[] {
        {"/raw", ucall_verb_post_k, &raw, ucall_body_delivery_buffered_k},
        {"/tool", ucall_verb_post_k, &tool_route, ucall_body_delivery_buffered_k},
        {"/tool/{name}", ucall_verb_post_k, &full, ucall_body_delivery_buffered_k},
        {"/context", ucall_verb_put_k, &context, ucall_body_delivery_streamed_k},
        {"/fanout", ucall_verb_post_k, &fanout, ucall_body_delivery_buffered_k},
        {"/stats", ucall_verb_get_k, &stats, ucall_body_delivery_buffered_k},
        {"/health", ucall_verb_get_k, &health, ucall_body_delivery_buffered_k},
    };
    for (route_t const& route : routes) {
        ucall_server_add_http_endpoint_t add {.server = state.server,
                                              .outcome = &outcome,
                                              .uri_template = uc::bytes_view(route.path),
                                              .verb = route.verb,
                                              .callback = route.callback,
                                              .context = &state,
                                              .body_delivery = route.delivery};
        ucall_server_add_http_endpoint(&add);
        if (outcome.status != ucall_status_success_k) exit_with(std::format("UCall refused {}", route.path));
    }
    ucall_http_batch_endpoint_t batches = nullptr;
    ucall_server_add_http_batch_endpoint_t add_batch {
        .server = state.server,
        .outcome = &outcome,
        .uri_template = uc::bytes_view("/tool/batch"),
        .verb = ucall_verb_post_k,
        .callback = &batch,
        .context = nullptr,
        .min_batch_size = batch_size,
        .max_batch_size = batch_size,
        .max_latency_microseconds = static_cast<std::size_t>(std::chrono::microseconds(batch_wait).count()),
        .endpoint = &batches};
    ucall_server_add_http_batch_endpoint(&add_batch);
    if (outcome.status != ucall_status_success_k) exit_with("UCall refused /tool/batch");

    sigset_t stopping;
    sigemptyset(&stopping);
    sigaddset(&stopping, SIGINT);
    sigaddset(&stopping, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stopping, nullptr);
    // A shell starting a background job ignores SIGINT for it, and `sigwait` never sees an ignored signal.
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    ucall_client_options_t const client_options {.max_concurrent_connections = state.upstreams.connections,
                                                 .noise_private_key = noise ? &client_key : nullptr,
                                                 .peer_authorizer = noise ? &admit_known_peer : nullptr,
                                                 .peer_authorizer_context = &server_public,
                                                 .server = state.server};
    std::latch registered {static_cast<std::ptrdiff_t>(threads)};
    std::vector<std::thread> pollers;
    for (std::size_t index = 0; index != threads; ++index)
        pollers.emplace_back(&poll, std::ref(state), index, client_options, std::ref(registered));
    pollers.emplace_back(&drain, state.server, batches);
    std::string listening;
    for (std::string const& address : addresses)
        listening += std::format("{}{}", listening.empty() ? "" : ", ", address);
    print_line(stdout, std::format("Listening on {} with {} UCall event loops", listening, threads));
    std::fflush(stdout);
    int signal_number = 0;
    sigwait(&stopping, &signal_number);
    ucall_server_stop(state.server);
    for (std::thread& poller : pollers) poller.join();
    ucall_server_free(state.server);
    return 0;
}
