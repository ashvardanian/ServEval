/**
 *  @file serveval.hpp
 *  @brief The flags, seeded payloads and Noise keys the C++ UCall server and client share, spelled as `arguments.rs`
 *      reads the flags and byte-identical with the bodies and replies of `payloads.rs`.
 */
#pragma once

#include <cstdint> // `std::uint32_t`, `std::uint64_t`
#include <cstdio>  // `std::FILE`, `std::fopen`, `std::fread`, `std::fwrite`, `stderr`, `stdout`
#include <cstdlib> // `std::exit`, `std::getenv`

#include <arpa/inet.h> // `inet_pton`, `AF_INET`, `AF_INET6`

#include <algorithm>   // `std::find_if`
#include <charconv>    // `std::from_chars`
#include <chrono>      // `std::chrono::milliseconds`
#include <format>      // `std::format`, `std::format_to`, `std::format_to_n`, `std::format_string`
#include <optional>    // `std::optional`
#include <random>      // `std::random_device`
#include <span>        // `std::span`
#include <string>      // `std::string`
#include <string_view> // `std::string_view`
#include <thread>      // `std::thread::hardware_concurrency`
#include <vector>      // `std::vector`

#include "ucall/types.h" // `ucall_noise_key_t`

namespace serveval {

#pragma region Output

/** Writes @p text and a newline to @p stream. */
inline void print_line(std::FILE* stream, std::string_view text) noexcept {
    std::fwrite(text.data(), 1, text.size(), stream);
    std::fputc('\n', stream);
}

/** Prints @p message and exits with 1. */
[[noreturn]] inline void exit_with(std::string_view message) noexcept {
    print_line(stderr, message);
    std::exit(1);
}

#pragma endregion Output

#pragma region Flags

/** One flag a binary takes, its default when neither the command line nor `SERVEVAL_<FLAG>` sets it. */
struct flag_t {
    std::string_view name;
    std::string_view fallback;
    std::string_view help;
};

/** A command line whose every flag reads `SERVEVAL_<FLAG>` as its default, upper-cased with dashes as underscores. */
class command_line_t {
    std::span<char*> arguments_;
    std::span<flag_t const> flags_;

  public:
    /** Exits on an unknown flag or one without a value, and prints the usage for `--help`. */
    command_line_t(int argc, char** argv, std::span<flag_t const> flags, std::string_view about) noexcept
        : arguments_(argv + 1, static_cast<std::size_t>(argc - 1)), flags_(flags) {
        for (std::size_t index = 0; index != arguments_.size(); ++index) {
            std::string_view const argument = arguments_[index];
            if (argument == "--help" || argument == "-h") {
                print_line(stdout, std::format("{}\n\nOptions:", about));
                for (flag_t const& flag : flags_)
                    print_line(stdout, std::format("  {} <VALUE>    {} [default: {}] [env: {}]", flag.name, flag.help,
                                                   flag.fallback, variable(flag.name)));
                std::exit(0);
            }
            std::string_view const name = argument.substr(0, argument.find('='));
            bool const known = std::any_of(flags_.begin(), flags_.end(),
                                           [&](flag_t const& flag) noexcept { return flag.name == name; });
            if (!known) exit_with(std::format("unexpected argument '{}'; see --help", argument));
            if (name.size() == argument.size() && ++index == arguments_.size())
                exit_with(std::format("{} needs a value; see --help", name));
        }
    }

    /** The environment variable `--time-limit` reads, `SERVEVAL_TIME_LIMIT`. */
    static std::string variable(std::string_view name) noexcept {
        std::string spelled {"SERVEVAL_"};
        for (char character : name.substr(2)) spelled += character == '-' ? '_' : static_cast<char>(character & ~0x20);
        return spelled;
    }

    /** The text of flag @p name: the last one on the command line, else its variable, else its default. */
    std::string_view text(std::string_view name) const noexcept {
        std::optional<std::string_view> given;
        for (std::size_t index = 0; index != arguments_.size(); ++index) {
            std::string_view const argument = arguments_[index];
            if (argument == name) given = arguments_[++index];
            else if (argument.starts_with(name) && argument.size() > name.size() && argument[name.size()] == '=')
                given = argument.substr(name.size() + 1);
        }
        if (given) return *given;
        if (char const* set = std::getenv(variable(name).c_str()); set && *set) return set;
        for (flag_t const& flag : flags_)
            if (flag.name == name) return flag.fallback;
        return {};
    }

    /** Flag @p name parsed by @p parse, exiting with `--name="text" does not parse, expected ...` when it fails. */
    template <typename parse_type_>
    auto read(std::string_view name, parse_type_ parse, std::string_view expected) const noexcept {
        std::string_view const spelled = text(name);
        auto parsed = parse(spelled);
        if (!parsed) exit_with(std::format("{}=\"{}\" does not parse, expected {}", name, spelled, expected));
        return *parsed;
    }

    /** Every comma-separated item of flag @p name, parsed by @p parse. */
    template <typename item_type_, typename parse_type_>
    std::vector<item_type_> read_list(std::string_view name, parse_type_ parse,
                                      std::string_view expected) const noexcept {
        std::string_view const spelled = text(name);
        std::vector<item_type_> items;
        for (std::size_t first = 0; first <= spelled.size();) {
            std::size_t const comma = std::min(spelled.find(',', first), spelled.size());
            auto parsed = parse(spelled.substr(first, comma - first));
            if (!parsed) exit_with(std::format("{}=\"{}\" does not parse, expected {}", name, spelled, expected));
            items.push_back(*parsed);
            first = comma + 1;
        }
        return items;
    }
};

/** Digits alone as an unsigned integer, or nothing. */
template <typename integer_type_>
std::optional<integer_type_> parse_digits(std::string_view text) noexcept {
    integer_type_ value {};
    auto const [end, parse_status] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parse_status != std::errc {} || end != text.data() + text.size()) return {};
    return value;
}

/** A count like `10000` or `10M`: `K`, `M`, `G`, `T` are powers of 1000 in any case, and zero is refused. */
inline std::optional<std::size_t> parse_count(std::string_view text) noexcept {
    std::size_t const split = std::min(text.find_first_not_of("0123456789"), text.size());
    std::string_view const units = "kmgt";
    std::size_t scale = 1;
    if (split != text.size()) {
        std::size_t const unit = text.size() == split + 1 ? units.find(text.back() | 0x20) : std::string_view::npos;
        if (unit == std::string_view::npos) return {};
        for (std::size_t level = 0; level <= unit; ++level) scale *= 1000;
    }
    std::optional<std::size_t> const count = parse_digits<std::size_t>(text.substr(0, split));
    if (!count || !*count) return {};
    return *count * scale;
}

/** A size like `4096`, `64KB` or `16GB`, in powers of 1024; zero is refused. */
inline std::optional<std::uint64_t> parse_size(std::string_view text) noexcept {
    std::size_t const split = std::min(text.find_first_not_of("0123456789"), text.size());
    std::string_view const unit = text.substr(split);
    std::string_view const units[] {"", "kb", "mb", "gb", "tb"};
    for (std::size_t level = 0; level != std::size(units); ++level) {
        bool const matches = unit.size() == units[level].size() &&
                             std::equal(unit.begin(), unit.end(), units[level].begin(),
                                        [](char given, char lower) noexcept { return (given | 0x20) == lower; });
        std::optional<std::uint64_t> const bytes = parse_digits<std::uint64_t>(text.substr(0, split));
        if (matches && bytes && *bytes) return *bytes << (10 * level);
    }
    return {};
}

/** A duration like `200ms` or `10s`; a bare number, a fraction or zero is refused. */
inline std::optional<std::chrono::milliseconds> parse_duration(std::string_view text) noexcept {
    if (text.ends_with("ms")) {
        std::optional<std::uint64_t> const count = parse_digits<std::uint64_t>(text.substr(0, text.size() - 2));
        if (count && *count) return std::chrono::milliseconds(*count);
        return {};
    }
    if (!text.ends_with('s')) return {};
    std::optional<std::uint64_t> const count = parse_digits<std::uint64_t>(text.substr(0, text.size() - 1));
    if (count && *count) return std::chrono::seconds(*count);
    return {};
}

/** A 32-bit unsigned integer, or `random` as 32 bits from the OS entropy source. */
inline std::optional<std::uint32_t> parse_seed(std::string_view text) noexcept {
    if (text == "random") return std::random_device {}();
    return parse_digits<std::uint32_t>(text);
}

/** A TCP port from 1 to 65535. */
inline std::optional<std::uint16_t> parse_port(std::string_view text) noexcept {
    std::optional<std::uint16_t> const port = parse_digits<std::uint16_t>(text);
    if (port && *port) return port;
    return {};
}

/** An IPv4 or IPv6 address in its numeric spelling. */
inline std::optional<std::string> parse_address(std::string_view text) noexcept {
    std::string const address {text};
    unsigned char parsed[16];
    if (inet_pton(AF_INET, address.c_str(), parsed) != 1 && inet_pton(AF_INET6, address.c_str(), parsed) != 1)
        return {};
    return address;
}

/** `host:port`, with the brackets an IPv6 host needs. */
inline std::string host_port(std::string_view host, std::uint16_t port) noexcept {
    if (host.find(':') != std::string_view::npos) return std::format("[{}]:{}", host, port);
    return std::format("{}:{}", host, port);
}

/** A local device and its addresses; an empty device routes by address alone. */
struct interface_t {
    std::string device;
    std::vector<std::string> addresses;
};

/** `DEVICE=ADDRESS[+ADDRESS...]` or `ADDRESS[+ADDRESS...]`. */
inline std::optional<interface_t> parse_interface(std::string_view text) noexcept {
    interface_t interface;
    std::size_t const equals = text.find('=');
    if (equals != std::string_view::npos) {
        if (equals == 0) return {};
        interface.device = text.substr(0, equals);
        text.remove_prefix(equals + 1);
    }
    for (std::size_t first = 0; first <= text.size();) {
        std::size_t const plus = std::min(text.find('+', first), text.size());
        std::optional<std::string> address = parse_address(text.substr(first, plus - first));
        if (!address) return {};
        interface.addresses.push_back(*address);
        first = plus + 1;
    }
    return interface;
}

/** The whole of a small file like those under `/proc` and `/sys`, or nothing when it cannot be read. */
inline std::optional<std::string> read_file(std::string const& path) noexcept {
    std::FILE* file = std::fopen(path.c_str(), "rb");
    if (!file) return {};
    std::string text;
    char chunk[4096];
    for (std::size_t read; (read = std::fread(chunk, 1, sizeof(chunk), file)) != 0;) text.append(chunk, read);
    std::fclose(file);
    return text;
}

/** The value after `prefix` on the first line of @p text starting with it, trimmed. */
inline std::optional<std::string_view> line_value(std::string_view text, std::string_view prefix) noexcept {
    for (std::size_t first = 0; first < text.size();) {
        std::size_t const end = std::min(text.find('\n', first), text.size());
        std::string_view const line = text.substr(first, end - first);
        if (line.starts_with(prefix)) {
            std::string_view value = line.substr(prefix.size());
            value.remove_prefix(std::min(value.find_first_not_of(" \t"), value.size()));
            return value.substr(0, value.find_last_not_of(" \t") + 1);
        }
        first = end + 1;
    }
    return {};
}

/** This process's affinity mask as a cpuset list like `0-15`, every core where the platform reports none. */
inline std::string allowed_cpus() noexcept {
    std::optional<std::string> const status = read_file("/proc/self/status");
    std::optional<std::string_view> const list = status ? line_value(*status, "Cpus_allowed_list:") : std::nullopt;
    if (list) return std::string {*list};
    return std::format("0-{}", std::max(std::thread::hardware_concurrency(), 1u) - 1);
}

/** Cores in a cpuset list like `0-3,8-11`. */
inline std::size_t count_cpus(std::string_view list) noexcept {
    std::size_t count = 0;
    for (std::size_t first = 0; first < list.size();) {
        std::size_t const comma = std::min(list.find(',', first), list.size());
        std::string_view const range = list.substr(first, comma - first);
        std::size_t const dash = std::min(range.find('-'), range.size());
        std::size_t const low = parse_digits<std::size_t>(range.substr(0, dash)).value_or(0);
        std::size_t const high = dash == range.size() ? low
                                                      : parse_digits<std::size_t>(range.substr(dash + 1)).value_or(low);
        count += high - low + 1;
        first = comma + 1;
    }
    return std::max<std::size_t>(count, 1);
}

/** TCP connections this box opened since boot, every process together, from `/proc/net/snmp`. */
inline std::optional<std::uint64_t> tcp_active_opens() noexcept {
    std::optional<std::string> const snmp = read_file("/proc/net/snmp");
    std::size_t const names = snmp ? snmp->find("Tcp:") : std::string::npos;
    std::size_t const values = names == std::string::npos ? names : snmp->find("Tcp:", names + 1);
    if (values == std::string::npos) return {};
    std::string_view const text {*snmp};
    for (std::size_t name = names, value = values; name < value && value < text.size() && text[value] != '\n';) {
        std::size_t const name_end = text.find_first_of(" \n", name + 1),
                          value_end = text.find_first_of(" \n", value + 1);
        if (text.substr(name + 1, name_end - name - 1) == "ActiveOpens")
            return parse_digits<std::uint64_t>(text.substr(value + 1, value_end - value - 1));
        name = name_end, value = value_end;
    }
    return {};
}

/** The box's TCP sockets in use and orphaned, from `/proc/net/sockstat`, to tell when closed ones were released. */
inline std::optional<std::string> sockets_in_use() noexcept {
    std::optional<std::string> const sockstat = read_file("/proc/net/sockstat");
    std::optional<std::string_view> const tcp = sockstat ? line_value(*sockstat, "TCP:") : std::nullopt;
    if (!tcp) return {};
    std::size_t const in_use = tcp->find("inuse "), orphan = tcp->find(" orphan ");
    if (in_use == std::string_view::npos || orphan == std::string_view::npos) return {};
    std::string_view const counts = tcp->substr(in_use, tcp->find(' ', orphan + 8) - in_use);
    return std::string {counts};
}

/** Every core in this process's affinity mask, at least one. */
inline std::size_t all_cores() noexcept { return count_cpus(allowed_cpus()); }

/** A thread count like `8`, or `0` as every core. */
inline std::optional<std::size_t> parse_threads(std::string_view text) noexcept {
    if (text == "0") return all_cores();
    return parse_count(text);
}

/** One of @p choices, returning its position. */
template <std::size_t count_>
std::optional<std::size_t> parse_choice(std::string_view text, std::string_view const (&choices)[count_]) noexcept {
    for (std::size_t index = 0; index != count_; ++index)
        if (choices[index] == text) return index;
    return {};
}

/** Part @p index of @p total split evenly over @p parts, the first parts taking the remainder. */
inline std::size_t share(std::size_t total, std::size_t index, std::size_t parts) noexcept {
    return total / parts + (index < total % parts);
}

inline std::string_view const security_names_k[] {"none", "noise-xx", "noise-ik", "tls"};

inline std::optional<std::size_t> parse_security(std::string_view text) noexcept {
    return parse_choice(text, security_names_k);
}

/** The Noise pattern `--security` names; UCall speaks Noise rather than TLS. */
inline ucall_security_t security_of(std::size_t choice) noexcept {
    ucall_security_t const patterns[] {ucall_security_none_k, ucall_security_noise_xx_k, ucall_security_noise_ik_k};
    if (choice == 3) exit_with("UCall secures connections with Noise; use --security none, noise-xx or noise-ik");
    return patterns[choice];
}

/** The key keys.sh wrote as 64 hexadecimal digits to `<data-dir>/noise/<name>`. */
inline void read_key(std::string_view data_dir, std::string_view name, ucall_noise_key_t& key) noexcept {
    std::string const path = std::format("{}/noise/{}", data_dir, name);
    char digits[2 * sizeof(ucall_noise_key_t) + 1] {};
    std::FILE* file = std::fopen(path.c_str(), "rb");
    std::size_t const read = file ? std::fread(digits, 1, sizeof(digits), file) : 0;
    if (file) std::fclose(file);
    bool parsed = read >= 2 * sizeof(ucall_noise_key_t);
    for (std::size_t index = 0; parsed && index != sizeof(ucall_noise_key_t); ++index) {
        char const* pair = digits + 2 * index;
        auto const [end, parse_status] = std::from_chars(pair, pair + 2, key[index], 16);
        parsed = parse_status == std::errc {} && end == pair + 2;
    }
    if (!parsed) exit_with(std::format("{} holds no Noise key; run keys.sh with UCall's ucall tool on PATH", path));
}

/** Admits a Noise peer only when its static key is the one @p context points to. */
inline ucall_status_t admit_known_peer(ucall_noise_key_t const* peer_key, void* context) noexcept {
    bool const known = std::equal(std::begin(*peer_key), std::end(*peer_key),
                                  std::begin(*static_cast<ucall_noise_key_t const*>(context)));
    return known ? ucall_status_success_k : ucall_status_peer_rejected_k;
}

#pragma endregion Flags

#pragma region Payloads

/** Bytes of every tool call body. */
inline constexpr std::size_t tool_bytes_k = 1024;
/** Bytes of every tool and fan-out reply. */
inline constexpr std::size_t reply_bytes_k = 256;
/** Distinct tool calls generated at startup and cycled through. */
inline constexpr std::size_t ring_k = 1024;
inline constexpr std::string_view tool_name_k = "search_documents";
inline constexpr std::string_view lang_k = "en";
/** The `authorization` header of every `full` call, which servers answer with 401 when it differs. */
inline constexpr std::string_view authorization_k = "Bearer 6f1c09a4e2b7d35c81f0aa4e92d1c7b3";

/** SplitMix64's finalizer. */
constexpr std::uint64_t split_mix_bits(std::uint64_t value) noexcept {
    value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ull;
    value = (value ^ (value >> 27)) * 0x94D049BB133111EBull;
    return value ^ (value >> 31);
}

/** The @p index -th stream of the family @p name under @p seed, as UCall's tools derive it. */
constexpr std::uint64_t stream_key(std::uint64_t seed, std::string_view name, std::uint64_t index) noexcept {
    std::uint64_t hashed = 0xCBF29CE484222325ull;
    for (char character : name) hashed = (hashed ^ static_cast<std::uint8_t>(character)) * 0x100000001B3ull;
    return split_mix_bits(split_mix_bits(seed ^ hashed) + index);
}

/** SplitMix64 over one counter, emitting each draw as 8 little-endian bytes. */
struct split_mix_t {
    std::uint64_t state = 0;

    std::uint64_t draw() noexcept { return split_mix_bits(state += 0x9E3779B97F4A7C15ull); }

    void fill(std::span<char> bytes) noexcept {
        for (std::size_t first = 0; first < bytes.size(); first += 8) {
            std::uint64_t const word = draw();
            for (std::size_t offset = 0; offset != 8 && first + offset != bytes.size(); ++offset)
                bytes[first + offset] = static_cast<char>(word >> (8 * offset));
        }
    }
};

/** A context upload of @p bytes seeded bytes. */
inline std::string context_body(std::uint64_t seed, std::uint64_t bytes) noexcept {
    std::string body(bytes, '\0');
    split_mix_t {stream_key(seed, "context", 0)}.fill(body);
    return body;
}

/** The seeded fields of one tool call, as `payloads.rs` draws them. */
struct tool_parts_t {
    std::string id;
    std::string query;
    std::uint32_t top_k = 0;
    std::uint64_t after = 0;
    std::string trace;

    tool_parts_t(std::uint64_t seed, std::size_t index) noexcept {
        std::string_view const query_alphabet = "abcdefghijklmnopqrstuvwxyz      ";
        std::string_view const hex_digits = "0123456789abcdef";
        std::size_t const query_bytes = 64;
        std::uint64_t const after_first = 1'767'225'600, after_span = 365 * 86'400;
        split_mix_t generator {stream_key(seed, "tool", index)};
        id = std::format("call_{:016x}", generator.draw());
        std::uint64_t const numbers = generator.draw();
        std::string bytes(tool_bytes_k, '\0');
        generator.fill(bytes);
        for (std::size_t position = 0; position != bytes.size(); ++position) {
            std::uint8_t const byte = static_cast<std::uint8_t>(bytes[position]);
            if (position < query_bytes) query += query_alphabet[byte & 31];
            else trace += hex_digits[byte & 15];
        }
        top_k = static_cast<std::uint32_t>(1 + numbers % 64);
        after = after_first + (numbers >> 32) % after_span;
    }

    /** What every reply echoes to prove the server read both typed values. */
    std::uint64_t check() const noexcept { return after + top_k; }
    std::string_view trace_id() const noexcept { return std::string_view {trace}.substr(trace.size() - 32); }
    std::string traceparent() const noexcept {
        return std::format("00-{}-{}-01", trace_id(), std::string_view {trace}.substr(trace.size() - 48, 16));
    }

    std::string padded(std::string head) const noexcept {
        head.append(trace, 0, tool_bytes_k - head.size() - 2);
        head += "\"}";
        return head;
    }

    /** The JSON body, exactly `tool_bytes_k` long. */
    std::string body() const noexcept {
        return padded(std::format(
            R"({{"id":"{}","type":"function","name":"{}","arguments":{{"query":"{}","top_k":{},"filters":{{"lang":"{}","after":{}}}}},"trace":")",
            id, tool_name_k, query, top_k, lang_k, after));
    }

    /** The `full` call's JSON body, its name, `lang` and `after` moved to the URL. */
    std::string full_body() const noexcept {
        return padded(std::format(R"({{"id":"{}","query":"{}","top_k":{},"trace":")", id, query, top_k));
    }

    std::string full_path() const noexcept {
        return std::format("/tool/{}?lang={}&after={}", tool_name_k, lang_k, after);
    }
};

/** A reply padded to `reply_bytes_k`, formatted without touching the heap. */
struct reply_t {
    char bytes[reply_bytes_k] {};
    std::size_t length = 0;

    std::string_view view() const noexcept { return {bytes, length}; }
};

template <typename... args_types_>
reply_t padded_reply(std::format_string<args_types_...> head, args_types_&&... args) noexcept {
    reply_t reply;
    char* end = std::format_to_n(reply.bytes, reply_bytes_k - 2, head, std::forward<args_types_>(args)...).out;
    end = std::fill_n(end, reply.bytes + reply_bytes_k - 2 - end, 'x');
    end = std::copy_n("\"}", 2, end);
    reply.length = static_cast<std::size_t>(end - reply.bytes);
    return reply;
}

/** The tool reply copying @p id and @p check. */
inline reply_t tool_reply(std::string_view id, std::uint64_t check) noexcept {
    return padded_reply(R"({{"id":"{}","ok":true,"check":{},"content":")", id, check);
}

/** The `full` reply echoing the body's id, the path's tool name, the trace id, `lang` and `check`. */
inline reply_t full_reply(std::string_view id, std::string_view name, std::string_view trace, std::string_view lang,
                          std::uint64_t check) noexcept {
    return padded_reply(R"({{"id":"{}","name":"{}","trace":"{}","lang":"{}","check":{},"content":")", id, name, trace,
                        lang, check);
}

/** The fan-out reply after @p width matching upstream replies. */
inline reply_t fanout_reply(std::string_view id, std::size_t width) noexcept {
    return padded_reply(R"({{"id":"{}","ok":true,"width":{},"content":")", id, width);
}

/** The context reply: the byte count as 16 lowercase hexadecimal digits. */
inline reply_t count_reply(std::uint64_t bytes) noexcept {
    reply_t reply;
    reply.length = static_cast<std::size_t>(std::format_to(reply.bytes, "{:016x}", bytes) - reply.bytes);
    return reply;
}

/** The trace id of a W3C `traceparent` header, or nothing when it does not parse. */
inline std::optional<std::string_view> trace_id(std::string_view traceparent) noexcept {
    if (traceparent.size() != 55) return {};
    for (std::size_t index = 0; index != traceparent.size(); ++index) {
        char const character = traceparent[index];
        bool const dash = index == 2 || index == 35 || index == 52;
        bool const hex = (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f');
        if (dash ? character != '-' : !hex) return {};
    }
    return traceparent.substr(3, 32);
}

#pragma endregion Payloads

} // namespace serveval
