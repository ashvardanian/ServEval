#!/usr/bin/env bash
#
# Samples, once per second until interrupted, the counters that can throttle or explain a ServEval row: `ethtool -S`
# per card, `nstat -az`, the recorded cards' lines of /proc/interrupts, /proc/net/sockstat and the per-core lines of
# /proc/stat, in clock ticks of `getconf CLK_TCK` named once at the file's top, each sample headed by
# `@<Unix milliseconds>`, beside `mpstat -P ALL 1` and `sar -n DEV,EDEV 1`, each headed by the stamp it started at.
# Every file is appended to, so a restart into the same directory keeps the earlier samples; give each run its own
# directory, like `records-client/2026-11-25T09:30:00Z/`, and pass the parent to `scripts/plot.py --records`, which
# turns the samples around each row's window into network interface rates and allowance counts.
#
#   scripts/record.sh DIRECTORY [DEVICE...]
#
# Devices default to every interface the `ena` driver runs.

set -euo pipefail

directory=${1:?pass a directory for the samples, then optionally the devices}
shift
mkdir -p "$directory"
devices=("$@")
if [ ${#devices[@]} -eq 0 ]; then
    for driver in /sys/class/net/*/device/driver; do
        [ "$(basename "$(readlink -f "$driver")")" = ena ] && devices+=("$(basename "$(dirname "$(dirname "$driver")")")")
    done
fi
[ ${#devices[@]} -gt 0 ] || { echo "record.sh: no ena devices found; name them after the directory" >&2; exit 1; }
device_patterns=()
for device in "${devices[@]}"; do device_patterns+=(-e "$device"); done

# ISO times carry no date, so the stamp before each tool's first line anchors its clock.
export S_TIME_FORMAT=ISO
{ echo "@$(date +%s%3N)"; exec mpstat -P ALL 1; } >> "$directory/mpstat.log" &
{ echo "@$(date +%s%3N)"; exec sar -n DEV,EDEV 1; } >> "$directory/sar.log" &
# Bash skips the EXIT trap when a signal ends it, which once left mpstat and sar running after every recorder.
trap 'exit' INT TERM HUP
trap 'kill $(jobs -p) 2> /dev/null' EXIT
# The per-core columns of /proc/interrupts stay fixed while the machine runs, so their header is written once.
{ echo "@$(date +%s%3N)"; head -n 1 /proc/interrupts; } >> "$directory/interrupts.log"
echo "clock_ticks_per_second $(getconf CLK_TCK)" >> "$directory/cpu.log"

while true; do
    stamp="@$(date +%s%3N)"
    for device in "${devices[@]}"; do
        { echo "$stamp"; ethtool -S "$device"; } >> "$directory/ethtool-$device.log"
    done
    { echo "$stamp"; nstat -az; } >> "$directory/nstat.log"
    # Only the recorded cards' queue lines: the whole file reached 1.6 GB in one run.
    { echo "$stamp"; grep -F "${device_patterns[@]}" /proc/interrupts || true; } >> "$directory/interrupts.log"
    { echo "$stamp"; cat /proc/net/sockstat; } >> "$directory/sockstat.log"
    { echo "$stamp"; grep '^cpu[0-9]' /proc/stat; } >> "$directory/cpu.log"
    sleep 1
done
