#!/usr/bin/env bash
#
# Samples, once per second until interrupted, the counters that can throttle or explain a ServEval row: `ethtool -S`
# per card, `nstat -az`, /proc/interrupts and /proc/net/sockstat, each sample headed by `@<Unix milliseconds>`, beside
# `mpstat -P ALL 1` and `sar -n DEV,EDEV 1`. Run it on both boxes for the whole run, then pass each box's directory to
# `scripts/plot.py --records`, which voids every row whose window saw an ENA allowance counter rise.
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

S_TIME_FORMAT=ISO mpstat -P ALL 1 > "$directory/mpstat.log" &
S_TIME_FORMAT=ISO sar -n DEV,EDEV 1 > "$directory/sar.log" &
trap 'kill $(jobs -p) 2> /dev/null' EXIT

while true; do
    stamp="@$(date +%s%3N)"
    for device in "${devices[@]}"; do
        { echo "$stamp"; ethtool -S "$device"; } >> "$directory/ethtool-$device.log"
    done
    { echo "$stamp"; nstat -az; } >> "$directory/nstat.log"
    { echo "$stamp"; cat /proc/interrupts; } >> "$directory/interrupts.log"
    { echo "$stamp"; cat /proc/net/sockstat; } >> "$directory/sockstat.log"
    sleep 1
done
