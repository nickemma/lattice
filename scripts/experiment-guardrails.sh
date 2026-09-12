#!/usr/bin/env bash
# Shared guardrails for the benchmark experiments.
#
# These scripts generate load deliberately, and the load generator is happy to
# use every core it can see. That is correct on a dedicated benchmark machine
# and destructive on a working laptop: an uncapped run alongside an editor takes
# the whole machine, and once the host starts swapping the editor stops
# responding entirely rather than merely slowing down.
#
# So the defaults here are conservative and the heavy sweep is opt-in. Set
# LATTICE_EXPERIMENT_HEAVY=1 when running on a machine you are not also using.
#
# Source this file; do not execute it.

# Default concurrency sweep. The 8/32/128 sweep referenced in the benchmark
# method is the heavy profile — 128 concurrent clients against the in-process
# backend, which rescores every document per query, will saturate any laptop.
if [[ "${LATTICE_EXPERIMENT_HEAVY:-0}" == "1" ]]; then
  LATTICE_DEFAULT_CONCURRENCY="8,32,128"
else
  LATTICE_DEFAULT_CONCURRENCY="4,8,16"
fi
export LATTICE_DEFAULT_CONCURRENCY

# Cap the Go processes this experiment starts. Without this the API and the load
# generator each expand to every core and compete with the desktop for CPU.
# Half the cores, minimum 2, unless the caller has already chosen.
if [[ -z "${GOMAXPROCS:-}" ]]; then
  lattice_cores="$(nproc 2>/dev/null || echo 4)"
  GOMAXPROCS=$(( lattice_cores / 2 ))
  (( GOMAXPROCS < 2 )) && GOMAXPROCS=2
  if [[ "${LATTICE_EXPERIMENT_HEAVY:-0}" == "1" ]]; then
    GOMAXPROCS="${lattice_cores}"
  fi
  export GOMAXPROCS
fi

# Run generated load at the lowest priority so the desktop always wins a
# contended core. Measured latencies still reflect real queueing; what changes
# is that the machine stays usable while they are taken.
if [[ "${LATTICE_EXPERIMENT_HEAVY:-0}" == "1" ]]; then
  LATTICE_NICE=()
else
  LATTICE_NICE=(nice -n 19)
fi

# require_memory refuses to start when the host cannot spare the RAM. Failing
# here with an explanation is better than failing later by freezing the machine.
require_memory() { # required MiB, what it is for
  local required="$1" purpose="$2" available
  available="$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)"
  if [[ "${available}" -eq 0 ]]; then
    echo "warning: could not read MemAvailable; skipping the memory pre-flight check" >&2
    return 0
  fi
  echo "==> memory pre-flight: ${available} MiB available, ${required} MiB needed for ${purpose}"
  if (( available < required )); then
    cat >&2 <<EOF

Not enough free memory: ${available} MiB available, ${required} MiB needed.

Running anyway would push this host into swap, which makes the whole machine
unresponsive rather than merely slow. Free some memory and retry, or set
LATTICE_EXPERIMENT_SKIP_MEMORY_CHECK=1 if you know better than this check.
EOF
    [[ "${LATTICE_EXPERIMENT_SKIP_MEMORY_CHECK:-0}" == "1" ]] || exit 1
  fi
}

announce_profile() {
  if [[ "${LATTICE_EXPERIMENT_HEAVY:-0}" == "1" ]]; then
    echo "==> profile: heavy (uncapped, for a dedicated machine)"
  else
    echo "==> profile: laptop-safe (nice -n 19, GOMAXPROCS=${GOMAXPROCS}, concurrency ${LATTICE_DEFAULT_CONCURRENCY})"
    echo "    set LATTICE_EXPERIMENT_HEAVY=1 on a dedicated machine for the full sweep"
  fi
}
