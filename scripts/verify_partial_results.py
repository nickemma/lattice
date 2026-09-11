#!/usr/bin/env python3
"""Verify that a partial-results experiment actually exercised the availability path.

Shared by scripts/partial-results-local.sh and
scripts/partial-results-opensearch.sh so both apply the same rule.

The rule is a like-for-like comparison, per (query class, concurrency)
configuration, because a concurrency sweep will find the point where the
offered load alone saturates the system. At that point the baseline itself
returns deadline-driven incomplete responses, and asking whether a *fault*
caused incompleteness is unanswerable: everything is incomplete either way.

So:

  * Configurations where the baseline was clean are the ones the claim rests
    on. There, the fault run must show incompleteness, and for an availability
    fault it must show it with no errors at all.
  * Configurations where the baseline was already saturated are excluded and
    reported as excluded, not quietly dropped.
  * If every configuration was saturated, the experiment proved nothing and
    this exits non-zero.

Usage:
  verify_partial_results.py <directory> <baseline.json> <fault.json> [recovered.json]
"""

import json
import os
import sys


def load(directory, name):
    with open(os.path.join(directory, name)) as handle:
        return json.load(handle)


def by_configuration(report):
    """Collapse a report into {(query_class, concurrency): counters}."""
    result = {}
    for config in report["configurations"]:
        key = (config["query_class"], config["concurrency"])
        counters = {
            "completed": 0,
            "incomplete": 0,
            "degraded": 0,
            "errors": 0,
            "cache_hits": 0,
            "reasons": {},
        }
        for run in config["runs"]:
            counters["completed"] += run["completed"]
            counters["incomplete"] += run["incomplete_responses"]
            counters["degraded"] += run["degraded_responses"]
            counters["errors"] += run["api_error_responses"]
            counters["cache_hits"] += run.get("cache_hits", 0)
            for reason, count in (run.get("coverage_reasons") or {}).items():
                counters["reasons"][reason] = counters["reasons"].get(reason, 0) + count
        result[key] = counters
    return result


def describe(label, counters):
    reasons = ", ".join(f"{k}={v}" for k, v in sorted(counters["reasons"].items()))
    return (
        f"{label:<22} completed={counters['completed']:<6} "
        f"incomplete={counters['incomplete']:<6} degraded={counters['degraded']:<6} "
        f"errors={counters['errors']:<6} cache_hits={counters['cache_hits']:<6} [{reasons}]"
    )


def main():
    if len(sys.argv) < 4:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    directory = sys.argv[1]
    baseline = by_configuration(load(directory, sys.argv[2]))
    fault = by_configuration(load(directory, sys.argv[3]))
    recovered = by_configuration(load(directory, sys.argv[4])) if len(sys.argv) > 4 else None

    failures = []
    clean, saturated = [], []

    for key in sorted(baseline):
        if key not in fault:
            failures.append(f"{key} present in baseline but not in the fault run")
            continue
        if baseline[key]["incomplete"] == 0 and baseline[key]["errors"] == 0:
            clean.append(key)
        else:
            saturated.append(key)

    print("Per-configuration comparison\n")
    for key in sorted(baseline):
        marker = "saturated" if key in saturated else "usable"
        print(f"[{marker}] query_class={key[0]!r} concurrency={key[1]}")
        print("  " + describe("baseline", baseline[key]))
        print("  " + describe("fault", fault[key]))
        if recovered and key in recovered:
            print("  " + describe("recovered", recovered[key]))
        print()

    if not clean:
        failures.append(
            "every configuration saturated under its own offered load, so no "
            "configuration can attribute incompleteness to the injected fault; "
            "lower the concurrency sweep or raise the deadline"
        )

    # The claim rests only on configurations where the baseline was clean.
    for key in clean:
        counters = fault[key]
        if counters["incomplete"] == 0:
            failures.append(f"{key}: the fault produced no incomplete responses")
        if counters["errors"] != 0:
            failures.append(
                f"{key}: the fault produced {counters['errors']} error responses; "
                "an unavailable shard is not an error"
            )
        if counters["degraded"] != 0:
            failures.append(f"{key}: the fault produced {counters['degraded']} degraded responses")
        if counters["reasons"].get("shard_unavailable", 0) == 0:
            failures.append(f"{key}: reasons were {counters['reasons']}, expected shard_unavailable")
        if recovered is not None:
            after = recovered.get(key)
            if after and (after["incomplete"] or after["errors"]):
                failures.append(
                    f"{key}: still degraded after recovery "
                    f"(incomplete={after['incomplete']} errors={after['errors']})"
                )

    if saturated:
        print(
            f"Excluded {len(saturated)} configuration(s) whose baseline was already "
            "incomplete under offered load alone:"
        )
        for key in saturated:
            print(f"  - query_class={key[0]!r} concurrency={key[1]}")
        print(
            "  These measure cluster saturation, not fault behaviour. They belong in\n"
            "  the load-sweep table, not in the partial-results claim.\n"
        )

    if failures:
        print("FAILED:")
        for failure in failures:
            print("  - " + failure)
        return 1

    print(
        f"OK: across {len(clean)} configuration(s) with a clean baseline, the fault "
        "produced incomplete responses with zero errors."
    )
    print("Availability-driven incompleteness is separately observable from deadline expiry.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
