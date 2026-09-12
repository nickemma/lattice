#!/usr/bin/env python3
"""Verify that a partial-results experiment actually exercised the availability path.

Shared by scripts/partial-results-local.sh and
scripts/partial-results-opensearch.sh so both apply the same rule.

Two things make a naive check wrong, and both were observed in real runs:

1. A concurrency sweep finds the load at which the system saturates on its own.
   There the baseline itself returns deadline-driven incomplete responses, and
   asking whether a *fault* caused incompleteness is unanswerable. Those
   configurations are excluded and reported as excluded.

2. A fault plus enough offered load can exceed the deadline, so both failure
   classes fire at once and the run becomes a *compound* of the two. On the
   in-process backend this is amplified because removing a shard also removes
   cacheability — only a complete, non-degraded response is cacheable — so the
   fault run does real work on every query. That is correct behaviour, not a
   defect, and must not be scored as a failure.

What is actually checked, per configuration whose baseline was clean:

  * the fault registered at all (incomplete > 0);
  * it was classified as an availability loss (shard_unavailable > 0);
  * and the core invariant from the coverage split:

        api_error_responses == coverage_reasons['deadline']

    Every error is accounted for by a deadline expiry, which means no
    availability-classified response carried an error. If coverage and errors
    were still welded together, every incomplete response would carry an error
    and this equality would break immediately.

And once globally: at least one configuration must be *pure* — incompleteness
with no deadline and no error anywhere in it. That is the headline claim
demonstrated in isolation, and without it the experiment proved nothing.

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
        f"{label:<12} completed={counters['completed']:<6} "
        f"incomplete={counters['incomplete']:<6} degraded={counters['degraded']:<6} "
        f"errors={counters['errors']:<6} cached={counters['cache_hits']:<6} [{reasons}]"
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
    usable, saturated, pure, compound = [], [], [], []

    for key in sorted(baseline):
        if key not in fault:
            failures.append(f"{key} present in baseline but not in the fault run")
            continue
        if baseline[key]["incomplete"] == 0 and baseline[key]["errors"] == 0:
            usable.append(key)
        else:
            saturated.append(key)

    for key in usable:
        counters = fault[key]
        deadlines = counters["reasons"].get("deadline", 0)

        if counters["incomplete"] == 0:
            failures.append(f"{key}: the fault produced no incomplete responses")
        if counters["reasons"].get("shard_unavailable", 0) == 0:
            failures.append(
                f"{key}: no response was classified shard_unavailable; reasons were {counters['reasons']}"
            )
        # The invariant the coverage split exists to guarantee.
        if counters["errors"] != deadlines:
            failures.append(
                f"{key}: {counters['errors']} error responses but {deadlines} deadline "
                "expiries — an availability loss must not carry an error"
            )
        if counters["degraded"] != deadlines:
            failures.append(
                f"{key}: {counters['degraded']} degraded responses but {deadlines} deadline "
                "expiries — degradation must be accounted for by the deadline"
            )

        if deadlines == 0 and counters["errors"] == 0:
            pure.append(key)
        else:
            compound.append(key)

        if recovered is not None:
            after = recovered.get(key)
            if after and (after["incomplete"] or after["errors"]):
                failures.append(
                    f"{key}: still degraded after recovery "
                    f"(incomplete={after['incomplete']} errors={after['errors']})"
                )

    print("Per-configuration comparison\n")
    for key in sorted(baseline):
        if key in saturated:
            marker = "saturated"
        elif key in pure:
            marker = "pure"
        elif key in compound:
            marker = "compound"
        else:
            marker = "unscored"
        print(f"[{marker}] query_class={key[0]!r} concurrency={key[1]}")
        print("  " + describe("baseline", baseline[key]))
        if key in fault:
            print("  " + describe("fault", fault[key]))
        if recovered and key in recovered:
            print("  " + describe("recovered", recovered[key]))
        print()

    if not usable:
        failures.append(
            "every configuration saturated under its own offered load, so none can "
            "attribute incompleteness to the injected fault; lower the concurrency "
            "sweep, shrink the corpus, or raise the deadline"
        )
    elif not pure:
        failures.append(
            "no configuration isolated the availability path: every one that "
            "registered the fault also expired the deadline, so 'partial results "
            "with no error' was never demonstrated on its own. Lower the "
            "concurrency sweep or raise the deadline."
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

    if compound:
        print(
            f"{len(compound)} configuration(s) are compound: the fault registered and "
            "the deadline also expired."
        )
        for key in compound:
            print(f"  - query_class={key[0]!r} concurrency={key[1]}")
        print(
            "  Expected rather than wrong: a fault plus enough offered load can exceed\n"
            "  the budget, and then both classes fire at once. On the in-process backend\n"
            "  the usual cause is lost cacheability — only a complete, non-degraded\n"
            "  response is cacheable, so the fault run does real work on every query. On\n"
            "  OpenSearch it is normally load alone. Either way every error here is\n"
            "  accounted for by a deadline expiry, never by the missing shard.\n"
            "  Quote the pure configurations for the availability claim.\n"
        )

    if failures:
        print("FAILED:")
        for failure in failures:
            print("  - " + failure)
        return 1

    print(
        f"OK: {len(pure)} configuration(s) isolated the availability path — incomplete "
        "responses, zero errors, classified shard_unavailable."
    )
    if compound:
        print(
            f"    {len(compound)} compound configuration(s) held the invariant too: "
            "every error was a deadline expiry, never an availability loss."
        )
    print("Availability-driven incompleteness is separately observable from deadline expiry.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
