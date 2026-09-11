package search

import (
	"context"
	"testing"
	"time"
)

// TestCoverageSeparatesCompletenessFromErrors is the regression test for the
// defect that made complete:false and "an error occurred" the same signal. Both
// halves of the matrix must be representable; before the split, neither was.
func TestCoverageSeparatesCompletenessFromErrors(t *testing.T) {
	cases := []struct {
		name     string
		signals  CoverageSignals
		complete bool
		degraded bool
		reason   string
	}{
		{
			name:     "every shard answered and nothing failed",
			signals:  CoverageSignals{ShardsQueried: 3, ShardsAnswered: 3, ShardsObserved: true},
			complete: true, degraded: false, reason: ReasonComplete,
		},
		{
			name:     "shard missing with no error is not degraded",
			signals:  CoverageSignals{ShardsQueried: 3, ShardsAnswered: 2, ShardsObserved: true},
			complete: false, degraded: false, reason: ReasonShardUnavailable,
		},
		{
			name:     "full coverage with a failed dependency is degraded but complete",
			signals:  CoverageSignals{ShardsQueried: 3, ShardsAnswered: 3, ShardsObserved: true, DependencyFailed: true},
			complete: true, degraded: true, reason: ReasonDependencyError,
		},
		{
			name:     "deadline outranks availability when both could explain the gap",
			signals:  CoverageSignals{ShardsQueried: 3, ShardsAnswered: 1, ShardsObserved: true, DeadlineExceeded: true},
			complete: false, degraded: true, reason: ReasonDeadline,
		},
		{
			name:     "no shard accounting at all is a dependency failure, not a missing shard",
			signals:  CoverageSignals{ShardsQueried: 1, ShardsAnswered: 0, DependencyFailed: true},
			complete: false, degraded: true, reason: ReasonDependencyError,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			coverage := EvaluateCoverage(testCase.signals)
			if coverage.Complete != testCase.complete || coverage.Degraded != testCase.degraded || coverage.Reason != testCase.reason {
				t.Fatalf("coverage = %+v, want complete=%v degraded=%v reason=%s",
					coverage, testCase.complete, testCase.degraded, testCase.reason)
			}
			if coverage.Usable() != (testCase.complete && !testCase.degraded) {
				t.Fatalf("Usable() = %v for %+v", coverage.Usable(), coverage)
			}
		})
	}
}

// TestUnavailableShardIsIncompleteWithoutErrors is the availability half of the
// P2 experiment reduced to a unit test: an incomplete response that carries no
// error at all. A benchmark run showing incomplete_responses > 0 with
// api_error_responses == 0 is this same event, measured at scale.
func TestUnavailableShardIsIncompleteWithoutErrors(t *testing.T) {
	index := NewIndex(3)
	for _, id := range []string{"one", "two", "three", "four", "five", "six"} {
		if err := index.Upsert(Document{ID: id, Title: "consensus", Body: "consensus"}); err != nil {
			t.Fatal(err)
		}
	}
	index.SetShardAvailable(1, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response := index.Search(ctx, "consensus", 0, 10)
	if response.Coverage.Complete {
		t.Fatalf("expected incomplete coverage: %+v", response.Coverage)
	}
	if len(response.Errors) != 0 || response.Coverage.Degraded {
		t.Fatalf("an unavailable shard is not an error: errors=%v coverage=%+v", response.Errors, response.Coverage)
	}
	if response.Coverage.Reason != ReasonShardUnavailable {
		t.Fatalf("reason = %q, want %q", response.Coverage.Reason, ReasonShardUnavailable)
	}
	if len(response.Results) == 0 {
		t.Fatal("expected partial results from the shards that stayed up")
	}
}

// TestDeadlineIsDistinguishableFromUnavailability holds the other half apart:
// the same complete:false, reached by a different route, must say so.
func TestDeadlineIsDistinguishableFromUnavailability(t *testing.T) {
	index := NewIndex(2)
	if err := index.Upsert(Document{ID: "one", Title: "consensus", Body: "consensus"}); err != nil {
		t.Fatal(err)
	}
	index.SetShardDelay(1, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	response := index.SearchWithFilters(ctx, "consensus", nil, 0, 10)
	if response.Coverage.Complete || response.Coverage.Reason != ReasonDeadline {
		t.Fatalf("coverage = %+v, want incomplete with reason %q", response.Coverage, ReasonDeadline)
	}
	if !response.Coverage.Degraded || len(response.Errors) == 0 {
		t.Fatalf("a lost deadline is an error: errors=%v coverage=%+v", response.Errors, response.Coverage)
	}
}
