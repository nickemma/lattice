package search

// Coverage is the correctness contract of a search response, and this file is
// the only place its meaning is defined. Both the in-process Index and the
// OpenSearch backend must build it through EvaluateCoverage so the two cannot
// drift into disagreeing about what "complete" means.
//
// The central rule: completeness and errors are separate facts.
//
//   - Complete answers exactly one question: did every queried shard answer?
//   - Degraded answers a different one: did anything go wrong while answering?
//
// A response can be complete and degraded (all shards answered, but the
// embedding service failed, so only the keyword branch contributed). It can be
// incomplete and not degraded (a shard is out of rotation; nothing errored).
// Collapsing the two makes it impossible to tell a slow shard from a broken
// dependency, which are different pages for whoever is on call.

// Coverage reason values. They are a closed set so a client can switch on the
// reason instead of pattern-matching error strings.
const (
	// ReasonComplete: every queried shard answered and nothing failed.
	ReasonComplete = "complete"
	// ReasonDeadline: the search budget expired before all work returned.
	// Coverage may still be complete if the lost work was a whole branch
	// rather than a shard.
	ReasonDeadline = "deadline"
	// ReasonShardUnavailable: a queried shard did not answer and no deadline
	// fired. The shard is out of rotation, not merely slow.
	ReasonShardUnavailable = "shard_unavailable"
	// ReasonDependencyError: a dependency the query needs (embedding service,
	// index client) failed. Shard coverage may still be complete.
	ReasonDependencyError = "dependency_error"
	// ReasonInvalidRequest: the request was rejected before any shard was
	// queried, so there is no coverage to report. The HTTP layer normally
	// catches these and returns 400; a backend that reaches this state is
	// defending its own contract.
	ReasonInvalidRequest = "invalid_request"
)

// RejectedCoverage describes a query that never reached a shard. It is not
// degraded — nothing in the cluster failed — and it is not complete, because
// no shard was asked.
func RejectedCoverage() Coverage { return Coverage{Reason: ReasonInvalidRequest} }

// Coverage travels in every search response.
type Coverage struct {
	ShardsQueried  int  `json:"shards_queried"`
	ShardsAnswered int  `json:"shards_answered"`
	Complete       bool `json:"complete"`
	// Degraded is true when the response carries at least one error. It is
	// independent of Complete.
	Degraded bool `json:"degraded"`
	// Reason is one of the Reason* constants above.
	Reason string `json:"reason"`
}

// CoverageSignals are the raw observations a backend collects while running a
// query. Backends report what they saw; EvaluateCoverage decides what it means.
type CoverageSignals struct {
	// ShardsQueried is how many shards the query was supposed to reach.
	ShardsQueried int
	// ShardsAnswered is how many of them returned results.
	ShardsAnswered int
	// ShardsObserved is true when at least one real shard-level accounting was
	// returned. When it is false the shard numbers are a fallback, so a missing
	// shard cannot be blamed on availability.
	ShardsObserved bool
	// DeadlineExceeded is true when the search budget expired before all
	// outstanding work returned.
	DeadlineExceeded bool
	// DependencyFailed is true when a dependency the query needs returned an
	// error, regardless of shard coverage.
	DependencyFailed bool
}

// EvaluateCoverage is the single definition of search completeness.
//
// Reason precedence, most specific first: a lost deadline explains an
// incomplete answer better than anything else; an unobserved shard count means
// the failure was the dependency, not the shard; otherwise missing shards mean
// a shard is unavailable. With full coverage, a deadline or dependency error is
// still reported so a degraded-but-complete answer is visible.
func EvaluateCoverage(signals CoverageSignals) Coverage {
	queried := signals.ShardsQueried
	if queried < 0 {
		queried = 0
	}
	answered := signals.ShardsAnswered
	if answered < 0 {
		answered = 0
	}
	if answered > queried {
		answered = queried
	}
	coverage := Coverage{
		ShardsQueried:  queried,
		ShardsAnswered: answered,
		Complete:       queried > 0 && answered == queried,
		Degraded:       signals.DeadlineExceeded || signals.DependencyFailed,
	}
	switch {
	case !coverage.Complete && signals.DeadlineExceeded:
		coverage.Reason = ReasonDeadline
	case !coverage.Complete && signals.DependencyFailed && !signals.ShardsObserved:
		coverage.Reason = ReasonDependencyError
	case !coverage.Complete:
		coverage.Reason = ReasonShardUnavailable
	case signals.DeadlineExceeded:
		coverage.Reason = ReasonDeadline
	case signals.DependencyFailed:
		coverage.Reason = ReasonDependencyError
	default:
		coverage.Reason = ReasonComplete
	}
	return coverage
}

// Usable reports whether a response is safe to cache and to continue with a
// cursor: full shard coverage and nothing degraded. Both backends use this so
// the caching rule cannot drift either.
func (c Coverage) Usable() bool { return c.Complete && !c.Degraded }
