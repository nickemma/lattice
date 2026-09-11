package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPercentileInterpolates(t *testing.T) {
	values := []float64{10, 20, 30, 40}
	if got := percentile(values, 0.50); got != 25 {
		t.Fatalf("p50 = %v, want 25", got)
	}
	if got := percentile(values, 0.99); math.Abs(got-39.7) > 1e-9 {
		t.Fatalf("p99 = %v, want 39.7", got)
	}
}

func TestSummariseReportsDispersion(t *testing.T) {
	got := summarise([]float64{10, 12, 14, 16, 18})
	if got.Median != 14 || got.Q1 != 12 || got.Q3 != 16 || got.IQR != 4 {
		t.Fatalf("summary = %+v", got)
	}
	if got.Samples != 5 || got.Min != 10 || got.Max != 18 {
		t.Fatalf("summary = %+v", got)
	}
}

func TestParseConcurrencySweep(t *testing.T) {
	levels, err := parseConcurrency("8, 32,128")
	if err != nil {
		t.Fatal(err)
	}
	if len(levels) != 3 || levels[0] != 8 || levels[1] != 32 || levels[2] != 128 {
		t.Fatalf("levels = %v", levels)
	}
	if _, err := parseConcurrency("8,zero"); err == nil {
		t.Fatal("expected an error for a non-numeric level")
	}
	if _, err := parseConcurrency("0"); err == nil {
		t.Fatal("expected an error for a non-positive level")
	}
}

func TestSplitQuerySpecNamesQueryClasses(t *testing.T) {
	class, query := splitQuerySpec("rare=paxos")
	if class != "rare" || query != "paxos" {
		t.Fatalf("class=%q query=%q", class, query)
	}
	class, query = splitQuerySpec("distributed consensus")
	if class != "distributed consensus" || query != "distributed consensus" {
		t.Fatalf("class=%q query=%q", class, query)
	}
}

// TestMeasureSeparatesIncompleteFromErrors is the harness-side half of the
// coverage split. A response that lost a shard but reported no error must land
// in incomplete_responses only; the earlier tool could not have told them apart
// because it never read coverage.degraded or coverage.reason.
func TestMeasureSeparatesIncompleteFromErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],
			"coverage":{"shards_queried":3,"shards_answered":2,"complete":false,"degraded":false,"reason":"shard_unavailable"},
			"timings_ms":{"parse":0.1,"bm25":4,"vector":9,"fuse":0.2}}`))
	}))
	defer server.Close()

	api := &client{http: server.Client(), base: server.URL, deadline: 200 * time.Millisecond}
	got := api.measure("consensus", 4, 40, 1)

	if got.Completed != 40 || got.HTTPFailures != 0 {
		t.Fatalf("report = %+v", got)
	}
	if got.Incomplete != 40 {
		t.Fatalf("incomplete = %d, want 40", got.Incomplete)
	}
	if got.APIErrors != 0 || got.Degraded != 0 {
		t.Fatalf("a shard that did not answer is not an error: %+v", got)
	}
	if got.CoverageReasons["shard_unavailable"] != 40 {
		t.Fatalf("coverage reasons = %v", got.CoverageReasons)
	}
	// Branch timings are captured per response so the segment-count experiment
	// can attribute a tail to BM25 or to the kNN path.
	if got.BM25P99MS != 4 || got.VectorP99MS != 9 {
		t.Fatalf("branch timings = bm25 %v vector %v", got.BM25P99MS, got.VectorP99MS)
	}
}

func TestMeasureCountsDegradedButCompleteResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],
			"coverage":{"shards_queried":3,"shards_answered":3,"complete":true,"degraded":true,"reason":"dependency_error"},
			"errors":["embedding: 500"],
			"timings_ms":{"parse":0.1,"bm25":4,"vector":0,"fuse":0.2}}`))
	}))
	defer server.Close()

	api := &client{http: server.Client(), base: server.URL, deadline: 200 * time.Millisecond}
	got := api.measure("consensus", 2, 10, 1)
	if got.Incomplete != 0 {
		t.Fatalf("full shard coverage is not incomplete: %+v", got)
	}
	if got.Degraded != 10 || got.APIErrors != 10 {
		t.Fatalf("report = %+v", got)
	}
	if got.CoverageReasons["dependency_error"] != 10 {
		t.Fatalf("coverage reasons = %v", got.CoverageReasons)
	}
}

func TestSegmentCountSumsPrimaryShards(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"indices":{"lattice-documents":{"shards":{
			"0":[{"routing":{"primary":true},"num_search_segments":12},{"routing":{"primary":false},"num_search_segments":30}],
			"1":[{"routing":{"primary":true},"num_search_segments":7}]}}}}`))
	}))
	defer server.Close()
	if got := segmentCount(server.URL, "lattice-documents"); got != 19 {
		t.Fatalf("segments = %d, want 19 (primaries only)", got)
	}
	if got := segmentCount("", "lattice-documents"); got != 0 {
		t.Fatalf("segments = %d without a URL, want 0", got)
	}
}

func TestEnvironmentRecordsWhatItCouldNotCollect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ready","dependencies":{"opensearch":{"mode":"configured-backend","documents":1000008}}}`))
	}))
	defer server.Close()

	api := &client{http: server.Client(), base: server.URL}
	env := collectEnvironment(api, environmentSources{index: "lattice-documents", commit: "abc1234"})
	if env.Documents != 1000008 || env.LatticeMode != "configured-backend" || env.Commit != "abc1234" {
		t.Fatalf("environment = %+v", env)
	}
	if env.Unavailable["opensearch"] == "" {
		t.Fatalf("a missing OpenSearch URL must be recorded, not silently omitted: %+v", env)
	}
	if _, err := json.Marshal(env); err != nil {
		t.Fatal(err)
	}
}
