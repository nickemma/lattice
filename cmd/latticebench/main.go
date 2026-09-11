// Command latticebench measures query latency against a running LATTICE API.
// It intentionally does not seed data: the corpus and embedding model must be
// declared by the operator before a capacity result is considered evidence.
//
// A single 6-second run at one concurrency with one query string is one sample
// of a noisy quantity, not a measurement. This tool therefore sweeps query
// classes and concurrency levels, repeats each configuration, and reports a
// median and interquartile range rather than a bare number. It also records the
// environment that produced the numbers, because a p99 without the segment
// count and shard layout beside it cannot be compared against anything.
//
// Coverage accounting is reported in three separate columns — incomplete,
// degraded, and a per-reason breakdown — so a run can distinguish "a shard
// stopped answering" from "the embedding service failed". Those used to be the
// same number.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// stat summarises repeated runs of one configuration. Median and IQR are used
// rather than mean and standard deviation because latency percentiles across a
// handful of runs are not normally distributed and one bad run should not move
// the headline.
type stat struct {
	Median  float64 `json:"median"`
	Q1      float64 `json:"q1"`
	Q3      float64 `json:"q3"`
	IQR     float64 `json:"iqr"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	Samples int     `json:"samples"`
}

// runReport is one measured run. Field names match the earlier single-run
// report format so historical files stay comparable.
type runReport struct {
	Run          int    `json:"run"`
	StartedAt    string `json:"started_at"`
	Queries      int    `json:"queries"`
	Completed    int    `json:"completed"`
	HTTPFailures int    `json:"http_failures"`
	Incomplete   int    `json:"incomplete_responses"`
	Degraded     int    `json:"degraded_responses"`
	APIErrors    int    `json:"api_error_responses"`
	// CacheHits matters for interpreting the other numbers. Only a complete,
	// non-degraded response is cacheable, so a degraded run is also an uncached
	// run: some of its latency is the lost cache, not the lost shard. Recording
	// it keeps that confound in the artifact instead of in a footnote.
	CacheHits       int            `json:"cache_hits"`
	CoverageReasons map[string]int `json:"coverage_reasons"`
	P50MS           float64        `json:"p50_ms"`
	P95MS           float64        `json:"p95_ms"`
	P99MS           float64        `json:"p99_ms"`
	BM25P50MS       float64        `json:"bm25_p50_ms"`
	BM25P99MS       float64        `json:"bm25_p99_ms"`
	VectorP50MS     float64        `json:"vector_p50_ms"`
	VectorP99MS     float64        `json:"vector_p99_ms"`
	Seconds         float64        `json:"duration_seconds"`
	QueriesPerSec   float64        `json:"queries_per_second"`
	Segments        int            `json:"segments,omitempty"`
}

type configReport struct {
	QueryClass    string          `json:"query_class"`
	Query         string          `json:"query"`
	Concurrency   int             `json:"concurrency"`
	QueriesPerRun int             `json:"queries_per_run"`
	DeadlineMS    float64         `json:"deadline_ms"`
	WarmupSeconds float64         `json:"warmup_seconds"`
	Runs          []runReport     `json:"runs"`
	Aggregate     map[string]stat `json:"aggregate"`
}

type environment struct {
	Note   string `json:"note,omitempty"`
	Commit string `json:"commit,omitempty"`
	// Backend and FaultSource exist so no table can be published without saying
	// which backend produced the number and whether the fault was injected
	// in-process or induced in a real cluster. Results from the in-memory index
	// and from OpenSearch are not interchangeable.
	Backend           string            `json:"backend"`
	FaultSource       string            `json:"fault_source"`
	Scenario          string            `json:"scenario,omitempty"`
	LatticeMode       string            `json:"lattice_mode,omitempty"`
	Documents         int               `json:"documents,omitempty"`
	OpenSearchVersion string            `json:"opensearch_version,omitempty"`
	ClusterName       string            `json:"opensearch_cluster,omitempty"`
	Nodes             int               `json:"opensearch_nodes,omitempty"`
	NodeHeap          []string          `json:"opensearch_node_heap,omitempty"`
	Index             string            `json:"opensearch_index,omitempty"`
	Shards            string            `json:"index_shards,omitempty"`
	Replicas          string            `json:"index_replicas,omitempty"`
	SegmentsAtStart   int               `json:"segments_at_start,omitempty"`
	Unavailable       map[string]string `json:"unavailable,omitempty"`
}

type report struct {
	Schema      string         `json:"schema"`
	StartedAt   string         `json:"started_at"`
	URL         string         `json:"url"`
	Environment environment    `json:"environment"`
	Configs     []configReport `json:"configurations"`
}

// stringList collects a flag that may be repeated.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

type client struct {
	http     *http.Client
	base     string
	apiKey   string
	deadline time.Duration
}

func main() {
	defaultURL := os.Getenv("LATTICE_E2E_URL")
	if defaultURL == "" {
		defaultURL = "http://localhost:8080"
	}
	var queryFlags stringList
	endpoint := flag.String("url", defaultURL, "LATTICE base URL")
	flag.Var(&queryFlags, "q", "search query, repeatable; use label=query to name the class (e.g. rare=paxos)")
	queries := flag.Int("queries", 1000, "measured queries per run")
	concurrencyList := flag.String("concurrency", "16", "comma-separated concurrency levels to sweep")
	runs := flag.Int("runs", 5, "repeats per configuration; a single run is one sample, not a measurement")
	warmup := flag.Duration("warmup", 5*time.Second, "warm-up period per configuration, discarded from results")
	deadline := flag.Duration("deadline", 200*time.Millisecond, "API deadline sent with each query")
	apiKey := flag.String("api-key", os.Getenv("LATTICE_E2E_API_KEY"), "X-API-Key value")
	openSearchURL := flag.String("opensearch-url", os.Getenv("LATTICE_BENCH_OPENSEARCH_URL"), "optional OpenSearch URL, used only to record the environment")
	openSearchIndex := flag.String("opensearch-index", envOr("LATTICE_BENCH_OPENSEARCH_INDEX", "lattice-documents"), "index whose shard, replica, and segment counts are recorded")
	commit := flag.String("commit", os.Getenv("LATTICE_BENCH_COMMIT"), "commit the measured build was made from")
	backend := flag.String("backend", envOr("LATTICE_BENCH_BACKEND", "unstated"), "backend that produced the numbers: in-memory or opensearch")
	faultSource := flag.String("fault-source", envOr("LATTICE_BENCH_FAULT_SOURCE", "none"), "how any fault was produced: none, injected-in-process, or induced-in-cluster")
	scenario := flag.String("scenario", "", "short name for what this run measured, e.g. shard-unavailable")
	note := flag.String("note", "", "free-text description of what this run measured")
	output := flag.String("output", "", "optional JSON report path")
	flag.Parse()

	if len(queryFlags) == 0 {
		queryFlags = stringList{"distributed consensus"}
	}
	concurrencies, err := parseConcurrency(*concurrencyList)
	if err != nil {
		fatal(err.Error())
	}
	if *queries < 1 || *runs < 1 || *deadline <= 0 {
		fatal("queries, runs, and deadline must be positive")
	}

	api := &client{
		http:     &http.Client{Timeout: 30 * time.Second},
		base:     strings.TrimRight(*endpoint, "/"),
		apiKey:   *apiKey,
		deadline: *deadline,
	}
	result := report{
		Schema:    "lattice-bench/2",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		URL:       api.base,
		Environment: collectEnvironment(api, environmentSources{
			openSearchURL: strings.TrimRight(*openSearchURL, "/"),
			index:         *openSearchIndex,
			commit:        *commit,
			note:          *note,
			backend:       *backend,
			faultSource:   *faultSource,
			scenario:      *scenario,
		}),
	}

	totalCompleted := 0
	for _, spec := range queryFlags {
		class, query := splitQuerySpec(spec)
		for _, concurrency := range concurrencies {
			config := configReport{
				QueryClass:    class,
				Query:         query,
				Concurrency:   concurrency,
				QueriesPerRun: *queries,
				DeadlineMS:    float64(deadline.Microseconds()) / 1000,
				WarmupSeconds: warmup.Seconds(),
			}
			if *warmup > 0 {
				api.warm(query, concurrency, *warmup)
			}
			for run := 1; run <= *runs; run++ {
				measured := api.measure(query, concurrency, *queries, run)
				measured.Segments = segmentCount(strings.TrimRight(*openSearchURL, "/"), *openSearchIndex)
				config.Runs = append(config.Runs, measured)
				totalCompleted += measured.Completed
			}
			config.Aggregate = aggregate(config.Runs)
			result.Configs = append(result.Configs, config)
			fmt.Fprintf(os.Stderr, "%s q=%q concurrency=%d runs=%d p99_median=%.2fms iqr=%.2fms\n",
				result.StartedAt, query, concurrency, len(config.Runs),
				config.Aggregate["p99_ms"].Median, config.Aggregate["p99_ms"].IQR)
		}
	}

	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	fmt.Println(string(encoded))
	if *output != "" {
		if err := os.WriteFile(*output, append(encoded, '\n'), 0o644); err != nil {
			fatal(err.Error())
		}
	}
	if totalCompleted == 0 {
		os.Exit(1)
	}
}

// searchPayload is the part of the response the benchmark reads. Coverage
// carries both completeness and degradation; reading only one of them is what
// made the earlier reports unable to separate the two.
type searchPayload struct {
	Coverage struct {
		Complete bool   `json:"complete"`
		Degraded bool   `json:"degraded"`
		Reason   string `json:"reason"`
	} `json:"coverage"`
	CacheHit bool `json:"cache_hit"`
	Timings  struct {
		BM25   float64 `json:"bm25"`
		Vector float64 `json:"vector"`
	} `json:"timings_ms"`
	Errors []string `json:"errors"`
}

func (c *client) searchURL(query string) string {
	return c.base + "/v1/search?q=" + url.QueryEscape(query) + "&deadline=" + url.QueryEscape(c.deadline.String())
}

// query issues one search and returns the wall-clock latency and the decoded
// body. A nil payload means the request did not produce a usable response.
func (c *client) query(query string) (float64, *searchPayload) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, c.searchURL(query), nil)
	if err != nil {
		return 0, nil
	}
	if c.apiKey != "" {
		request.Header.Set("X-API-Key", c.apiKey)
	}
	started := time.Now()
	response, requestErr := c.http.Do(request)
	elapsed := time.Since(started).Seconds() * 1000
	if requestErr != nil {
		return 0, nil
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	if response.StatusCode >= 300 || readErr != nil {
		return 0, nil
	}
	var payload searchPayload
	if json.Unmarshal(body, &payload) != nil {
		return elapsed, &searchPayload{}
	}
	return elapsed, &payload
}

// warm drives the configuration for a fixed period before measurement so caches,
// JIT, and file-system page cache are in a steady state. Nothing here is
// recorded.
func (c *client) warm(query string, concurrency int, duration time.Duration) {
	deadline := time.Now().Add(duration)
	var wait sync.WaitGroup
	wait.Add(concurrency)
	for n := 0; n < concurrency; n++ {
		go func() {
			defer wait.Done()
			for time.Now().Before(deadline) {
				c.query(query)
			}
		}()
	}
	wait.Wait()
}

func (c *client) measure(query string, concurrency, queries, run int) runReport {
	workers := concurrency
	if workers > queries {
		workers = queries
	}
	started := time.Now().UTC()
	jobs := make(chan int)
	var wait sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]float64, 0, queries)
	bm25 := make([]float64, 0, queries)
	vector := make([]float64, 0, queries)
	result := runReport{Run: run, Queries: queries, StartedAt: started.Format(time.RFC3339), CoverageReasons: map[string]int{}}

	wait.Add(workers)
	for n := 0; n < workers; n++ {
		go func() {
			defer wait.Done()
			for range jobs {
				elapsed, payload := c.query(query)
				mu.Lock()
				if payload == nil {
					result.HTTPFailures++
					mu.Unlock()
					continue
				}
				result.Completed++
				latencies = append(latencies, elapsed)
				bm25 = append(bm25, payload.Timings.BM25)
				vector = append(vector, payload.Timings.Vector)
				if !payload.Coverage.Complete {
					result.Incomplete++
				}
				if payload.Coverage.Degraded {
					result.Degraded++
				}
				if len(payload.Errors) > 0 {
					result.APIErrors++
				}
				if payload.CacheHit {
					result.CacheHits++
				}
				if payload.Coverage.Reason != "" {
					result.CoverageReasons[payload.Coverage.Reason]++
				}
				mu.Unlock()
			}
		}()
	}
	for n := 0; n < queries; n++ {
		jobs <- n
	}
	close(jobs)
	wait.Wait()

	elapsed := time.Since(started).Seconds()
	sort.Float64s(latencies)
	sort.Float64s(bm25)
	sort.Float64s(vector)
	if len(latencies) > 0 {
		result.P50MS = percentile(latencies, 0.50)
		result.P95MS = percentile(latencies, 0.95)
		result.P99MS = percentile(latencies, 0.99)
		// Branch timings are reported separately so a tail that belongs to the
		// kNN path is not attributed to the query as a whole.
		result.BM25P50MS = percentile(bm25, 0.50)
		result.BM25P99MS = percentile(bm25, 0.99)
		result.VectorP50MS = percentile(vector, 0.50)
		result.VectorP99MS = percentile(vector, 0.99)
	}
	result.Seconds = elapsed
	if elapsed > 0 {
		result.QueriesPerSec = float64(result.Completed) / elapsed
	}
	return result
}

// aggregate turns repeated runs into a median and an interquartile range for
// every headline field. A configuration measured once reports Samples: 1, which
// is the honest way to say "this number has no dispersion behind it".
func aggregate(runs []runReport) map[string]stat {
	fields := map[string]func(runReport) float64{
		"p50_ms":               func(r runReport) float64 { return r.P50MS },
		"p95_ms":               func(r runReport) float64 { return r.P95MS },
		"p99_ms":               func(r runReport) float64 { return r.P99MS },
		"bm25_p50_ms":          func(r runReport) float64 { return r.BM25P50MS },
		"bm25_p99_ms":          func(r runReport) float64 { return r.BM25P99MS },
		"vector_p50_ms":        func(r runReport) float64 { return r.VectorP50MS },
		"vector_p99_ms":        func(r runReport) float64 { return r.VectorP99MS },
		"queries_per_second":   func(r runReport) float64 { return r.QueriesPerSec },
		"incomplete_responses": func(r runReport) float64 { return float64(r.Incomplete) },
		"degraded_responses":   func(r runReport) float64 { return float64(r.Degraded) },
		"api_error_responses":  func(r runReport) float64 { return float64(r.APIErrors) },
		"http_failures":        func(r runReport) float64 { return float64(r.HTTPFailures) },
		"cache_hits":           func(r runReport) float64 { return float64(r.CacheHits) },
	}
	result := make(map[string]stat, len(fields))
	for name, extract := range fields {
		values := make([]float64, 0, len(runs))
		for _, run := range runs {
			values = append(values, extract(run))
		}
		result[name] = summarise(values)
	}
	return result
}

func summarise(values []float64) stat {
	if len(values) == 0 {
		return stat{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	q1 := percentile(sorted, 0.25)
	q3 := percentile(sorted, 0.75)
	return stat{
		Median:  percentile(sorted, 0.50),
		Q1:      q1,
		Q3:      q3,
		IQR:     q3 - q1,
		Min:     sorted[0],
		Max:     sorted[len(sorted)-1],
		Samples: len(sorted),
	}
}

type environmentSources struct {
	openSearchURL string
	index         string
	commit        string
	note          string
	backend       string
	faultSource   string
	scenario      string
}

// collectEnvironment records what produced the numbers. Anything it cannot
// reach is listed under "unavailable" rather than silently omitted, so a reader
// can tell a zero from a value that was never collected.
func collectEnvironment(api *client, sources environmentSources) environment {
	env := environment{
		Commit:      sources.commit,
		Note:        sources.note,
		Backend:     sources.backend,
		FaultSource: sources.faultSource,
		Scenario:    sources.scenario,
		Index:       sources.index,
		Unavailable: map[string]string{},
	}

	if body, err := fetch(api.http, api.base+"/readyz", api.apiKey); err == nil {
		var ready struct {
			Dependencies struct {
				OpenSearch struct {
					Mode      string `json:"mode"`
					Documents int    `json:"documents"`
				} `json:"opensearch"`
			} `json:"dependencies"`
		}
		if json.Unmarshal(body, &ready) == nil {
			env.LatticeMode = ready.Dependencies.OpenSearch.Mode
			env.Documents = ready.Dependencies.OpenSearch.Documents
		}
	} else {
		env.Unavailable["lattice_readyz"] = err.Error()
	}

	if sources.openSearchURL == "" {
		env.Unavailable["opensearch"] = "no -opensearch-url given; cluster shape not recorded"
		return env
	}

	if body, err := fetch(api.http, sources.openSearchURL+"/", ""); err == nil {
		var root struct {
			ClusterName string `json:"cluster_name"`
			Version     struct {
				Number string `json:"number"`
			} `json:"version"`
		}
		if json.Unmarshal(body, &root) == nil {
			env.ClusterName = root.ClusterName
			env.OpenSearchVersion = root.Version.Number
		}
	} else {
		env.Unavailable["opensearch_root"] = err.Error()
	}

	if body, err := fetch(api.http, sources.openSearchURL+"/_cat/nodes?format=json&h=name,node.role,heap.max", ""); err == nil {
		var nodes []struct {
			Name    string `json:"name"`
			Role    string `json:"node.role"`
			HeapMax string `json:"heap.max"`
		}
		if json.Unmarshal(body, &nodes) == nil {
			env.Nodes = len(nodes)
			for _, node := range nodes {
				env.NodeHeap = append(env.NodeHeap, fmt.Sprintf("%s role=%s heap_max=%s", node.Name, node.Role, node.HeapMax))
			}
		}
	} else {
		env.Unavailable["opensearch_nodes"] = err.Error()
	}

	if body, err := fetch(api.http, sources.openSearchURL+"/"+sources.index+"/_settings", ""); err == nil {
		var settings map[string]struct {
			Settings struct {
				Index struct {
					NumberOfShards   string `json:"number_of_shards"`
					NumberOfReplicas string `json:"number_of_replicas"`
				} `json:"index"`
			} `json:"settings"`
		}
		if json.Unmarshal(body, &settings) == nil {
			for _, value := range settings {
				env.Shards = value.Settings.Index.NumberOfShards
				env.Replicas = value.Settings.Index.NumberOfReplicas
				break
			}
		}
	} else {
		env.Unavailable["opensearch_settings"] = err.Error()
	}

	env.SegmentsAtStart = segmentCount(sources.openSearchURL, sources.index)
	if env.SegmentsAtStart == 0 {
		env.Unavailable["opensearch_segments"] = "segment count not collected"
	}
	if len(env.Unavailable) == 0 {
		env.Unavailable = nil
	}
	return env
}

// segmentCount reports primary-shard segments at the moment it is called. It is
// sampled per run because segment count is the variable that dominates tail
// latency, and a p99 recorded without it cannot be placed on a curve.
func segmentCount(openSearchURL, index string) int {
	if openSearchURL == "" || index == "" {
		return 0
	}
	body, err := fetch(&http.Client{Timeout: 10 * time.Second}, openSearchURL+"/"+index+"/_segments", "")
	if err != nil {
		return 0
	}
	var response struct {
		Indices map[string]struct {
			Shards map[string][]struct {
				Routing struct {
					Primary bool `json:"primary"`
				} `json:"routing"`
				NumCommittedSegments int `json:"num_committed_segments"`
				NumSearchSegments    int `json:"num_search_segments"`
			} `json:"shards"`
		} `json:"indices"`
	}
	if json.Unmarshal(body, &response) != nil {
		return 0
	}
	total := 0
	for _, indexEntry := range response.Indices {
		for _, shard := range indexEntry.Shards {
			for _, copyEntry := range shard {
				if copyEntry.Routing.Primary {
					total += copyEntry.NumSearchSegments
				}
			}
		}
	}
	return total
}

func fetch(httpClient *http.Client, target, apiKey string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if apiKey != "" {
		request.Header.Set("X-API-Key", apiKey)
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", target, response.Status)
	}
	return body, nil
}

// splitQuerySpec accepts either a bare query or label=query, so a report can
// name its query classes instead of leaving a reader to infer them.
func splitQuerySpec(spec string) (string, string) {
	if label, query, found := strings.Cut(spec, "="); found && strings.TrimSpace(label) != "" && strings.TrimSpace(query) != "" {
		return strings.TrimSpace(label), query
	}
	return spec, spec
}

func parseConcurrency(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	levels := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		level, err := strconv.Atoi(part)
		if err != nil || level < 1 {
			return nil, fmt.Errorf("concurrency %q must be a comma-separated list of positive integers", value)
		}
		levels = append(levels, level)
	}
	if len(levels) == 0 {
		return nil, fmt.Errorf("concurrency %q must contain at least one level", value)
	}
	return levels, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	position := quantile * float64(len(values)-1)
	lower := int(position)
	upper := lower
	if upper+1 < len(values) {
		upper++
	}
	weight := position - float64(lower)
	return values[lower] + (values[upper]-values[lower])*weight
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
