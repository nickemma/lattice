// Command latticebench measures query latency against a running LATTICE API.
// It intentionally does not seed data: the corpus and embedding model must be
// declared by the operator before a capacity result is considered evidence.
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
	"strings"
	"sync"
	"time"
)

type report struct {
	URL           string  `json:"url"`
	Query         string  `json:"query"`
	Queries       int     `json:"queries"`
	Concurrency   int     `json:"concurrency"`
	Completed     int     `json:"completed"`
	HTTPFailures  int     `json:"http_failures"`
	Incomplete    int     `json:"incomplete_responses"`
	APIErrors     int     `json:"api_error_responses"`
	P50MS         float64 `json:"p50_ms"`
	P95MS         float64 `json:"p95_ms"`
	P99MS         float64 `json:"p99_ms"`
	Seconds       float64 `json:"duration_seconds"`
	QueriesPerSec float64 `json:"queries_per_second"`
	StartedAt     string  `json:"started_at"`
}

func main() {
	defaultURL := os.Getenv("LATTICE_E2E_URL")
	if defaultURL == "" {
		defaultURL = "http://localhost:8080"
	}
	endpoint := flag.String("url", defaultURL, "LATTICE base URL")
	query := flag.String("q", "distributed consensus", "search query")
	queries := flag.Int("queries", 1000, "number of queries")
	concurrency := flag.Int("concurrency", 16, "number of concurrent clients")
	deadline := flag.Duration("deadline", 200*time.Millisecond, "API deadline sent with each query")
	apiKey := flag.String("api-key", os.Getenv("LATTICE_E2E_API_KEY"), "X-API-Key value")
	output := flag.String("output", "", "optional JSON report path")
	flag.Parse()
	if *queries < 1 || *concurrency < 1 || *deadline <= 0 {
		fatal("queries, concurrency, and deadline must be positive")
	}

	base := strings.TrimRight(*endpoint, "/")
	client := &http.Client{Timeout: 30 * time.Second}
	started := time.Now().UTC()
	jobs := make(chan int)
	var wait sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]float64, 0, *queries)
	completed, httpFailures, incomplete, apiErrors := 0, 0, 0, 0
	worker := func() {
		defer wait.Done()
		for range jobs {
			requestURL := base + "/v1/search?q=" + url.QueryEscape(*query) + "&deadline=" + url.QueryEscape(deadline.String())
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, requestURL, nil)
			if err != nil {
				mu.Lock()
				httpFailures++
				mu.Unlock()
				continue
			}
			if *apiKey != "" {
				request.Header.Set("X-API-Key", *apiKey)
			}
			requestStarted := time.Now()
			response, requestErr := client.Do(request)
			elapsed := time.Since(requestStarted).Seconds() * 1000
			if requestErr != nil {
				mu.Lock()
				httpFailures++
				mu.Unlock()
				continue
			}
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			mu.Lock()
			if response.StatusCode >= 300 || readErr != nil {
				httpFailures++
			} else {
				completed++
				latencies = append(latencies, elapsed)
				var payload struct {
					Coverage struct {
						Complete bool `json:"complete"`
					} `json:"coverage"`
					Errors []string `json:"errors"`
				}
				if json.Unmarshal(body, &payload) == nil {
					if !payload.Coverage.Complete {
						incomplete++
					}
					if len(payload.Errors) > 0 {
						apiErrors++
					}
				}
			}
			mu.Unlock()
		}
	}
	workers := *concurrency
	if workers > *queries {
		workers = *queries
	}
	wait.Add(workers)
	for n := 0; n < workers; n++ {
		go worker()
	}
	for n := 0; n < *queries; n++ {
		jobs <- n
	}
	close(jobs)
	wait.Wait()

	sort.Float64s(latencies)
	elapsed := time.Since(started).Seconds()
	result := report{URL: base, Query: *query, Queries: *queries, Concurrency: workers, Completed: completed, HTTPFailures: httpFailures, Incomplete: incomplete, APIErrors: apiErrors, Seconds: elapsed, StartedAt: started.Format(time.RFC3339)}
	if len(latencies) > 0 {
		result.P50MS = percentile(latencies, 0.50)
		result.P95MS = percentile(latencies, 0.95)
		result.P99MS = percentile(latencies, 0.99)
	}
	if elapsed > 0 {
		result.QueriesPerSec = float64(completed) / elapsed
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
	if completed == 0 {
		os.Exit(1)
	}
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
