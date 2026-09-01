package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
)

func main() {
	address := "http://localhost:8080"
	if value := os.Getenv("LATTICE_URL"); value != "" {
		address = value
	}
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "health":
		get(address + "/readyz")
	case "seed":
		seed(address, os.Args[2:])
	case "search":
		searchCommand(address, os.Args[2:])
	default:
		usage()
	}
}

func seed(address string, args []string) {
	seedFlags := flag.NewFlagSet("seed", flag.ExitOnError)
	count := seedFlags.Int("count", 3, "number of deterministic documents to publish")
	concurrency := seedFlags.Int("concurrency", 32, "maximum concurrent publish requests")
	_ = seedFlags.Parse(args)
	if *count < 1 || *concurrency < 1 {
		fatal(fmt.Errorf("count and concurrency must be positive"))
	}
	if *concurrency > *count {
		*concurrency = *count
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = *concurrency
	transport.MaxIdleConnsPerHost = *concurrency
	transport.MaxConnsPerHost = *concurrency
	client := &http.Client{Transport: transport}
	var progressMu sync.Mutex
	err := seedDocuments(address, *count, *concurrency, client, func(completed int) {
		if *count <= 10 || completed == 1 || completed%1000 == 0 || completed == *count {
			progressMu.Lock()
			fmt.Printf("seeded %d/%d documents\n", completed, *count)
			progressMu.Unlock()
		}
	})
	if err != nil {
		fatal(err)
	}
}

// seedDocuments publishes a deterministic corpus with bounded concurrency.
// The limit is deliberately explicit: callers can increase throughput for a
// capacity run without allowing the CLI to create unbounded pressure on the
// API or Kafka producer.
func seedDocuments(address string, count, concurrency int, client *http.Client, progress func(int)) error {
	if count < 1 || concurrency < 1 {
		return fmt.Errorf("count and concurrency must be positive")
	}
	if concurrency > count {
		concurrency = count
	}
	if client == nil {
		client = http.DefaultClient
	}
	jobs := make(chan int)
	var wait sync.WaitGroup
	var completed atomic.Int64
	var firstErr error
	var errOnce sync.Once
	worker := func() {
		defer wait.Done()
		for n := range jobs {
			body, err := json.Marshal(seedDocument(n))
			if err == nil {
				_, err = postResponseWithClient(client, address+"/v1/documents", body)
			}
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				continue
			}
			if progress != nil {
				progress(int(completed.Add(1)))
			} else {
				completed.Add(1)
			}
		}
	}
	wait.Add(concurrency)
	for n := 0; n < concurrency; n++ {
		go worker()
	}
	for n := 0; n < count; n++ {
		jobs <- n
	}
	close(jobs)
	wait.Wait()
	return firstErr
}

func seedDocument(n int) map[string]any {
	topics := []struct {
		title string
		body  string
		tags  []string
	}{
		{title: "Raft leader election", body: "A replicated system chooses a leader and commits a log safely.", tags: []string{"consensus", "raft"}},
		{title: "LSM storage", body: "A write ahead log and sorted string tables make durable writes recoverable.", tags: []string{"storage", "wal"}},
		{title: "Consistent hashing", body: "Sharding distributes keys while allowing nodes to be rebalanced.", tags: []string{"sharding", "distribution"}},
	}
	topic := topics[n%len(topics)]
	return map[string]any{
		"id":    fmt.Sprintf("seed-%08d", n),
		"title": topic.title,
		"body":  fmt.Sprintf("%s Synthetic corpus record %d.", topic.body, n),
		"tags":  topic.tags,
	}
}

func searchCommand(address string, args []string) {
	searchFlags := flag.NewFlagSet("search", flag.ExitOnError)
	query := searchFlags.String("q", "distributed consensus", "query text")
	_ = searchFlags.Parse(args)
	get(address + "/v1/search?q=" + url.QueryEscape(*query) + "&deadline=150ms")
}

func get(endpoint string) {
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		fatal(err)
	}
	setAPIKey(request)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		fatal(fmt.Errorf("%s returned %s", endpoint, response.Status))
	}
	data, _ := io.ReadAll(response.Body)
	fmt.Println(string(data))
}

func postResponse(endpoint string, body []byte) ([]byte, error) {
	return postResponseWithClient(http.DefaultClient, endpoint, body)
}

func postResponseWithClient(client *http.Client, endpoint string, body []byte) ([]byte, error) {
	request, err := http.NewRequest(http.MethodPost, endpoint, bytesReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	setAPIKey(request)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, readErr
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s returned %s: %s", endpoint, response.Status, string(data))
	}
	return data, nil
}

func setAPIKey(request *http.Request) {
	if key := os.Getenv("LATTICE_API_KEY"); key != "" {
		request.Header.Set("X-API-Key", key)
	}
}

type byteReader struct {
	data   []byte
	offset int
}

func bytesReader(data []byte) io.Reader { return &byteReader{data: data} }

func (r *byteReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: latticectl health | seed [-count N] [-concurrency N] | search -q 'query'")
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
