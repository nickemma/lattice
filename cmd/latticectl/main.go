package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
		seed(address)
	case "search":
		searchCommand(address, os.Args[2:])
	default:
		usage()
	}
}

func seed(address string) {
	documents := []map[string]any{
		{"id": "doc-001", "title": "Raft leader election", "body": "A replicated system chooses a leader and commits a log safely.", "tags": []string{"consensus"}},
		{"id": "doc-002", "title": "LSM storage", "body": "A write ahead log and sorted string tables make durable writes recoverable.", "tags": []string{"storage"}},
		{"id": "doc-003", "title": "Consistent hashing", "body": "Sharding distributes keys while allowing nodes to be rebalanced.", "tags": []string{"sharding"}},
	}
	for _, document := range documents {
		body, _ := json.Marshal(document)
		post(address+"/v1/documents", body)
	}
}

func searchCommand(address string, args []string) {
	searchFlags := flag.NewFlagSet("search", flag.ExitOnError)
	query := searchFlags.String("q", "distributed consensus", "query text")
	_ = searchFlags.Parse(args)
	get(address + "/v1/search?q=" + url.QueryEscape(*query) + "&deadline=150ms")
}

func get(endpoint string) {
	response, err := http.Get(endpoint)
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

func post(endpoint string, body []byte) {
	response, err := http.Post(endpoint, "application/json", bytesReader(body))
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
	fmt.Fprintln(os.Stderr, "usage: latticectl health | seed | search -q 'query'")
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
