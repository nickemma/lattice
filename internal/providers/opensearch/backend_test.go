package opensearch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nickemma/lattice/internal/search"
)

func TestBackendIndexesAndFusesOpenSearchBranches(t *testing.T) {
	client := New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(request *http.Request) (*http.Response, error) {
		var body string
		switch request.URL.Path {
		case "/_bulk":
			body = `{"errors":false,"items":[{"index":{"status":201}}]}`
		case "/lattice/_count":
			body = `{"count":1}`
		case "/lattice/_search":
			body = `{"_shards":{"total":3,"successful":3},"hits":{"hits":[{"_id":"doc-1","_score":2,"_source":{"title":"Raft","body":"consensus"}}]}}`
		default:
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	backend := NewBackend(client, "lattice")
	if err := backend.Upsert(search.Document{ID: "doc-1", Title: "Raft", Body: "consensus"}); err != nil {
		t.Fatal(err)
	}
	if backend.Count() != 1 {
		t.Fatalf("count = %d", backend.Count())
	}
	response := backend.Search(context.Background(), "consensus", 0, 10)
	if len(response.Results) != 1 || !response.Coverage.Complete || response.Timings.BM25 == 0 || response.Timings.Vector == 0 {
		t.Fatalf("response = %+v", response)
	}
}

func TestBulkUpsertReturnsPerDocumentOutcomes(t *testing.T) {
	client := New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/_bulk" {
			t.Fatalf("path = %s", request.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":429,"error":{"reason":"queue full"}}}]}`))}, nil
	})}
	backend := NewBackend(client, "lattice")
	outcomes, err := backend.BulkUpsert(context.Background(), []search.Document{
		{ID: "one", Title: "One", Body: "body"},
		{ID: "two", Title: "Two", Body: "body"},
	})
	if err != nil || len(outcomes) != 2 {
		t.Fatalf("outcomes = %+v, %v", outcomes, err)
	}
	if outcomes[0].Status != http.StatusCreated || outcomes[1].Status != http.StatusTooManyRequests {
		t.Fatalf("outcomes = %+v", outcomes)
	}
}

func TestBackendReindexesAndSwapsAlias(t *testing.T) {
	client := New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(request *http.Request) (*http.Response, error) {
		var body string
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/_alias/search":
			body = `{"lattice-1":{}}`
		case request.Method == http.MethodPut && strings.HasPrefix(request.URL.Path, "/lattice-"):
			body = `{}`
		case request.Method == http.MethodPost && request.URL.Path == "/_reindex":
			body = `{"total":1,"created":1,"failures":[]}`
		case request.Method == http.MethodPost && request.URL.Path == "/_aliases":
			body = `{}`
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	backend := NewBackendWithAlias(client, "lattice", "search")
	report, err := backend.Reindex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "lattice-1" || report.Alias != "search" || report.Destination == "" {
		t.Fatalf("report = %+v", report)
	}
}

func TestBulkUpsertDualWritesDuringReindex(t *testing.T) {
	client := New("http://opensearch")
	bulkCalls := 0
	client.HTTPClient = &http.Client{Transport: roundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/_bulk" {
			t.Fatalf("path = %s", request.URL.Path)
		}
		bulkCalls++
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"errors":false,"items":[{"index":{"status":201}}]}`))}, nil
	})}
	backend := NewBackend(client, "lattice")
	backend.writeMu.Lock()
	backend.dualWrite = "lattice-new"
	backend.writeMu.Unlock()
	outcomes, err := backend.BulkUpsert(context.Background(), []search.Document{{ID: "one", Title: "One", Body: "body"}})
	if err != nil || len(outcomes) != 1 || outcomes[0].Status != http.StatusCreated || bulkCalls != 2 {
		t.Fatalf("outcomes=%+v err=%v bulk_calls=%d", outcomes, err, bulkCalls)
	}
}
