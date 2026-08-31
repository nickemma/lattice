package opensearch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBulkInspectsPerItemFailures(t *testing.T) {
	client := New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/_bulk" || r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Fatalf("request = %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":429,"error":{"reason":"queue full"}}}]}`))}, nil
	})}
	result, err := client.Bulk(context.Background(), []byte("{}\n"))
	if err != nil || !result.Errors || len(result.Items) != 2 || result.Items[1].Index.Status != 429 {
		t.Fatalf("bulk result = %+v, %v", result, err)
	}
}
