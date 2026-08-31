package embedding

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPClientEmbedsBatches(t *testing.T) {
	client := New("http://embedding")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/embeddings" {
			t.Fatalf("request = %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"embeddings":[[1,2],[3,4]]}`))}, nil
	})}
	vectors, err := client.Embed(t.Context(), []string{"one", "two"})
	if err != nil || len(vectors) != 2 || len(vectors[0]) != 2 {
		t.Fatalf("vectors = %#v, %v", vectors, err)
	}
}
