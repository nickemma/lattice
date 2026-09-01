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

func TestLifecycleRequestsUseNativeOpenSearchEndpoints(t *testing.T) {
	client := New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		var body string
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/_alias/search":
			body = `{"lattice-1":{}}`
		case r.Method == http.MethodPut && r.URL.Path == "/_snapshot/repo/snap":
			body = `{}`
		case r.Method == http.MethodPost && r.URL.Path == "/_snapshot/repo/snap/_restore":
			body = `{}`
		default:
			t.Fatalf("unexpected lifecycle request: %s %s", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	if targets, err := client.AliasTargets(context.Background(), "search"); err != nil || len(targets) != 1 || targets[0] != "lattice-1" {
		t.Fatalf("alias targets = %v, %v", targets, err)
	}
	if err := client.Snapshot(context.Background(), "repo", "snap", "search"); err != nil {
		t.Fatal(err)
	}
	if err := client.Restore(context.Background(), "repo", "snap", "search", ""); err != nil {
		t.Fatal(err)
	}
}

func TestClientAddsBasicAuth(t *testing.T) {
	client := New("http://opensearch")
	client.Username = "lattice"
	client.Password = "secret"
	client.HTTPClient = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "lattice" || password != "secret" {
			t.Fatalf("basic auth = %q %q %t", username, password, ok)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	if err := client.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigureTLSRequiresCompleteClientCertificate(t *testing.T) {
	client := New("https://opensearch")
	if err := client.ConfigureTLS("", "client.crt", ""); err == nil {
		t.Fatal("expected incomplete client certificate configuration to fail")
	}
}
