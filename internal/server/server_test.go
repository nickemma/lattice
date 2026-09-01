package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nickemma/lattice/internal/providers/opensearch"
	"github.com/nickemma/lattice/internal/search"
)

type readinessBackend struct {
	ready bool
}

type responseCache struct{ value []byte }

type serverRoundTripper func(*http.Request) (*http.Response, error)

func (f serverRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (c *responseCache) Get(context.Context, string) ([]byte, bool, error) {
	return c.value, len(c.value) > 0, nil
}
func (c *responseCache) Set(_ context.Context, _ string, value []byte) error {
	c.value = append([]byte(nil), value...)
	return nil
}

func (b readinessBackend) Upsert(search.Document) error { return nil }
func (b readinessBackend) Count() int                   { return 0 }
func (b readinessBackend) Search(context.Context, string, int, int) search.Response {
	return search.Response{Coverage: search.Coverage{ShardsQueried: 1, ShardsAnswered: 1, Complete: true}}
}
func (b readinessBackend) Ready(context.Context) error {
	if !b.ready {
		return errors.New("dependency unavailable")
	}
	return nil
}

func TestDocumentSearchAndDeveloperSurfaces(t *testing.T) {
	handler := New(search.NewIndex(2)).Handler()
	body := `{"id":"1","title":"Raft","body":"leader election and consensus"}`
	request := httptest.NewRequest(http.MethodPost, "/v1/documents", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("publish: response=%v body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/v1/search?q=consensus&deadline=150ms", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("search: response=%v body=%s", recorder.Code, recorder.Body.String())
	}
	for _, path := range []string{"/docs", "/playground", "/openapi.yaml", "/metrics", "/readyz"} {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s: response=%v", path, recorder.Code)
		}
	}
}

func TestRequestIDIsPropagatedAndSanitized(t *testing.T) {
	h := New(search.NewIndex(1)).Handler()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("X-Request-ID", "trace-123")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got != "trace-123" {
		t.Fatalf("request id = %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("X-Request-ID", "bad id")
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got == "bad id" || !strings.HasPrefix(got, "lattice-") {
		t.Fatalf("sanitized request id = %q", got)
	}
}

func TestPaginationGuard(t *testing.T) {
	h := New(search.NewIndex(1)).Handler()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/search?q=x&from=10000", nil)
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestSearchAfterReturnsNextPageAndBindsRequest(t *testing.T) {
	index := search.NewIndex(1)
	for _, id := range []string{"one", "two", "three"} {
		if err := index.Upsert(search.Document{ID: id, Title: "consensus", Body: "consensus"}); err != nil {
			t.Fatal(err)
		}
	}
	h := New(index).Handler()
	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/search?q=consensus&size=1", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first page status = %d: %s", first.Code, first.Body.String())
	}
	var firstPayload search.Response
	if err := json.Unmarshal(first.Body.Bytes(), &firstPayload); err != nil {
		t.Fatal(err)
	}
	if firstPayload.NextCursor == "" || len(firstPayload.Results) != 1 {
		t.Fatalf("first page = %+v", firstPayload)
	}

	next := httptest.NewRecorder()
	path := "/v1/search?q=consensus&size=1&search_after=" + firstPayload.NextCursor
	h.ServeHTTP(next, httptest.NewRequest(http.MethodGet, path, nil))
	if next.Code != http.StatusOK {
		t.Fatalf("next page status = %d: %s", next.Code, next.Body.String())
	}
	var nextPayload search.Response
	if err := json.Unmarshal(next.Body.Bytes(), &nextPayload); err != nil {
		t.Fatal(err)
	}
	if len(nextPayload.Results) != 1 || nextPayload.Results[0].Document.ID == firstPayload.Results[0].Document.ID {
		t.Fatalf("next page = %+v", nextPayload)
	}

	mismatch := httptest.NewRecorder()
	h.ServeHTTP(mismatch, httptest.NewRequest(http.MethodGet, "/v1/search?q=other&size=1&search_after="+firstPayload.NextCursor, nil))
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("mismatch status = %d: %s", mismatch.Code, mismatch.Body.String())
	}
}

func TestRemoteReadinessReflectsBackend(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewWithBackend(readinessBackend{}).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "not_ready") {
		t.Fatalf("not-ready response = %d %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	NewWithBackend(readinessBackend{ready: true}).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ready response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestSharedCacheServesACompleteResponse(t *testing.T) {
	cache := &responseCache{}
	backend := readinessBackend{ready: true}
	server := NewWithBackendAndPublisherAndCache(backend, nil, cache)
	first := httptest.NewRecorder()
	server.Handler().ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/search?q=cache", nil))
	if first.Code != http.StatusOK || len(cache.value) == 0 {
		t.Fatalf("first response = %d %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	server.Handler().ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/v1/search?q=cache", nil))
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"cache_hit":true`) {
		t.Fatalf("cached response = %d %s", second.Code, second.Body.String())
	}
}

func TestTagFilterIsAcceptedByAPI(t *testing.T) {
	index := search.NewIndex(1)
	index.Upsert(search.Document{ID: "match", Title: "Raft", Body: "consensus", Tags: []string{"systems", "raft"}})
	index.Upsert(search.Document{ID: "miss", Title: "Raft", Body: "consensus", Tags: []string{"systems"}})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/search?q=consensus&tag=systems&tag=raft", nil)
	New(index).Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"id":"match"`) || strings.Contains(recorder.Body.String(), `"id":"miss"`) {
		t.Fatalf("filtered response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestConfiguredAPIKeyProtectsV1Routes(t *testing.T) {
	h := NewWithAPIKey(search.NewIndex(1), "secret").Handler()
	unauthorized := httptest.NewRecorder()
	h.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/search?q=x", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/search?q=x", nil)
	request.Header.Set("X-API-Key", "secret")
	authorized := httptest.NewRecorder()
	h.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d", authorized.Code)
	}

	health := httptest.NewRecorder()
	h.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}
}

func TestDocumentBodyIsBounded(t *testing.T) {
	body := strings.Repeat("x", 1<<20)
	recorder := httptest.NewRecorder()
	New(search.NewIndex(1)).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/documents", strings.NewReader(`{"id":"large","title":"title","body":"`+body+`"}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("large document status = %d", recorder.Code)
	}
}

func TestRemoteAdminLifecycleIsExposedThroughAPI(t *testing.T) {
	client := opensearch.New("http://opensearch")
	client.HTTPClient = &http.Client{Transport: serverRoundTripper(func(request *http.Request) (*http.Response, error) {
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
		case request.Method == http.MethodPut && request.URL.Path == "/_snapshot/repo/snap":
			body = `{}`
		case request.Method == http.MethodPost && request.URL.Path == "/_snapshot/repo/snap/_restore":
			body = `{}`
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	backend := opensearch.NewBackendWithAlias(client, "lattice", "search")
	h := NewWithBackend(backend).Handler()

	reindex := httptest.NewRecorder()
	h.ServeHTTP(reindex, httptest.NewRequest(http.MethodPost, "/v1/admin/reindex", nil))
	if reindex.Code != http.StatusOK || !strings.Contains(reindex.Body.String(), `"alias":"search"`) {
		t.Fatalf("reindex response = %d %s", reindex.Code, reindex.Body.String())
	}

	snapshot := httptest.NewRecorder()
	h.ServeHTTP(snapshot, httptest.NewRequest(http.MethodPost, "/v1/admin/snapshot", strings.NewReader(`{"repository":"repo","snapshot":"snap"}`)))
	if snapshot.Code != http.StatusOK {
		t.Fatalf("snapshot response = %d %s", snapshot.Code, snapshot.Body.String())
	}

	restore := httptest.NewRecorder()
	h.ServeHTTP(restore, httptest.NewRequest(http.MethodPost, "/v1/admin/restore", strings.NewReader(`{"repository":"repo","snapshot":"snap","source_index":"lattice-1","target_index":"lattice-restore"}`)))
	if restore.Code != http.StatusOK {
		t.Fatalf("restore response = %d %s", restore.Code, restore.Body.String())
	}
}
