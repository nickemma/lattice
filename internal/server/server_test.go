package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nickemma/lattice/internal/search"
)

type readinessBackend struct {
	ready bool
}

type responseCache struct{ value []byte }

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

func TestPaginationGuard(t *testing.T) {
	h := New(search.NewIndex(1)).Handler()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/search?q=x&from=10000", nil)
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", recorder.Code)
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
