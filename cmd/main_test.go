package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nickemma/lattice/internal/search"
	"github.com/nickemma/lattice/internal/server"
)

func TestServeRejectsIncompleteTLSConfiguration(t *testing.T) {
	t.Setenv("LATTICE_TLS_CERT_FILE", "/missing/cert.pem")
	t.Setenv("LATTICE_TLS_KEY_FILE", "")
	if err := serve(&http.Server{}); err == nil {
		t.Fatal("expected incomplete TLS configuration to fail")
	}
}

func TestDependencyTimeout(t *testing.T) {
	t.Setenv("LATTICE_DEPENDENCY_TIMEOUT", "17s")
	if got := dependencyTimeout(); got != 17*time.Second {
		t.Fatalf("dependency timeout = %s, want 17s", got)
	}
	t.Setenv("LATTICE_DEPENDENCY_TIMEOUT", "not-a-duration")
	if got := dependencyTimeout(); got != 10*time.Minute {
		t.Fatalf("invalid dependency timeout = %s, want 10m", got)
	}
}

// TestNoRedisYieldsGenuinelyNilCache is the regression test for a crash in the
// readiness probe. Returning a nil *redis.Cache as a server.Cache produces a
// non-nil interface holding a nil pointer, so every `if s.Cache != nil` guard in
// the server passes and the first call through it panics. A deployment with
// OpenSearch but no Redis hit this on /readyz.
func TestNoRedisYieldsGenuinelyNilCache(t *testing.T) {
	cache, closeCache, err := newResultCache("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeCache(); err != nil {
			t.Fatal(err)
		}
	}()
	if cache != nil {
		t.Fatalf("cache = %#v, want a nil interface", cache)
	}
	// The server only guards with != nil, so that is the check that must hold.
	api := server.NewWithBackendAndPublisherAndCache(search.NewIndex(1), nil, cache)
	if api.Cache != nil {
		t.Fatalf("server cache = %#v, want nil", api.Cache)
	}
	recorder := httptest.NewRecorder()
	api.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("readyz = %d %s", recorder.Code, recorder.Body.String())
	}
}
