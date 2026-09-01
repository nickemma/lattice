package main

import (
	"net/http"
	"testing"
	"time"
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
