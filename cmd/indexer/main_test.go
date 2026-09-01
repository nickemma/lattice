package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposePartitionLag(t *testing.T) {
	m := &metrics{lagByPart: map[int]int64{2: 2500, 0: 500}}
	recorder := httptest.NewRecorder()
	m.handler(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `lattice_ingest_lag_seconds{partition="0"} 0.500`) || !strings.Contains(body, `lattice_ingest_lag_seconds{partition="2"} 2.500`) {
		t.Fatalf("partition lag metrics = %s", body)
	}
}

func TestDependencyTimeout(t *testing.T) {
	t.Setenv("LATTICE_DEPENDENCY_TIMEOUT", "17s")
	if got := dependencyTimeout(); got != 17*time.Second {
		t.Fatalf("dependency timeout = %s, want 17s", got)
	}
}
