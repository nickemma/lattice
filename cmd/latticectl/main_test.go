package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSeedDocumentUsesStableIDsAndRotatingTopics(t *testing.T) {
	first := seedDocument(0)
	second := seedDocument(3)
	if first["id"] != "seed-00000000" || second["id"] != "seed-00000003" {
		t.Fatalf("ids = %v, %v", first["id"], second["id"])
	}
	if first["title"] != second["title"] || first["body"] == second["body"] {
		t.Fatalf("seed records are not deterministic/unique: %#v %#v", first, second)
	}
}

func TestSeedDocumentsHonorsConcurrencyLimit(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/documents" || r.Method != http.MethodPost {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		var document map[string]any
		if err := json.NewDecoder(r.Body).Decode(&document); err != nil {
			t.Fatalf("decode document: %v", err)
		}
		requests.Add(1)
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	if err := seedDocuments(server.URL, 24, 4, server.Client(), nil); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 24 {
		t.Fatalf("requests = %d, want 24", requests.Load())
	}
	if maximum.Load() != 4 {
		t.Fatalf("maximum concurrency = %d, want 4", maximum.Load())
	}
}
