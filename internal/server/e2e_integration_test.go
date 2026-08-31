//go:build integration

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nickemma/lattice/internal/search"
)

func TestWalkthroughJourney(t *testing.T) {
	index, err := search.OpenPersistentIndex(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	handler := New(index).Handler()
	post := func(path, body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(recorder, request)
		return recorder
	}
	for _, document := range []string{
		`{"id":"doc-1","title":"Raft leader election","body":"consensus chooses a leader"}`,
		`{"id":"doc-2","title":"LSM storage","body":"a write ahead log recovers data"}`,
	} {
		if response := post("/v1/documents", document); response.Code != http.StatusAccepted {
			t.Fatalf("publish status = %d", response.Code)
		}
	}
	searchResponse := httptest.NewRecorder()
	handler.ServeHTTP(searchResponse, httptest.NewRequest(http.MethodGet, "/v1/search?q=leader%20consensus&deadline=150ms", nil))
	if searchResponse.Code != http.StatusOK || !strings.Contains(searchResponse.Body.String(), "doc-1") {
		t.Fatalf("search response = %d %s", searchResponse.Code, searchResponse.Body.String())
	}
	if response := post("/v1/debug/shards/0", `{"available":false}`); response.Code != http.StatusOK {
		t.Fatalf("failure drill status = %d", response.Code)
	}
	degraded := httptest.NewRecorder()
	handler.ServeHTTP(degraded, httptest.NewRequest(http.MethodGet, "/v1/search?q=leader&deadline=150ms", nil))
	if degraded.Code != http.StatusOK || !strings.Contains(degraded.Body.String(), `"complete":false`) {
		t.Fatalf("degraded response = %d %s", degraded.Code, degraded.Body.String())
	}
	if response := post("/v1/debug/shards/0", `{"available":true}`); response.Code != http.StatusOK {
		t.Fatalf("restore shard status = %d", response.Code)
	}
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	body, _ := json.Marshal(map[string]string{"path": snapshotPath})
	if response := post("/v1/admin/snapshot", string(body)); response.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d", response.Code)
	}
	if err := index.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	if response := post("/v1/admin/restore", string(body)); response.Code != http.StatusOK || index.Count() != 2 {
		t.Fatalf("restore status=%d count=%d", response.Code, index.Count())
	}
	if response := post("/v1/admin/reindex", "{}"); response.Code != http.StatusOK {
		t.Fatalf("reindex status = %d", response.Code)
	}
}
