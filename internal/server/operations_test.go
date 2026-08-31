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

func TestOperationalEndpoints(t *testing.T) {
	index, err := search.OpenPersistentIndex(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	s := New(index)
	h := s.Handler()
	publish := httptest.NewRequest(http.MethodPost, "/v1/documents", strings.NewReader(`{"id":"one","title":"One","body":"durable"}`))
	publish.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, publish)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("publish status = %d", recorder.Code)
	}
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	recorder = httptest.NewRecorder()
	snapshotBody, _ := json.Marshal(map[string]string{"path": snapshotPath})
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/admin/snapshot", strings.NewReader(string(snapshotBody))))
	if recorder.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := index.Delete("one"); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/admin/restore", strings.NewReader(string(snapshotBody))))
	if recorder.Code != http.StatusOK || index.Count() != 1 {
		t.Fatalf("restore status = %d count=%d body=%s", recorder.Code, index.Count(), recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/admin/reindex", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("reindex status = %d", recorder.Code)
	}
}
