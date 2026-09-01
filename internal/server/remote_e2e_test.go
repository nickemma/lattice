//go:build integration

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestComposeJourney exercises the real query service when LATTICE_E2E_URL is
// supplied (for example, http://localhost:8080 after make compose-up). It is
// skipped in ordinary unit runs because external services are intentional.
func TestComposeJourney(t *testing.T) {
	base := strings.TrimRight(os.Getenv("LATTICE_E2E_URL"), "/")
	if base == "" {
		t.Skip("set LATTICE_E2E_URL to run against Compose/Kubernetes")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	apiKey := os.Getenv("LATTICE_E2E_API_KEY")
	response, err := client.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("readyz status=%d body=%s", response.StatusCode, body)
	}
	response.Body.Close()

	id := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	document, _ := json.Marshal(map[string]any{"id": id, "title": "LATTICE integration document", "body": "end to end Kafka OpenSearch hybrid search", "tags": []string{"integration"}})
	request, err := http.NewRequest(http.MethodPost, base+"/v1/documents", bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		request.Header.Set("X-API-Key", apiKey)
	}
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("publish status=%d body=%s", response.StatusCode, body)
	}
	response.Body.Close()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		request, requestErr := http.NewRequest(http.MethodGet, base+"/v1/search?q="+id+"&deadline=2s", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		if apiKey != "" {
			request.Header.Set("X-API-Key", apiKey)
		}
		response, err = client.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && bytes.Contains(body, []byte(id)) {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("document %s did not become queryable before deadline", id)
}
