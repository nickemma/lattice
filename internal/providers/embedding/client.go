// Package embedding defines the boundary between indexing/querying and an
// embedding model. The HTTP adapter accepts a simple batch contract; when no
// endpoint is configured LATTICE uses its deterministic local embedder.
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Embedder interface {
	Embed(context.Context, []string) ([][]float64, error)
}

type HTTPClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: http.DefaultClient}
}

func (c *HTTPClient) Embed(ctx context.Context, texts []string) ([][]float64, error) {
	payload, err := json.Marshal(map[string]any{"texts": texts})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("embedding service: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var result struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Embeddings) != len(texts) {
		return nil, fmt.Errorf("embedding service returned %d vectors for %d texts", len(result.Embeddings), len(texts))
	}
	return result.Embeddings, nil
}

func (c *HTTPClient) Ready(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return fmt.Errorf("embedding service: %s", response.Status)
	}
	return nil
}
