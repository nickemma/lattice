// Package opensearch contains the HTTP boundary used by the production
// indexer/query adapters. The local search implementation remains dependency
// free, but both paths share request and response contracts.
package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: http.DefaultClient}
}

func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(body))
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
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("opensearch: %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (c *Client) Bulk(ctx context.Context, payload []byte) (BulkResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/_bulk", bytes.NewReader(payload))
	if err != nil {
		return BulkResponse{}, err
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return BulkResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return BulkResponse{}, fmt.Errorf("opensearch bulk: %s", response.Status)
	}
	var result BulkResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return BulkResponse{}, err
	}
	return result, nil
}

type BulkResponse struct {
	Errors bool       `json:"errors"`
	Items  []BulkItem `json:"items"`
}

type BulkItem struct {
	Index BulkItemResult `json:"index"`
}

type BulkItemResult struct {
	Status int             `json:"status"`
	Error  json.RawMessage `json:"error,omitempty"`
}

func (c *Client) Search(ctx context.Context, index string, body []byte) ([]byte, error) {
	return c.do(ctx, http.MethodPost, "/"+index+"/_search", body)
}

func (c *Client) Count(ctx context.Context, index string) (int64, error) {
	data, err := c.do(ctx, http.MethodGet, "/"+index+"/_count", nil)
	if err != nil {
		return 0, err
	}
	var response struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return 0, err
	}
	return response.Count, nil
}

func (c *Client) Ready(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/", nil)
	return err
}
