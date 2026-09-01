// Package opensearch contains the HTTP boundary used by the production
// indexer/query adapters. The local search implementation remains dependency
// free, but both paths share request and response contracts.
package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Username   string
	Password   string
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
	if c.Username != "" {
		request.SetBasicAuth(c.Username, c.Password)
	}
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
	if c.Username != "" {
		request.SetBasicAuth(c.Username, c.Password)
	}
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

// ConfigureCAFile installs a client transport that trusts the supplied CA in
// addition to the system roots. It is intended for operator-managed or
// organization-managed OpenSearch certificates.
func (c *Client) ConfigureCAFile(path string) error {
	return c.ConfigureTLS(path, "", "")
}

// ConfigureTLS installs system/organization CA trust and, when both are
// supplied, a client certificate for mutual TLS. The server certificate and
// key are intentionally explicit so a deployment cannot silently downgrade
// from an expected client-authenticated connection.
func (c *Client) ConfigureTLS(caFile, clientCertFile, clientKeyFile string) error {
	if (clientCertFile == "") != (clientKeyFile == "") {
		return fmt.Errorf("opensearch: both client certificate and key are required")
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		data, readErr := os.ReadFile(caFile)
		if readErr != nil {
			return readErr
		}
		if !pool.AppendCertsFromPEM(data) {
			return fmt.Errorf("opensearch: parse CA file %q", caFile)
		}
	}
	baseTransport := http.DefaultTransport
	if c.HTTPClient != nil && c.HTTPClient.Transport != nil {
		baseTransport = c.HTTPClient.Transport
	}
	transport, ok := baseTransport.(*http.Transport)
	if !ok {
		return fmt.Errorf("opensearch: configure TLS requires an HTTP transport")
	}
	transport = transport.Clone()
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool} //nolint:gosec -- TLS 1.3 is the deployment floor.
	if clientCertFile != "" {
		certificate, certErr := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if certErr != nil {
			return fmt.Errorf("opensearch: load client certificate: %w", certErr)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	transport.TLSClientConfig = config
	client := http.DefaultClient
	if c.HTTPClient != nil {
		client = c.HTTPClient
	}
	configuredClient := *client
	configuredClient.Transport = transport
	c.HTTPClient = &configuredClient
	return nil
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

func (c *Client) AliasTargets(ctx context.Context, alias string) ([]string, error) {
	data, err := c.do(ctx, http.MethodGet, "/_alias/"+url.PathEscape(alias), nil)
	if err != nil {
		return nil, err
	}
	var targets map[string]json.RawMessage
	if err := json.Unmarshal(data, &targets); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(targets))
	for index := range targets {
		result = append(result, index)
	}
	return result, nil
}

func (c *Client) AddAlias(ctx context.Context, index, alias string) error {
	body, err := json.Marshal(map[string]any{"actions": []any{map[string]any{"add": map[string]any{"index": index, "alias": alias, "is_write_index": true}}}})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "/_aliases", body)
	return err
}

func (c *Client) SwapAlias(ctx context.Context, alias, target string, oldIndexes []string) error {
	actions := make([]any, 0, len(oldIndexes)+1)
	for _, oldIndex := range oldIndexes {
		actions = append(actions, map[string]any{"remove": map[string]string{"index": oldIndex, "alias": alias}})
	}
	actions = append(actions, map[string]any{"add": map[string]any{"index": target, "alias": alias, "is_write_index": true}})
	body, err := json.Marshal(map[string]any{"actions": actions})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "/_aliases", body)
	return err
}

func (c *Client) Reindex(ctx context.Context, source, destination string) error {
	body, err := json.Marshal(map[string]any{"source": map[string]string{"index": source}, "dest": map[string]string{"index": destination}, "conflicts": "proceed"})
	if err != nil {
		return err
	}
	data, err := c.do(ctx, http.MethodPost, "/_reindex?wait_for_completion=true&refresh=true", body)
	if err != nil {
		return err
	}
	var result struct {
		TimedOut bool              `json:"timed_out"`
		Failures []json.RawMessage `json:"failures"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if result.TimedOut || len(result.Failures) > 0 {
		return fmt.Errorf("opensearch: reindex incomplete (timed_out=%t failures=%d)", result.TimedOut, len(result.Failures))
	}
	return nil
}

func (c *Client) Snapshot(ctx context.Context, repository, snapshot, index string) error {
	body, err := json.Marshal(map[string]any{"indices": index, "include_global_state": false})
	if err != nil {
		return err
	}
	path := "/_snapshot/" + url.PathEscape(repository) + "/" + url.PathEscape(snapshot) + "?wait_for_completion=true"
	data, err := c.do(ctx, http.MethodPut, path, body)
	if err != nil {
		return err
	}
	var result struct {
		Snapshot struct {
			State string `json:"state"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if result.Snapshot.State != "" && result.Snapshot.State != "SUCCESS" {
		return fmt.Errorf("opensearch: snapshot completed with state %s", result.Snapshot.State)
	}
	return nil
}

func (c *Client) Restore(ctx context.Context, repository, snapshot, sourceIndex, targetIndex string) error {
	request := map[string]any{"include_global_state": false, "include_aliases": false}
	if sourceIndex != "" {
		request["indices"] = sourceIndex
	}
	if targetIndex != "" {
		if sourceIndex == "" {
			return fmt.Errorf("opensearch: source index is required when restoring to a new index")
		}
		request["rename_pattern"] = sourceIndex
		request["rename_replacement"] = targetIndex
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	path := "/_snapshot/" + url.PathEscape(repository) + "/" + url.PathEscape(snapshot) + "/_restore?wait_for_completion=true"
	data, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	var result struct {
		Snapshot struct {
			Failures []json.RawMessage `json:"failures"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if len(result.Snapshot.Failures) > 0 {
		return fmt.Errorf("opensearch: restore reported %d failures", len(result.Snapshot.Failures))
	}
	return nil
}

func (c *Client) Ready(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/", nil)
	return err
}
