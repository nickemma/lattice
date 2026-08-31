package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nickemma/lattice/internal/providers/embedding"
	"github.com/nickemma/lattice/internal/search"
)

type Backend struct {
	Client   *Client
	Index    string
	Embedder embedding.Embedder
	count    atomic.Int64
}

type branchResult struct {
	name    string
	results []search.Result
	shards  shardsInfo
	err     error
	elapsed time.Duration
}

func NewBackend(client *Client, index string) *Backend {
	return &Backend{Client: client, Index: index}
}

func NewBackendWithEmbedder(client *Client, index string, embedder embedding.Embedder) *Backend {
	backend := NewBackend(client, index)
	backend.Embedder = embedder
	return backend
}

func (b *Backend) Upsert(doc search.Document) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcomes, err := b.BulkUpsert(ctx, []search.Document{doc})
	if err != nil {
		return err
	}
	if len(outcomes) != 1 || outcomes[0].Status >= 300 {
		if len(outcomes) == 0 {
			return errors.New("opensearch: bulk response did not contain the document")
		}
		return fmt.Errorf("opensearch index status %d: %s", outcomes[0].Status, outcomes[0].Error)
	}
	return nil
}

type BulkOutcome struct {
	ID     string
	Status int
	Error  string
}

// BulkUpsert preserves OpenSearch's per-item result semantics for the Kafka
// indexer. A successful HTTP response is not enough: every item must be
// classified before the offset can advance.
func (b *Backend) BulkUpsert(ctx context.Context, documents []search.Document) ([]BulkOutcome, error) {
	if len(documents) == 0 {
		return nil, nil
	}
	texts := make([]string, len(documents))
	for n, doc := range documents {
		texts[n] = doc.Title + " " + doc.Body + " " + strings.Join(doc.Tags, " ")
	}
	vectors, err := b.embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	var payload []byte
	for n, doc := range documents {
		meta, err := json.Marshal(map[string]any{"index": map[string]string{"_index": b.Index, "_id": doc.ID}})
		if err != nil {
			return nil, err
		}
		document := map[string]any{
			"id": doc.ID, "title": doc.Title, "body": doc.Body,
			"tags": doc.Tags, "embedding": vectors[n],
		}
		body, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		payload = append(payload, meta...)
		payload = append(payload, '\n')
		payload = append(payload, body...)
		payload = append(payload, '\n')
	}
	response, err := b.Client.Bulk(ctx, payload)
	if err != nil {
		return nil, err
	}
	if len(response.Items) != len(documents) {
		return nil, fmt.Errorf("opensearch: bulk returned %d items for %d documents", len(response.Items), len(documents))
	}
	outcomes := make([]BulkOutcome, len(documents))
	indexed := int64(0)
	for n, item := range response.Items {
		outcomes[n] = BulkOutcome{ID: documents[n].ID, Status: item.Index.Status, Error: strings.TrimSpace(string(item.Index.Error))}
		if item.Index.Status >= 200 && item.Index.Status < 300 {
			indexed++
		}
	}
	b.count.Add(indexed)
	return outcomes, nil
}

func (b *Backend) embed(ctx context.Context, texts []string) ([][]float64, error) {
	if b.Embedder == nil {
		vectors := make([][]float64, len(texts))
		for n, text := range texts {
			vectors[n] = search.EmbedText(text)
		}
		return vectors, nil
	}
	vectors, err := b.Embedder.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vectors) != len(texts) {
		return nil, fmt.Errorf("embedding service returned %d vectors for %d texts", len(vectors), len(texts))
	}
	for n, vector := range vectors {
		if len(vector) != 64 {
			return nil, fmt.Errorf("embedding vector %d has dimension %d, want 64", n, len(vector))
		}
	}
	return vectors, nil
}

func (b *Backend) EnsureIndex(ctx context.Context) error {
	body := []byte(`{"settings":{"index":{"knn":true,"number_of_shards":3,"number_of_replicas":1}},"mappings":{"properties":{"id":{"type":"keyword"},"title":{"type":"text"},"body":{"type":"text"},"tags":{"type":"keyword"},"embedding":{"type":"knn_vector","dimension":64}}}}`)
	_, err := b.Client.do(ctx, "PUT", "/"+b.Index, body)
	if err != nil && !strings.Contains(err.Error(), "resource_already_exists_exception") {
		return err
	}
	return nil
}

func (b *Backend) WaitForIndex(ctx context.Context) error {
	var lastErr error
	for {
		if err := b.EnsureIndex(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if lastErr != nil {
				return fmt.Errorf("wait for OpenSearch index: %w", lastErr)
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *Backend) Ready(ctx context.Context) error {
	if err := b.Client.Ready(ctx); err != nil {
		return err
	}
	if checker, ok := b.Embedder.(interface{ Ready(context.Context) error }); ok {
		return checker.Ready(ctx)
	}
	return nil
}

func (b *Backend) Count() int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count, err := b.Client.Count(ctx, b.Index)
	if err == nil && count >= 0 {
		b.count.Store(count)
	}
	return int(b.count.Load())
}

func (b *Backend) Search(ctx context.Context, query string, from, size int) search.Response {
	started := time.Now()
	if size <= 0 {
		size = 10
	}
	limit := from + size
	if limit < 10 {
		limit = 10
	}
	keywordBody, _ := json.Marshal(map[string]any{
		"from": 0, "size": limit,
		"query": map[string]any{"multi_match": map[string]any{"query": query, "fields": []string{"title^2", "body", "tags"}}},
	})
	response := search.Response{}
	branchCount := 1
	channels := make(chan branchResult, 2)
	go func() { channels <- b.searchBranch(ctx, "bm25", keywordBody) }()
	queryVectors, embedErr := b.embed(ctx, []string{query})
	if embedErr != nil {
		response.Errors = append(response.Errors, "embedding: "+embedErr.Error())
	}
	if embedErr == nil {
		vectorBody, _ := json.Marshal(map[string]any{
			"size":  limit,
			"query": map[string]any{"knn": map[string]any{"embedding": map[string]any{"vector": queryVectors[0], "k": limit}}},
		})
		branchCount = 2
		go func() { channels <- b.searchBranch(ctx, "vector", vectorBody) }()
	}
	var bm25, vector []search.Result
	coverage := search.Coverage{ShardsQueried: 0}
	for n := 0; n < branchCount; n++ {
		select {
		case result := <-channels:
			if result.shards.Total > coverage.ShardsQueried {
				coverage.ShardsQueried = result.shards.Total
			}
			if result.shards.Successful > coverage.ShardsAnswered {
				coverage.ShardsAnswered = result.shards.Successful
			}
			if result.err != nil {
				response.Errors = append(response.Errors, result.name+": "+result.err.Error())
				continue
			}
			if result.name == "bm25" {
				bm25 = result.results
				response.Timings.BM25 = float64(result.elapsed.Microseconds()) / 1000
			} else {
				vector = result.results
				response.Timings.Vector = float64(result.elapsed.Microseconds()) / 1000
			}
		case <-ctx.Done():
			response.Errors = append(response.Errors, "deadline exceeded while querying OpenSearch")
			n = 2
		}
	}
	if coverage.ShardsQueried == 0 {
		coverage.ShardsQueried = 1
	}
	coverage.Complete = coverage.ShardsAnswered == coverage.ShardsQueried && len(response.Errors) == 0
	response.Coverage = coverage
	response.Timings.Parse = float64(time.Since(started).Microseconds()) / 1000
	response.Results = search.FuseResults(bm25, vector)
	if from >= len(response.Results) {
		response.Results = nil
	} else {
		end := from + size
		if end > len(response.Results) {
			end = len(response.Results)
		}
		response.Results = response.Results[from:end]
	}
	response.Timings.Fuse = float64(time.Since(started).Microseconds()) / 1000
	return response
}

type shardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
}

func (b *Backend) searchBranch(ctx context.Context, name string, body []byte) branchResult {
	started := time.Now()
	data, err := b.Client.Search(ctx, b.Index, body)
	result := branchResult{name: name, err: err}
	if err != nil {
		result.elapsed = time.Since(started)
		return result
	}
	var response struct {
		Shards shardsInfo `json:"_shards"`
		Hits   struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Score  float64         `json:"_score"`
				Source search.Document `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		result.err = err
		result.elapsed = time.Since(started)
		return result
	}
	result.shards = response.Shards
	for _, hit := range response.Hits.Hits {
		if hit.Source.ID == "" {
			hit.Source.ID = hit.ID
		}
		result.results = append(result.results, search.Result{Document: hit.Source, Score: hit.Score, Source: name})
	}
	result.elapsed = time.Since(started)
	return result
}

var _ search.Backend = (*Backend)(nil)

func (b *Backend) String() string {
	return strings.TrimSpace(b.Client.BaseURL) + "/" + b.Index + "#" + strconv.Itoa(b.Count())
}
