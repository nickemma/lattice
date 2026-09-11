package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nickemma/lattice/internal/providers/embedding"
	"github.com/nickemma/lattice/internal/search"
)

type Backend struct {
	Client    *Client
	Index     string
	Alias     string
	Embedder  embedding.Embedder
	layout    *indexLayout
	count     atomic.Int64
	indexMu   sync.RWMutex
	writeMu   sync.RWMutex
	dualWrite string
}

type indexLayout struct{ shards, replicas int }

// Index layout used when SetIndexLayout has not been called.
const (
	defaultShards   = 3
	defaultReplicas = 1
)

// SetIndexLayout overrides the shard and replica counts this backend uses when
// it creates an index. Zero replicas is a meaningful value, not "unset": the
// partial-results experiment needs a replica-free index, because with a replica
// available OpenSearch routes around a stopped data node and coverage correctly
// stays complete — which is the wrong experiment. Existing indexes are never
// re-laid-out; this only affects creation.
func (b *Backend) SetIndexLayout(shards, replicas int) {
	if shards < 1 {
		shards = defaultShards
	}
	if replicas < 0 {
		replicas = 0
	}
	b.layout = &indexLayout{shards: shards, replicas: replicas}
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

func NewBackendWithAlias(client *Client, index, alias string) *Backend {
	return &Backend{Client: client, Index: index, Alias: alias}
}

func NewBackendWithAliasAndEmbedder(client *Client, index, alias string, embedder embedding.Embedder) *Backend {
	backend := NewBackendWithAlias(client, index, alias)
	backend.Embedder = embedder
	return backend
}

func (b *Backend) searchIndex() string {
	b.indexMu.RLock()
	defer b.indexMu.RUnlock()
	if b.Alias != "" {
		return b.Alias
	}
	return b.Index
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
	b.writeMu.RLock()
	defer b.writeMu.RUnlock()
	dualWrite := b.dualWrite
	primary, err := b.bulkInto(ctx, b.searchIndex(), documents, vectors)
	if err != nil {
		return nil, err
	}
	if dualWrite != "" {
		secondary, err := b.bulkInto(ctx, dualWrite, documents, vectors)
		if err != nil {
			return nil, err
		}
		for n := range primary {
			if primary[n].Status >= 200 && primary[n].Status < 300 && secondary[n].Status >= 400 {
				primary[n] = secondary[n]
			}
		}
	}
	indexed := int64(0)
	for _, outcome := range primary {
		if outcome.Status >= 200 && outcome.Status < 300 {
			indexed++
		}
	}
	b.count.Add(indexed)
	return primary, nil
}

func (b *Backend) bulkInto(ctx context.Context, index string, documents []search.Document, vectors [][]float64) ([]BulkOutcome, error) {
	var payload []byte
	for n, doc := range documents {
		meta, err := json.Marshal(map[string]any{"index": map[string]string{"_index": index, "_id": doc.ID}})
		if err != nil {
			return nil, err
		}
		document, err := json.Marshal(map[string]any{
			"id": doc.ID, "title": doc.Title, "body": doc.Body,
			"tags": doc.Tags, "embedding": vectors[n],
		})
		if err != nil {
			return nil, err
		}
		payload = append(payload, meta...)
		payload = append(payload, '\n')
		payload = append(payload, document...)
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
	for n, item := range response.Items {
		outcomes[n] = BulkOutcome{ID: documents[n].ID, Status: item.Index.Status, Error: strings.TrimSpace(string(item.Index.Error))}
	}
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
	if b.Alias != "" {
		targets, err := b.Client.AliasTargets(ctx, b.Alias)
		if err == nil && len(targets) > 0 {
			return nil
		}
		if err != nil && !strings.Contains(err.Error(), "404 Not Found") {
			return err
		}
	}
	if err := b.ensureIndex(ctx, b.Index); err != nil {
		return err
	}
	if b.Alias != "" {
		return b.Client.AddAlias(ctx, b.Index, b.Alias)
	}
	return nil
}

func (b *Backend) ensureIndex(ctx context.Context, index string) error {
	shards, replicas := defaultShards, defaultReplicas
	if b.layout != nil {
		shards, replicas = b.layout.shards, b.layout.replicas
	}
	body := fmt.Appendf(nil, `{"settings":{"index":{"knn":true,"number_of_shards":%d,"number_of_replicas":%d}},"mappings":{"properties":{"id":{"type":"keyword"},"title":{"type":"text"},"body":{"type":"text"},"tags":{"type":"keyword"},"embedding":{"type":"knn_vector","dimension":64}}}}`, shards, replicas)
	_, err := b.Client.do(ctx, "PUT", "/"+index, body)
	if err != nil && !strings.Contains(err.Error(), "resource_already_exists_exception") {
		return err
	}
	return nil
}

type ReindexReport struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Alias       string `json:"alias"`
}

func (b *Backend) Reindex(ctx context.Context) (ReindexReport, error) {
	if b.Alias == "" {
		return ReindexReport{}, errors.New("opensearch: reindex requires a configured alias")
	}
	oldIndexes, err := b.Client.AliasTargets(ctx, b.Alias)
	if err != nil || len(oldIndexes) == 0 {
		return ReindexReport{}, fmt.Errorf("opensearch: resolve alias %q: %w", b.Alias, err)
	}
	destination := fmt.Sprintf("%s-%d", b.Index, time.Now().UTC().UnixNano())
	if err := b.ensureIndex(ctx, destination); err != nil {
		return ReindexReport{}, fmt.Errorf("opensearch: create reindex target: %w", err)
	}
	b.writeMu.Lock()
	b.dualWrite = destination
	b.writeMu.Unlock()
	clearDualWrite := func() {
		b.writeMu.Lock()
		b.dualWrite = ""
		b.writeMu.Unlock()
	}
	if err := b.Client.Reindex(ctx, oldIndexes[0], destination); err != nil {
		clearDualWrite()
		return ReindexReport{}, fmt.Errorf("opensearch: reindex: %w", err)
	}
	b.writeMu.Lock()
	err = b.Client.SwapAlias(ctx, b.Alias, destination, oldIndexes)
	b.dualWrite = ""
	b.writeMu.Unlock()
	if err != nil {
		return ReindexReport{}, fmt.Errorf("opensearch: swap alias: %w", err)
	}
	return ReindexReport{Source: oldIndexes[0], Destination: destination, Alias: b.Alias}, nil
}

func (b *Backend) Snapshot(ctx context.Context, repository, snapshot string) error {
	if strings.TrimSpace(repository) == "" || strings.TrimSpace(snapshot) == "" {
		return errors.New("opensearch: repository and snapshot are required")
	}
	return b.Client.Snapshot(ctx, repository, snapshot, b.searchIndex())
}

func (b *Backend) Restore(ctx context.Context, repository, snapshot, sourceIndex, targetIndex string) error {
	if strings.TrimSpace(repository) == "" || strings.TrimSpace(snapshot) == "" {
		return errors.New("opensearch: repository and snapshot are required")
	}
	if b.Alias != "" && targetIndex == "" {
		return errors.New("opensearch: target index is required when restoring behind an alias")
	}
	if sourceIndex == "" && targetIndex == "" {
		sourceIndex = b.Index
	}
	if err := b.Client.Restore(ctx, repository, snapshot, sourceIndex, targetIndex); err != nil {
		return err
	}
	if b.Alias != "" {
		// A snapshot may carry alias metadata even with global state excluded.
		// Resolve the alias after restore so the swap removes both the previous
		// generation and any alias that OpenSearch restored on the target.
		currentIndexes, err := b.Client.AliasTargets(ctx, b.Alias)
		if err == nil && len(currentIndexes) > 0 {
			return b.Client.SwapAlias(ctx, b.Alias, targetIndex, currentIndexes)
		}
		if err != nil && !strings.Contains(err.Error(), "404 Not Found") {
			return err
		}
		return b.Client.AddAlias(ctx, targetIndex, b.Alias)
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
	if b.Alias != "" {
		targets, err := b.Client.AliasTargets(ctx, b.Alias)
		if err != nil {
			return fmt.Errorf("opensearch alias %q: %w", b.Alias, err)
		}
		if len(targets) == 0 {
			return fmt.Errorf("opensearch alias %q has no target", b.Alias)
		}
	}
	if checker, ok := b.Embedder.(interface{ Ready(context.Context) error }); ok {
		return checker.Ready(ctx)
	}
	return nil
}

func (b *Backend) Count() int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count, err := b.Client.Count(ctx, b.searchIndex())
	if err == nil && count >= 0 {
		b.count.Store(count)
	}
	return int(b.count.Load())
}

func (b *Backend) Search(ctx context.Context, query string, from, size int) search.Response {
	return b.SearchWithFilters(ctx, query, nil, from, size)
}

func (b *Backend) SearchWithFilters(ctx context.Context, query string, tags []string, from, size int) search.Response {
	return b.searchWithFilters(ctx, query, tags, from, size, true)
}

func (b *Backend) SearchWithFiltersCursor(ctx context.Context, query string, tags []string, token string, size int) search.Response {
	cursor, err := search.DecodeCursor(token)
	if err != nil {
		return search.Response{Coverage: search.RejectedCoverage(), Errors: []string{err.Error()}}
	}
	if cursor.Query != query || cursor.Size != size || !sameTags(cursor.Tags, tags) {
		return search.Response{Coverage: search.RejectedCoverage(), Errors: []string{"cursor does not match query, filters, or page size"}}
	}
	return b.searchWithFilters(ctx, query, tags, cursor.Offset, size, true)
}

func (b *Backend) searchWithFilters(ctx context.Context, query string, tags []string, from, size int, cursorEnabled bool) search.Response {
	started := time.Now()
	if size <= 0 {
		size = 10
	}
	limit := from + size
	if limit < 10 {
		limit = 10
	}
	keywordQuery := map[string]any{"multi_match": map[string]any{"query": query, "fields": []string{"title^2", "body", "tags"}}}
	if len(tags) > 0 {
		filters := make([]any, 0, len(tags))
		for _, tag := range tags {
			filters = append(filters, map[string]any{"term": map[string]any{"tags": tag}})
		}
		keywordQuery = map[string]any{"bool": map[string]any{
			"must":   []any{keywordQuery},
			"filter": filters,
		}}
	}
	keywordBody, _ := json.Marshal(map[string]any{"from": 0, "size": limit, "query": keywordQuery})
	response := search.Response{}
	branchCount := 1
	channels := make(chan branchResult, 2)
	go func() { channels <- b.searchBranch(ctx, "bm25", keywordBody) }()
	queryVectors, embedErr := b.embed(ctx, []string{query})
	if embedErr != nil {
		response.Errors = append(response.Errors, "embedding: "+embedErr.Error())
	}
	if embedErr == nil {
		knn := map[string]any{"vector": queryVectors[0], "k": limit}
		if len(tags) > 0 {
			filters := make([]any, 0, len(tags))
			for _, tag := range tags {
				filters = append(filters, map[string]any{"term": map[string]any{"tags": tag}})
			}
			knn["filter"] = map[string]any{"bool": map[string]any{"filter": filters}}
		}
		vectorBody, _ := json.Marshal(map[string]any{
			"size":  limit,
			"query": map[string]any{"knn": map[string]any{"embedding": knn}},
		})
		branchCount = 2
		go func() { channels <- b.searchBranch(ctx, "vector", vectorBody) }()
	}
	var bm25, vector []search.Result
	// Coverage is collected as raw observations and classified in one place by
	// search.EvaluateCoverage. A branch that errored carries no shard
	// accounting, so it reports a dependency failure and contributes nothing to
	// the shard numbers.
	//
	// Answered is the minimum successful count across the branches that did
	// report, not the maximum. If the keyword branch saw 3/3 while the vector
	// branch saw 2/3 because a data node is down, the response really is
	// missing a shard's worth of vector candidates, and taking the maximum
	// would hide exactly the event this field exists to expose.
	signals := search.CoverageSignals{DependencyFailed: embedErr != nil}
	answered := -1
gather:
	for n := 0; n < branchCount; n++ {
		select {
		case result := <-channels:
			if result.err != nil {
				signals.DependencyFailed = true
				response.Errors = append(response.Errors, result.name+": "+result.err.Error())
				continue
			}
			signals.ShardsObserved = true
			if result.shards.Total > signals.ShardsQueried {
				signals.ShardsQueried = result.shards.Total
			}
			if answered < 0 || result.shards.Successful < answered {
				answered = result.shards.Successful
			}
			if result.name == "bm25" {
				bm25 = result.results
				response.Timings.BM25 = float64(result.elapsed.Microseconds()) / 1000
			} else {
				vector = result.results
				response.Timings.Vector = float64(result.elapsed.Microseconds()) / 1000
			}
		case <-ctx.Done():
			signals.DeadlineExceeded = true
			response.Errors = append(response.Errors, "deadline exceeded while querying OpenSearch")
			break gather
		}
	}
	if answered > 0 {
		signals.ShardsAnswered = answered
	}
	if signals.ShardsQueried == 0 {
		signals.ShardsQueried = 1
	}
	response.Coverage = search.EvaluateCoverage(signals)
	response.Timings.Parse = float64(time.Since(started).Microseconds()) / 1000
	response.Results = search.FuseResults(bm25, vector)
	totalResults := len(response.Results)
	if from >= totalResults {
		response.Results = nil
	} else {
		end := from + size
		if end > totalResults {
			end = totalResults
		}
		response.Results = response.Results[from:end]
		if cursorEnabled && end < totalResults && response.Coverage.Usable() {
			response.NextCursor = search.EncodeCursor(search.Cursor{Query: query, Tags: append([]string(nil), tags...), Size: size, Offset: end})
		}
	}
	response.Timings.Fuse = float64(time.Since(started).Microseconds()) / 1000
	return response
}

func sameTags(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for n := range left {
		if left[n] != right[n] {
			return false
		}
	}
	return true
}

type shardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
}

func (b *Backend) searchBranch(ctx context.Context, name string, body []byte) branchResult {
	started := time.Now()
	data, err := b.Client.Search(ctx, b.searchIndex(), body)
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
