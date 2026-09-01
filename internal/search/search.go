package search

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nickemma/lattice/pkg/lsm"
)

const vectorDimensions = 64

type Document struct {
	ID    string   `json:"id"`
	Title string   `json:"title"`
	Body  string   `json:"body"`
	Tags  []string `json:"tags,omitempty"`
}

type Result struct {
	Document Document `json:"document"`
	Score    float64  `json:"score"`
	Source   string   `json:"source"`
}

type Coverage struct {
	ShardsQueried  int  `json:"shards_queried"`
	ShardsAnswered int  `json:"shards_answered"`
	Complete       bool `json:"complete"`
}

type Timings struct {
	Parse  float64 `json:"parse"`
	BM25   float64 `json:"bm25"`
	Vector float64 `json:"vector"`
	Fuse   float64 `json:"fuse"`
}

type Response struct {
	Results    []Result `json:"results"`
	NextCursor string   `json:"next_cursor,omitempty"`
	Coverage   Coverage `json:"coverage"`
	Timings    Timings  `json:"timings_ms"`
	Errors     []string `json:"errors,omitempty"`
	CacheHit   bool     `json:"cache_hit,omitempty"`
}

type Backend interface {
	Upsert(Document) error
	Count() int
	Search(context.Context, string, int, int) Response
}

// FilteredBackend is an optional extension used when a backend can apply
// structured filters at the shard rather than filtering results in the API.
// Backend remains intentionally small so teaching and test doubles stay easy
// to implement.
type FilteredBackend interface {
	SearchWithFilters(context.Context, string, []string, int, int) Response
}

// CursorBackend is the continuation form of FilteredBackend. The token is
// opaque to callers; the backend owns how it resumes a stable result stream.
type CursorBackend interface {
	SearchWithFiltersCursor(context.Context, string, []string, string, int) Response
}

type Cursor struct {
	Query  string   `json:"query"`
	Tags   []string `json:"tags,omitempty"`
	Size   int      `json:"size"`
	Offset int      `json:"offset"`
}

func EncodeCursor(cursor Cursor) string {
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func DecodeCursor(token string) (Cursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor encoding")
	}
	var cursor Cursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor payload")
	}
	if cursor.Size < 1 || cursor.Size > 100 || cursor.Offset < 0 || cursor.Query == "" {
		return Cursor{}, fmt.Errorf("invalid cursor values")
	}
	return cursor, nil
}

func EmbedText(text string) []float64 { return embed(text) }

func FuseResults(bm25, vector []Result) []Result { return fuse(bm25, vector) }

type Snapshot struct {
	CreatedAt time.Time  `json:"created_at"`
	Documents []Document `json:"documents"`
}

type Index struct {
	mu          sync.RWMutex
	shards      []map[string]Document
	available   []bool
	delays      []time.Duration
	shardCount  int
	dataDir     string
	stores      []*lsm.Store
	cache       map[string]Response
	cacheExpiry map[string]time.Time
}

func NewIndex(shards int) *Index {
	if shards < 1 {
		shards = 1
	}
	i := &Index{shardCount: shards, cache: make(map[string]Response), cacheExpiry: make(map[string]time.Time)}
	i.shards = make([]map[string]Document, shards)
	i.available = make([]bool, shards)
	i.delays = make([]time.Duration, shards)
	for n := range i.shards {
		i.shards[n] = make(map[string]Document)
		i.available[n] = true
	}
	return i
}

func OpenPersistentIndex(dir string, shards int) (*Index, error) {
	i := NewIndex(shards)
	i.dataDir = dir
	i.stores = make([]*lsm.Store, shards)
	for shardID := 0; shardID < shards; shardID++ {
		store, err := lsm.Open(filepath.Join(dir, fmt.Sprintf("shard-%d", shardID)))
		if err != nil {
			_ = i.Close()
			return nil, err
		}
		i.stores[shardID] = store
		values, err := store.Scan("")
		if err != nil {
			_ = i.Close()
			return nil, err
		}
		for id, value := range values {
			var doc Document
			if err := json.Unmarshal(value, &doc); err != nil {
				_ = i.Close()
				return nil, fmt.Errorf("search: recover %s: %w", id, err)
			}
			i.shards[shardID][id] = doc
		}
	}
	return i, nil
}

func (i *Index) SnapshotPath() string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.dataDir == "" {
		return filepath.Join(".lattice-data", "snapshot.json")
	}
	return filepath.Join(i.dataDir, "snapshot.json")
}

func (i *Index) Upsert(doc Document) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	shard := i.shard(doc.ID)
	if shard < 0 || shard >= len(i.shards) {
		return fmt.Errorf("search: invalid shard for %q", doc.ID)
	}
	if len(i.stores) > shard && i.stores[shard] != nil {
		encoded, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		if err := i.stores[shard].Put(doc.ID, encoded); err != nil {
			return err
		}
	}
	i.shards[shard][doc.ID] = doc
	i.cache = make(map[string]Response)
	i.cacheExpiry = make(map[string]time.Time)
	return nil
}

func (i *Index) Delete(id string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	shard := i.shard(id)
	if len(i.stores) > shard && i.stores[shard] != nil {
		if err := i.stores[shard].Delete(id); err != nil {
			return err
		}
	}
	delete(i.shards[shard], id)
	i.cache = make(map[string]Response)
	i.cacheExpiry = make(map[string]time.Time)
	return nil
}

func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, store := range i.stores {
		if store != nil {
			if err := store.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Index) Count() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.CountLocked()
}

func (i *Index) CountLocked() int {
	total := 0
	for _, shard := range i.shards {
		total += len(shard)
	}
	return total
}

func (i *Index) Documents() []Document {
	i.mu.RLock()
	defer i.mu.RUnlock()
	result := make([]Document, 0, i.CountLocked())
	for _, shard := range i.shards {
		for _, doc := range shard {
			result = append(result, doc)
		}
	}
	sort.Slice(result, func(a, b int) bool { return result[a].ID < result[b].ID })
	return result
}

func (i *Index) Reindex() error { return i.Replace(i.Documents()) }

func (i *Index) Replace(documents []Document) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	old := make(map[string]struct{})
	for _, shard := range i.shards {
		for id := range shard {
			old[id] = struct{}{}
		}
	}
	for _, doc := range documents {
		if doc.ID == "" || doc.Title == "" || doc.Body == "" {
			return fmt.Errorf("search: invalid document %q during replacement", doc.ID)
		}
		delete(old, doc.ID)
	}
	for id := range old {
		shard := i.shard(id)
		if len(i.stores) > shard && i.stores[shard] != nil {
			if err := i.stores[shard].Delete(id); err != nil {
				return err
			}
		}
	}
	newShards := make([]map[string]Document, len(i.shards))
	for shardID := range newShards {
		newShards[shardID] = make(map[string]Document)
	}
	for _, doc := range documents {
		shard := i.shard(doc.ID)
		if len(i.stores) > shard && i.stores[shard] != nil {
			encoded, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			if err := i.stores[shard].Put(doc.ID, encoded); err != nil {
				return err
			}
		}
		newShards[shard][doc.ID] = doc
	}
	i.shards = newShards
	i.cache = make(map[string]Response)
	i.cacheExpiry = make(map[string]time.Time)
	return nil
}

func (i *Index) Snapshot(path string) error {
	snapshot := Snapshot{CreatedAt: time.Now().UTC(), Documents: i.Documents()}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".lattice-snapshot-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

func (i *Index) Restore(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return 0, err
	}
	if err := i.Replace(snapshot.Documents); err != nil {
		return 0, err
	}
	return len(snapshot.Documents), nil
}

func (i *Index) SetShardAvailable(shard int, available bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if shard >= 0 && shard < len(i.available) {
		i.available[shard] = available
		i.cache = make(map[string]Response)
		i.cacheExpiry = make(map[string]time.Time)
	}
}

func (i *Index) SetShardDelay(shard int, delay time.Duration) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if shard >= 0 && shard < len(i.delays) {
		i.delays[shard] = delay
		i.cache = make(map[string]Response)
		i.cacheExpiry = make(map[string]time.Time)
	}
}

func (i *Index) Search(ctx context.Context, query string, from, size int) Response {
	return i.SearchWithFilters(ctx, query, nil, from, size)
}

func (i *Index) SearchWithFilters(ctx context.Context, query string, tags []string, from, size int) Response {
	return i.searchWithFilters(ctx, query, tags, from, size, true)
}

func (i *Index) SearchWithFiltersCursor(ctx context.Context, query string, tags []string, token string, size int) Response {
	cursor, err := DecodeCursor(token)
	if err != nil {
		return Response{Errors: []string{err.Error()}}
	}
	if cursor.Query != query || !sameTags(cursor.Tags, tags) || cursor.Size != size {
		return Response{Errors: []string{"cursor does not match query, filters, or page size"}}
	}
	return i.searchWithFilters(ctx, query, tags, cursor.Offset, size, true)
}

func (i *Index) searchWithFilters(ctx context.Context, query string, tags []string, from, size int, cursorEnabled bool) Response {
	started := time.Now()
	if size <= 0 {
		size = 10
	}
	if from < 0 {
		from = 0
	}
	cacheKey := query + "\x00" + strings.Join(tags, "\x1f") + "\x00" + string(rune(from)) + "\x00" + string(rune(size))
	i.mu.RLock()
	if cached, ok := i.cache[cacheKey]; ok && time.Now().Before(i.cacheExpiry[cacheKey]) {
		i.mu.RUnlock()
		cached.CacheHit = true
		return cached
	}
	tokens := tokenize(query)
	available := append([]bool(nil), i.available...)
	delays := append([]time.Duration(nil), i.delays...)
	shards := make([]map[string]Document, len(i.shards))
	for n, shard := range i.shards {
		shards[n] = make(map[string]Document, len(shard))
		for id, doc := range shard {
			if matchesTags(doc, tags) {
				shards[n][id] = doc
			}
		}
	}
	i.mu.RUnlock()

	resp := Response{Coverage: Coverage{ShardsQueried: len(shards)}}
	type branchResult struct {
		shard       int
		bm25        []Result
		vec         []Result
		bm25Elapsed time.Duration
		vecElapsed  time.Duration
	}
	results := make(chan branchResult, len(shards))
	for shardID := range shards {
		go func(shardID int) {
			if !available[shardID] {
				return
			}
			timer := time.NewTimer(delays[shardID])
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			branchStarted := time.Now()
			bm25 := i.bm25(tokens, shards[shardID])
			bm25Elapsed := time.Since(branchStarted)
			vectorStarted := time.Now()
			vec := i.vector(query, shards[shardID])
			results <- branchResult{shard: shardID, bm25: bm25, vec: vec, bm25Elapsed: bm25Elapsed, vecElapsed: time.Since(vectorStarted)}
		}(shardID)
	}
	allBM25 := make([]Result, 0)
	allVector := make([]Result, 0)
	answered := 0
	var bm25Elapsed, vectorElapsed time.Duration
	expected := 0
	for _, isAvailable := range available {
		if isAvailable {
			expected++
		}
	}
	for n := 0; n < expected; n++ {
		select {
		case result := <-results:
			answered++
			if result.bm25Elapsed > bm25Elapsed {
				bm25Elapsed = result.bm25Elapsed
			}
			if result.vecElapsed > vectorElapsed {
				vectorElapsed = result.vecElapsed
			}
			allBM25 = append(allBM25, result.bm25...)
			allVector = append(allVector, result.vec...)
		case <-ctx.Done():
			resp.Errors = append(resp.Errors, "deadline exceeded while gathering shards")
			n = len(shards)
		}
	}
	resp.Coverage.ShardsAnswered = answered
	resp.Coverage.Complete = answered == len(shards)
	resp.Timings.Parse = float64(time.Since(started).Microseconds()) / 1000
	sortResults(allBM25)
	sortResults(allVector)
	resp.Timings.BM25 = float64(bm25Elapsed.Microseconds()) / 1000
	resp.Timings.Vector = float64(vectorElapsed.Microseconds()) / 1000
	resp.Results = fuse(allBM25, allVector)
	totalResults := len(resp.Results)
	if from >= totalResults {
		resp.Results = nil
	} else {
		end := from + size
		if end > totalResults {
			end = totalResults
		}
		resp.Results = resp.Results[from:end]
		if cursorEnabled && end < totalResults && resp.Coverage.Complete && len(resp.Errors) == 0 {
			resp.NextCursor = EncodeCursor(Cursor{Query: query, Tags: append([]string(nil), tags...), Size: size, Offset: end})
		}
	}
	resp.Timings.Fuse = float64(time.Since(started).Microseconds()) / 1000
	if resp.Coverage.Complete && len(resp.Errors) == 0 {
		i.mu.Lock()
		i.cache[cacheKey] = resp
		i.cacheExpiry[cacheKey] = time.Now().Add(30 * time.Second)
		i.mu.Unlock()
	}
	return resp
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

func matchesTags(doc Document, required []string) bool {
	if len(required) == 0 {
		return true
	}
	seen := make(map[string]struct{}, len(doc.Tags))
	for _, tag := range doc.Tags {
		seen[strings.ToLower(tag)] = struct{}{}
	}
	for _, tag := range required {
		if _, ok := seen[strings.ToLower(strings.TrimSpace(tag))]; !ok {
			return false
		}
	}
	return true
}

func (i *Index) shard(id string) int {
	var value uint64
	for n := 0; n < len(id); n++ {
		value = value*131 + uint64(id[n])
	}
	return int(value % uint64(i.shardCount))
}

func (i *Index) bm25(tokens []string, docs map[string]Document) []Result {
	if len(tokens) == 0 {
		return nil
	}
	result := make([]Result, 0)
	for _, doc := range docs {
		text := tokenize(doc.Title + " " + doc.Body + " " + strings.Join(doc.Tags, " "))
		counts := map[string]int{}
		for _, token := range text {
			counts[token]++
		}
		score := 0.0
		for _, token := range tokens {
			if counts[token] > 0 {
				score += 1 + math.Log1p(float64(counts[token]))
			}
		}
		if score > 0 {
			result = append(result, Result{Document: doc, Score: score, Source: "bm25"})
		}
	}
	return result
}

func (i *Index) vector(query string, docs map[string]Document) []Result {
	queryVector := embed(query)
	result := make([]Result, 0, len(docs))
	for _, doc := range docs {
		score := cosine(queryVector, embed(doc.Title+" "+doc.Body+" "+strings.Join(doc.Tags, " ")))
		if score > 0 {
			result = append(result, Result{Document: doc, Score: score, Source: "vector"})
		}
	}
	return result
}

func fuse(bm25, vector []Result) []Result {
	scores := map[string]float64{}
	docs := map[string]Document{}
	for rank, result := range bm25 {
		scores[result.Document.ID] += 1.0 / float64(60+rank+1)
		docs[result.Document.ID] = result.Document
	}
	for rank, result := range vector {
		scores[result.Document.ID] += 1.0 / float64(60+rank+1)
		docs[result.Document.ID] = result.Document
	}
	result := make([]Result, 0, len(scores))
	for id, score := range scores {
		result = append(result, Result{Document: docs[id], Score: score, Source: "hybrid"})
	}
	sortResults(result)
	return result
}

func sortResults(results []Result) {
	sort.SliceStable(results, func(a, b int) bool {
		if results[a].Score == results[b].Score {
			return results[a].Document.ID < results[b].Document.ID
		}
		return results[a].Score > results[b].Score
	})
}

func tokenize(text string) []string {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
	return words
}

func embed(text string) []float64 {
	vector := make([]float64, vectorDimensions)
	for n, token := range tokenize(text) {
		var hash uint64 = 1469598103934665603
		for _, b := range token {
			hash ^= uint64(b)
			hash *= 1099511628211
		}
		vector[(int(hash%vectorDimensions)+n)%vectorDimensions] += 1
	}
	return vector
}

func cosine(a, b []float64) float64 {
	var dot, aa, bb float64
	for n := range a {
		dot += a[n] * b[n]
		aa += a[n] * a[n]
		bb += b[n] * b[n]
	}
	if aa == 0 || bb == 0 {
		return 0
	}
	return dot / math.Sqrt(aa*bb)
}
