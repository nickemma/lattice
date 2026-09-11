package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/nickemma/lattice/internal/ingest"
	"github.com/nickemma/lattice/internal/providers/opensearch"
	"github.com/nickemma/lattice/internal/search"
)

type Server struct {
	Index        *search.Index
	Backend      search.Backend
	Publisher    Publisher
	Broker       *ingest.Broker
	Indexer      *ingest.Indexer
	Cache        Cache
	apiKey       string
	querySlots   chan struct{}
	mode         string
	queries      atomic.Uint64
	docs         atomic.Uint64
	incomplete   atomic.Uint64
	alias        atomic.Uint64
	cacheHits    atomic.Uint64
	cacheMisses  atomic.Uint64
	queryNanos   atomic.Uint64
	queryBuckets [6]atomic.Uint64
	degraded     atomic.Uint64
	reasons      map[string]*atomic.Uint64
}

// coverageReasons is the fixed label set for lattice_query_coverage_reason_total.
// Every reason is always exported, including at zero, so a dashboard can tell
// "no deadline expiries" from "this build does not report deadline expiries".
var coverageReasons = []string{
	search.ReasonComplete,
	search.ReasonDeadline,
	search.ReasonShardUnavailable,
	search.ReasonDependencyError,
	search.ReasonInvalidRequest,
}

var requestSequence atomic.Uint64

type Publisher interface {
	Publish(context.Context, []byte, []byte) error
}

type Cache interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}

type cacheInvalidator interface {
	Clear(context.Context) error
}

func New(index *search.Index) *Server {
	broker := ingest.NewBroker()
	return newServer(index, broker, "local-index")
}

func NewWithBackend(backend search.Backend) *Server {
	broker := ingest.NewBroker()
	return newServer(backend, broker, "configured-backend")
}

func newServer(backend search.Backend, broker *ingest.Broker, mode string) *Server {
	server := &Server{Backend: backend, Broker: broker, Indexer: ingest.NewIndexer(broker, backend), mode: mode, apiKey: os.Getenv("LATTICE_API_KEY")}
	server.reasons = make(map[string]*atomic.Uint64, len(coverageReasons))
	for _, reason := range coverageReasons {
		server.reasons[reason] = new(atomic.Uint64)
	}
	if value := os.Getenv("LATTICE_MAX_INFLIGHT_QUERIES"); value != "" {
		if limit, err := strconv.Atoi(value); err == nil && limit > 0 {
			server.querySlots = make(chan struct{}, limit)
		}
	}
	if index, ok := backend.(*search.Index); ok {
		server.Index = index
	}
	return server
}

// NewWithAPIKey is useful for embedding the service and for tests. The
// executable normally obtains the key from LATTICE_API_KEY.
func NewWithAPIKey(index *search.Index, apiKey string) *Server {
	server := New(index)
	server.apiKey = apiKey
	return server
}

func NewWithBackendAndPublisher(backend search.Backend, publisher Publisher) *Server {
	return NewWithBackendAndPublisherAndCache(backend, publisher, nil)
}

func NewWithBackendAndPublisherAndCache(backend search.Backend, publisher Publisher, cache Cache) *Server {
	server := NewWithBackend(backend)
	server.Publisher = publisher
	server.Cache = cache
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/openapi.yaml", openapi)
	mux.HandleFunc("/docs", swagger)
	mux.HandleFunc("/playground", playground)
	mux.HandleFunc("/v1/documents", s.documents)
	mux.HandleFunc("/v1/search", s.search)
	mux.HandleFunc("/v1/debug/shards/", s.debugShard)
	mux.HandleFunc("/v1/admin/reindex", s.reindex)
	mux.HandleFunc("/v1/admin/snapshot", s.snapshot)
	mux.HandleFunc("/v1/admin/restore", s.restore)
	return requestLog(s.authenticate(mux))
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" || !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		provided := r.Header.Get("X-API-Key")
		if len(provided) != len(s.apiKey) || subtle.ConstantTimeCompare([]byte(provided), []byte(s.apiKey)) != 1 {
			w.Header().Set("WWW-Authenticate", `ApiKey realm="lattice"`)
			writeError(w, http.StatusUnauthorized, "valid X-API-Key is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	redisDependency := map[string]any{"status": "ready", "mode": "local-cache"}
	if s.Cache != nil {
		redisDependency["mode"] = "redis"
	}
	dependencies := map[string]any{
		"kafka":      map[string]any{"status": "ready", "mode": "local-broker"},
		"opensearch": map[string]any{"status": "ready", "mode": s.mode, "documents": s.Backend.Count()},
		"redis":      redisDependency,
		"embedding":  map[string]any{"status": "ready", "mode": "deterministic-local"},
	}
	ready := true
	if checker, ok := s.Backend.(interface{ Ready(context.Context) error }); ok {
		if err := checker.Ready(ctx); err != nil {
			ready = false
			dependencies["opensearch"] = map[string]any{"status": "not_ready", "mode": s.mode, "error": err.Error()}
		}
	}
	if checker, ok := s.Publisher.(interface{ Ready(context.Context) error }); ok {
		if err := checker.Ready(ctx); err != nil {
			ready = false
			dependencies["kafka"] = map[string]any{"status": "not_ready", "mode": "kafka", "error": err.Error()}
		}
	}
	if checker, ok := s.Cache.(interface{ Ready(context.Context) error }); ok {
		if err := checker.Ready(ctx); err != nil {
			ready = false
			dependencies["redis"] = map[string]any{"status": "not_ready", "mode": "redis", "error": err.Error()}
		}
	}
	status := "ready"
	code := http.StatusOK
	if !ready {
		status = "not_ready"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"status": status, "dependencies": dependencies})
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE lattice_query_requests_total counter\nlattice_query_requests_total %d\n", s.queries.Load())
	fmt.Fprintf(w, "# TYPE lattice_ingest_documents_total counter\nlattice_ingest_documents_total %d\n", s.docs.Load())
	fmt.Fprintf(w, "# TYPE lattice_index_documents gauge\nlattice_index_documents %d\n", s.Backend.Count())
	// Incomplete and degraded are deliberately separate series. Incomplete
	// counts responses that lost shard coverage; degraded counts responses that
	// carried an error. A response can be in either, both, or neither.
	fmt.Fprintf(w, "# TYPE lattice_query_incomplete_total counter\nlattice_query_incomplete_total %d\n", s.incomplete.Load())
	fmt.Fprintf(w, "# TYPE lattice_query_degraded_total counter\nlattice_query_degraded_total %d\n", s.degraded.Load())
	fmt.Fprintln(w, "# TYPE lattice_query_coverage_reason_total counter")
	for _, reason := range coverageReasons {
		fmt.Fprintf(w, "lattice_query_coverage_reason_total{reason=\"%s\"} %d\n", reason, s.reasons[reason].Load())
	}
	fmt.Fprintf(w, "# TYPE lattice_query_cache_hits_total counter\nlattice_query_cache_hits_total %d\n", s.cacheHits.Load())
	fmt.Fprintf(w, "# TYPE lattice_query_cache_misses_total counter\nlattice_query_cache_misses_total %d\n", s.cacheMisses.Load())
	fmt.Fprintln(w, "# TYPE lattice_query_duration_seconds histogram")
	limits := [...]string{"0.01", "0.05", "0.1", "0.2", "0.4", "+Inf"}
	for n, limit := range limits {
		fmt.Fprintf(w, "lattice_query_duration_seconds_bucket{le=\"%s\"} %d\n", limit, s.queryBuckets[n].Load())
	}
	fmt.Fprintf(w, "lattice_query_duration_seconds_sum %.9f\nlattice_query_duration_seconds_count %d\n", float64(s.queryNanos.Load())/1e9, s.queries.Load())
	if s.Indexer != nil {
		fmt.Fprintf(w, "# TYPE lattice_ingest_dlq_total counter\nlattice_ingest_dlq_total %d\n", len(s.Indexer.DLQ()))
	} else {
		fmt.Fprintln(w, "# TYPE lattice_ingest_dlq_total counter\nlattice_ingest_dlq_total 0")
	}
}

func (s *Server) documents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var doc search.Document
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid document: "+err.Error())
		return
	}
	if doc.ID == "" || doc.Title == "" || doc.Body == "" {
		writeError(w, http.StatusBadRequest, "id, title, and body are required")
		return
	}
	if s.Publisher != nil {
		payload, err := json.Marshal(doc)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "encode document: "+err.Error())
			return
		}
		if err := s.Publisher.Publish(r.Context(), []byte(doc.ID), payload); err != nil {
			writeError(w, http.StatusBadGateway, "publish document: "+err.Error())
			return
		}
		s.docs.Add(1)
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "id": doc.ID, "streamed": true})
		return
	}
	s.Broker.Publish(doc)
	s.Indexer.ProcessAvailable()
	s.docs.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "id": doc.ID})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method must be GET")
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeError(w, http.StatusBadRequest, "q is required")
		return
	}
	from, size, err := parsePaging(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	searchAfter := strings.TrimSpace(r.URL.Query().Get("search_after"))
	if searchAfter != "" {
		cursor, cursorErr := search.DecodeCursor(searchAfter)
		if cursorErr != nil {
			writeError(w, http.StatusBadRequest, cursorErr.Error())
			return
		}
		if r.URL.Query().Get("from") != "" && from != 0 {
			writeError(w, http.StatusBadRequest, "from cannot be combined with search_after")
			return
		}
		if cursor.Query != query || cursor.Size != size || !sameSearchTags(cursor.Tags, normalizedTags(r.URL.Query()["tag"])) {
			writeError(w, http.StatusBadRequest, "search_after does not match query, filters, or page size")
			return
		}
		from = cursor.Offset
	}
	deadline := 200 * time.Millisecond
	if value := r.URL.Query().Get("deadline"); value != "" {
		deadline, err = time.ParseDuration(value)
		if err != nil || deadline <= 0 || deadline > 30*time.Second {
			writeError(w, http.StatusBadRequest, "deadline must be a positive duration no greater than 30s")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), deadline)
	defer cancel()
	if s.querySlots != nil {
		select {
		case s.querySlots <- struct{}{}:
			defer func() { <-s.querySlots }()
		default:
			writeError(w, http.StatusTooManyRequests, "query concurrency limit reached")
			return
		}
	}
	tags := normalizedTags(r.URL.Query()["tag"])
	s.queries.Add(1)
	started := time.Now()
	// The remote index is updated asynchronously by the Kafka consumer. Include
	// the current document count so a response cached before an ingest cannot
	// remain authoritative after the index changes.
	cacheKey := fmt.Sprintf("lattice:query:%d:%d:%d:%s:%s", s.Backend.Count(), from, size, query, strings.Join(tags, ","))
	if s.Cache != nil {
		if payload, ok, cacheErr := s.Cache.Get(ctx, cacheKey); cacheErr == nil && ok {
			var cached search.Response
			if json.Unmarshal(payload, &cached) == nil {
				cached.CacheHit = true
				s.observeQuery(time.Since(started))
				s.observeCoverage(cached.Coverage)
				s.cacheHits.Add(1)
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}
	var response search.Response
	if searchAfter != "" {
		if cursorBackend, ok := s.Backend.(search.CursorBackend); ok {
			response = cursorBackend.SearchWithFiltersCursor(ctx, query, tags, searchAfter, size)
		} else if filtered, ok := s.Backend.(search.FilteredBackend); ok {
			response = filtered.SearchWithFilters(ctx, query, tags, from, size)
		} else {
			response = s.Backend.Search(ctx, query, from, size)
		}
	} else if filtered, ok := s.Backend.(search.FilteredBackend); ok {
		response = filtered.SearchWithFilters(ctx, query, tags, from, size)
	} else {
		response = s.Backend.Search(ctx, query, from, size)
	}
	s.observeQuery(time.Since(started))
	if response.CacheHit {
		s.cacheHits.Add(1)
	} else {
		s.cacheMisses.Add(1)
	}
	if s.Cache != nil && response.Coverage.Usable() {
		if payload, marshalErr := json.Marshal(response); marshalErr == nil {
			_ = s.Cache.Set(context.Background(), cacheKey, payload)
		}
	}
	s.observeCoverage(response.Coverage)
	writeJSON(w, http.StatusOK, response)
}

func normalizedTags(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		for _, tag := range strings.Split(value, ",") {
			tag = strings.ToLower(strings.TrimSpace(tag))
			if tag == "" {
				continue
			}
			if _, ok := seen[tag]; ok {
				continue
			}
			seen[tag] = struct{}{}
			result = append(result, tag)
		}
	}
	return result
}

func sameSearchTags(left, right []string) bool {
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

func (s *Server) observeQuery(elapsed time.Duration) {
	s.queryNanos.Add(uint64(elapsed))
	seconds := elapsed.Seconds()
	limits := [...]float64{0.01, 0.05, 0.1, 0.2, 0.4, 1e100}
	for n, limit := range limits {
		if seconds <= limit {
			s.queryBuckets[n].Add(1)
		}
	}
}

func (s *Server) observeCoverage(coverage search.Coverage) {
	if !coverage.Complete {
		s.incomplete.Add(1)
	}
	if coverage.Degraded {
		s.degraded.Add(1)
	}
	if counter, ok := s.reasons[coverage.Reason]; ok {
		counter.Add(1)
	}
}

func (s *Server) invalidateCache() {
	if invalidator, ok := s.Cache.(cacheInvalidator); ok {
		if err := invalidator.Clear(context.Background()); err != nil {
			log.Printf("lattice cache invalidation: %v", err)
		}
	}
}

func (s *Server) debugShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	const prefix = "/v1/debug/shards/"
	value := strings.TrimPrefix(r.URL.Path, prefix)
	shard, err := strconv.Atoi(value)
	if err != nil || shard < 0 {
		writeError(w, http.StatusBadRequest, "shard must be a non-negative integer")
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "debug shard controls require the local backend")
		return
	}
	var request struct {
		Available *bool `json:"available"`
		DelayMS   int   `json:"delay_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid debug request: "+err.Error())
		return
	}
	if request.Available != nil {
		s.Index.SetShardAvailable(shard, *request.Available)
	}
	if request.DelayMS < 0 || request.DelayMS > 30000 {
		writeError(w, http.StatusBadRequest, "delay_ms must be between 0 and 30000")
		return
	}
	s.Index.SetShardDelay(shard, time.Duration(request.DelayMS)*time.Millisecond)
	s.invalidateCache()
	writeJSON(w, http.StatusOK, map[string]any{"shard": shard, "available": request.Available, "delay_ms": request.DelayMS})
}

func (s *Server) reindex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if remote, ok := s.Backend.(interface {
		Reindex(context.Context) (opensearch.ReindexReport, error)
	}); ok {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		report, err := remote.Reindex(ctx)
		if err != nil {
			writeError(w, http.StatusBadGateway, "reindex: "+err.Error())
			return
		}
		s.invalidateCache()
		writeJSON(w, http.StatusOK, report)
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "reindex requires a local backend or configured OpenSearch alias")
		return
	}
	if err := s.Index.Reindex(); err != nil {
		writeError(w, http.StatusInternalServerError, "reindex: "+err.Error())
		return
	}
	s.invalidateCache()
	version := s.alias.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{"alias": "search", "version": version, "documents": s.Backend.Count()})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	var request struct {
		Path       string `json:"path"`
		Repository string `json:"repository"`
		Snapshot   string `json:"snapshot"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&request)
	}
	if remote, ok := s.Backend.(interface {
		Snapshot(context.Context, string, string) error
	}); ok {
		if request.Repository == "" || request.Snapshot == "" {
			writeError(w, http.StatusBadRequest, "repository and snapshot are required for a remote snapshot")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		if err := remote.Snapshot(ctx, request.Repository, request.Snapshot); err != nil {
			writeError(w, http.StatusBadGateway, "snapshot: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"repository": request.Repository, "snapshot": request.Snapshot})
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "snapshot requires a local backend or configured OpenSearch repository")
		return
	}
	path := s.Index.SnapshotPath()
	if request.Path != "" {
		path = request.Path
	}
	if err := s.Index.Snapshot(path); err != nil {
		writeError(w, http.StatusInternalServerError, "snapshot: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot": path, "documents": s.Backend.Count()})
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	var request struct {
		Path        string `json:"path"`
		Repository  string `json:"repository"`
		Snapshot    string `json:"snapshot"`
		SourceIndex string `json:"source_index"`
		TargetIndex string `json:"target_index"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&request)
	}
	if remote, ok := s.Backend.(interface {
		Restore(context.Context, string, string, string, string) error
	}); ok {
		if request.Repository == "" || request.Snapshot == "" {
			writeError(w, http.StatusBadRequest, "repository and snapshot are required for a remote restore")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		if err := remote.Restore(ctx, request.Repository, request.Snapshot, request.SourceIndex, request.TargetIndex); err != nil {
			writeError(w, http.StatusBadGateway, "restore: "+err.Error())
			return
		}
		s.invalidateCache()
		writeJSON(w, http.StatusOK, map[string]any{"repository": request.Repository, "snapshot": request.Snapshot})
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "restore requires a local backend or configured OpenSearch repository")
		return
	}
	path := s.Index.SnapshotPath()
	if request.Path != "" {
		path = request.Path
	}
	count, err := s.Index.Restore(path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "restore: "+err.Error())
		return
	}
	s.invalidateCache()
	writeJSON(w, http.StatusOK, map[string]any{"restored": path, "documents": count})
}

func parsePaging(r *http.Request) (int, int, error) {
	from, size := 0, 10
	var err error
	if value := r.URL.Query().Get("from"); value != "" {
		from, err = strconv.Atoi(value)
		if err != nil || from < 0 {
			return 0, 0, fmt.Errorf("from must be a non-negative integer")
		}
	}
	if value := r.URL.Query().Get("size"); value != "" {
		size, err = strconv.Atoi(value)
		if err != nil || size < 1 || size > 100 {
			return 0, 0, fmt.Errorf("size must be between 1 and 100")
		}
	}
	if from >= 10000 && r.URL.Query().Get("search_after") == "" {
		return 0, 0, fmt.Errorf("deep pagination requires search_after")
	}
	return from, size, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := requestID(r)
		w.Header().Set("X-Request-ID", requestID)
		recorder := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		log.Printf("lattice request_id=%s method=%s path=%s status=%d bytes=%d duration_ms=%.3f", requestID, r.Method, r.URL.Path, status, recorder.bytes, float64(time.Since(started).Microseconds())/1000)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(data)
	r.bytes += n
	return n, err
}

func requestID(r *http.Request) string {
	value := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if value != "" && len(value) <= 128 {
		valid := true
		for _, character := range value {
			if !(unicode.IsLetter(character) || unicode.IsDigit(character) || strings.ContainsRune("-_.:", character)) {
				valid = false
				break
			}
		}
		if valid {
			return value
		}
	}
	return fmt.Sprintf("lattice-%d", requestSequence.Add(1))
}

const openapiSpec = `openapi: 3.0.3
info:
  title: LATTICE Search API
  version: 0.1.0
  description: Hybrid search with honest shard coverage and deadlines. Configure LATTICE_API_KEY and send it as X-API-Key to protect /v1 routes.
servers:
  - url: http://localhost:8080
paths:
  /healthz:
    get:
      summary: Liveness check
      responses:
        '200': {description: Service is alive}
  /readyz:
    get:
      summary: Readiness check
      responses:
        '200': {description: Service is ready}
  /v1/documents:
    post:
      security: [{ApiKeyAuth: []}]
      summary: Publish a document through the local ingest adapter
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: '#/components/schemas/Document'}
      responses:
        '202': {description: Document accepted}
        '400': {description: Invalid document}
  /v1/search:
    get:
      security: [{ApiKeyAuth: []}]
      summary: Hybrid keyword and semantic search
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
        - {name: deadline, in: query, schema: {type: string, example: 150ms}}
        - {name: from, in: query, schema: {type: integer, minimum: 0}}
        - {name: size, in: query, schema: {type: integer, minimum: 1, maximum: 100}}
        - {name: search_after, in: query, description: Opaque continuation cursor returned by a previous search; required for deep pagination, schema: {type: string}}
        - {name: tag, in: query, description: Require every listed tag; repeat the parameter or use a comma-separated value, schema: {type: array, items: {type: string}}, style: form, explode: true}
      responses:
        '200':
          description: Results with coverage and timings
          content:
            application/json:
              schema: {$ref: '#/components/schemas/SearchResponse'}
  /v1/debug/shards/{shard}:
    post:
      security: [{ApiKeyAuth: []}]
      summary: Toggle a local shard failure for testing
      parameters:
        - {name: shard, in: path, required: true, schema: {type: integer}}
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                available: {type: boolean}
                delay_ms: {type: integer, minimum: 0, maximum: 30000}
      responses:
        '200': {description: Failure mode changed}
  /v1/admin/reindex:
    post:
      security: [{ApiKeyAuth: []}]
      summary: Rebuild OpenSearch into a new generation and atomically swap the configured alias
      responses:
        '200': {description: Reindex completed}
        '502': {description: OpenSearch operation failed}
  /v1/admin/snapshot:
    post:
      security: [{ApiKeyAuth: []}]
      summary: Create a local atomic snapshot or a native OpenSearch repository snapshot
      requestBody:
        content:
          application/json:
            schema: {$ref: '#/components/schemas/RemoteSnapshotRequest'}
      responses:
        '200': {description: Snapshot completed}
        '400': {description: Remote snapshots require repository and snapshot names}
        '502': {description: OpenSearch operation failed}
  /v1/admin/restore:
    post:
      security: [{ApiKeyAuth: []}]
      summary: Restore a local snapshot or a native OpenSearch repository snapshot
      requestBody:
        content:
          application/json:
            schema: {$ref: '#/components/schemas/RemoteRestoreRequest'}
      responses:
        '200': {description: Restore completed}
        '400': {description: Remote restores require repository and snapshot names}
        '502': {description: OpenSearch operation failed}
components:
  securitySchemes:
    ApiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key
  schemas:
    Document:
      type: object
      required: [id, title, body]
      properties:
        id: {type: string}
        title: {type: string}
        body: {type: string}
        tags: {type: array, items: {type: string}}
    RemoteSnapshotRequest:
      type: object
      properties:
        path: {type: string, description: Local snapshot path}
        repository: {type: string, description: OpenSearch snapshot repository}
        snapshot: {type: string, description: OpenSearch snapshot name}
    RemoteRestoreRequest:
      type: object
      properties:
        path: {type: string, description: Local snapshot path}
        repository: {type: string, description: OpenSearch snapshot repository}
        snapshot: {type: string, description: OpenSearch snapshot name}
        source_index: {type: string, description: Concrete index stored in the snapshot}
        target_index: {type: string, description: Concrete index to restore to before attaching the read alias}
    SearchResponse:
      type: object
      properties:
        results: {type: array, items: {type: object}}
        next_cursor: {type: string, description: Opaque cursor for the next page when more results are available}
        cache_hit: {type: boolean}
        coverage:
          type: object
          description: >-
            Completeness and degradation are separate facts. complete answers
            only "did every queried shard answer?"; degraded answers "did
            anything fail while answering?". A response can be complete and
            degraded, or incomplete and not degraded.
          required: [shards_queried, shards_answered, complete, degraded, reason]
          properties:
            shards_queried: {type: integer, description: Shards the query was supposed to reach}
            shards_answered: {type: integer, description: Shards that returned results}
            complete: {type: boolean, description: True only when shards_answered equals shards_queried}
            degraded: {type: boolean, description: True when the response carries at least one error, independent of complete}
            reason:
              type: string
              description: >-
                Machine-readable classification, so a client never parses error
                strings. complete: nothing went wrong. deadline: the budget
                expired before outstanding work returned. shard_unavailable: a
                shard did not answer and no deadline fired. dependency_error: a
                dependency such as the embedding service failed; shard coverage
                may still be complete. invalid_request: rejected before any
                shard was queried.
              enum: [complete, deadline, shard_unavailable, dependency_error, invalid_request]
        timings_ms: {type: object}
        errors: {type: array, items: {type: string}}
`

func openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	_, _ = w.Write([]byte(openapiSpec))
}

func swagger(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerHTML))
}

func playground(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(playgroundHTML))
}

const swaggerHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>LATTICE Swagger UI</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="swagger-ui">Loading LATTICE API…</div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>window.onload=()=>SwaggerUIBundle({url:'/openapi.yaml',dom_id:'#swagger-ui',deepLinking:true});</script>
</body></html>`

const playgroundHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>LATTICE Playground</title>
<style>body{font:16px system-ui;max-width:900px;margin:2rem auto;padding:0 1rem}textarea{width:100%;min-height:100px}input{padding:.5rem;width:70%}button{padding:.55rem 1rem;margin:.25rem}pre{background:#111;color:#eee;padding:1rem;overflow:auto}.grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem}</style></head>
<body><h1>LATTICE Playground</h1><p>Publish documents, run hybrid searches, and inspect coverage and timings.</p>
<div class="grid"><section><h2>Seed document</h2><textarea id="document">{"id":"doc-001","title":"Raft leader election","body":"A replicated system chooses a leader and commits a log safely.","tags":["consensus"]}</textarea><button onclick="seed()">Publish document</button></section>
<section><h2>Search</h2><input id="query" value="how does a cluster choose a leader?"><button onclick="search()">Search</button><h3>Failure drill</h3><input id="shard" type="number" min="0" value="0"><button onclick="failShard()">Make shard unavailable</button><button onclick="restoreShard()">Restore shard</button><h3>Operations</h3><button onclick="reindex()">Reindex</button><button onclick="snapshot()">Snapshot</button><button onclick="restore()">Restore</button><p><a href="/docs">Open Swagger UI</a> · <a href="/metrics">Metrics</a></p></section></div>
<h2>Response</h2><pre id="output">Ready.</pre>
<script>const out=document.querySelector('#output');async function call(url,opt){const r=await fetch(url,opt);let x;try{x=await r.json()}catch(_){x={error:r.statusText}}x.http_status=r.status;out.textContent=JSON.stringify(x,null,2)}function seed(){call('/v1/documents',{method:'POST',headers:{'content-type':'application/json'},body:document.querySelector('#document').value})}function search(){call('/v1/search?q='+encodeURIComponent(document.querySelector('#query').value)+'&deadline=150ms')}function failShard(){call('/v1/debug/shards/'+document.querySelector('#shard').value,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({available:false})})}function restoreShard(){call('/v1/debug/shards/'+document.querySelector('#shard').value,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({available:true})})}function reindex(){call('/v1/admin/reindex',{method:'POST'})}function snapshot(){call('/v1/admin/snapshot',{method:'POST'})}function restore(){call('/v1/admin/restore',{method:'POST'})}</script>
</body></html>`
