package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nickemma/lattice/internal/ingest"
	"github.com/nickemma/lattice/internal/search"
)

type Server struct {
	Index        *search.Index
	Backend      search.Backend
	Publisher    Publisher
	Broker       *ingest.Broker
	Indexer      *ingest.Indexer
	Cache        Cache
	mode         string
	queries      atomic.Uint64
	docs         atomic.Uint64
	incomplete   atomic.Uint64
	alias        atomic.Uint64
	cacheHits    atomic.Uint64
	cacheMisses  atomic.Uint64
	queryNanos   atomic.Uint64
	queryBuckets [6]atomic.Uint64
}

type Publisher interface {
	Publish(context.Context, []byte, []byte) error
}

type Cache interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}

func New(index *search.Index) *Server {
	broker := ingest.NewBroker()
	return &Server{Index: index, Backend: index, Broker: broker, Indexer: ingest.NewIndexer(broker, index), mode: "local-index"}
}

func NewWithBackend(backend search.Backend) *Server {
	broker := ingest.NewBroker()
	return &Server{Backend: backend, Broker: broker, Indexer: ingest.NewIndexer(broker, backend), mode: "configured-backend"}
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
	return requestLog(mux)
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
	fmt.Fprintf(w, "# TYPE lattice_query_incomplete_total counter\nlattice_query_incomplete_total %d\n", s.incomplete.Load())
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
	s.queries.Add(1)
	started := time.Now()
	cacheKey := fmt.Sprintf("lattice:query:%d:%d:%s", from, size, query)
	if s.Cache != nil {
		if payload, ok, cacheErr := s.Cache.Get(ctx, cacheKey); cacheErr == nil && ok {
			var cached search.Response
			if json.Unmarshal(payload, &cached) == nil {
				cached.CacheHit = true
				s.observeQuery(time.Since(started))
				s.cacheHits.Add(1)
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}
	response := s.Backend.Search(ctx, query, from, size)
	s.observeQuery(time.Since(started))
	if response.CacheHit {
		s.cacheHits.Add(1)
	} else {
		s.cacheMisses.Add(1)
	}
	if s.Cache != nil && response.Coverage.Complete && len(response.Errors) == 0 {
		if payload, marshalErr := json.Marshal(response); marshalErr == nil {
			_ = s.Cache.Set(context.Background(), cacheKey, payload)
		}
	}
	if !response.Coverage.Complete {
		s.incomplete.Add(1)
	}
	writeJSON(w, http.StatusOK, response)
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
	writeJSON(w, http.StatusOK, map[string]any{"shard": shard, "available": request.Available, "delay_ms": request.DelayMS})
}

func (s *Server) reindex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "reindex requires the local backend")
		return
	}
	if err := s.Index.Reindex(); err != nil {
		writeError(w, http.StatusInternalServerError, "reindex: "+err.Error())
		return
	}
	version := s.alias.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{"alias": "search", "version": version, "documents": s.Backend.Count()})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "snapshot requires the local backend")
		return
	}
	path := s.Index.SnapshotPath()
	var request struct {
		Path string `json:"path"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&request)
	}
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
	if s.Index == nil {
		writeError(w, http.StatusNotImplemented, "restore requires the local backend")
		return
	}
	path := s.Index.SnapshotPath()
	var request struct {
		Path string `json:"path"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&request)
	}
	if request.Path != "" {
		path = request.Path
	}
	count, err := s.Index.Restore(path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "restore: "+err.Error())
		return
	}
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
		next.ServeHTTP(w, r)
		_ = started
	})
}

const openapiSpec = `openapi: 3.0.3
info:
  title: LATTICE Search API
  version: 0.1.0
  description: Hybrid search with honest shard coverage and deadlines.
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
      summary: Hybrid keyword and semantic search
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
        - {name: deadline, in: query, schema: {type: string, example: 150ms}}
        - {name: from, in: query, schema: {type: integer, minimum: 0}}
        - {name: size, in: query, schema: {type: integer, minimum: 1, maximum: 100}}
      responses:
        '200':
          description: Results with coverage and timings
          content:
            application/json:
              schema: {$ref: '#/components/schemas/SearchResponse'}
  /v1/debug/shards/{shard}:
    post:
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
      summary: Rebuild the search index and advance the search alias
      responses:
        '200': {description: Reindex completed}
  /v1/admin/snapshot:
    post:
      summary: Create a verified local snapshot
      responses:
        '200': {description: Snapshot completed}
  /v1/admin/restore:
    post:
      summary: Restore a local snapshot
      responses:
        '200': {description: Restore completed}
components:
  schemas:
    Document:
      type: object
      required: [id, title, body]
      properties:
        id: {type: string}
        title: {type: string}
        body: {type: string}
        tags: {type: array, items: {type: string}}
    SearchResponse:
      type: object
      properties:
        results: {type: array, items: {type: object}}
        cache_hit: {type: boolean}
        coverage:
          type: object
          properties:
            shards_queried: {type: integer}
            shards_answered: {type: integer}
            complete: {type: boolean}
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
