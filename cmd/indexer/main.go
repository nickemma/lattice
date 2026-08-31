package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nickemma/lattice/internal/providers/embedding"
	"github.com/nickemma/lattice/internal/providers/kafka"
	"github.com/nickemma/lattice/internal/providers/opensearch"
	"github.com/nickemma/lattice/internal/search"
	kafkaapi "github.com/segmentio/kafka-go"
)

const (
	maxBatchSize = 100
	batchWindow  = 25 * time.Millisecond
)

type metrics struct {
	documents atomic.Uint64
	dlq       atomic.Uint64
	batches   atomic.Uint64
	retries   atomic.Uint64
	lagMillis atomic.Int64
}

func (m *metrics) handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE lattice_ingest_documents_total counter\nlattice_ingest_documents_total %d\n", m.documents.Load())
	fmt.Fprintf(w, "# TYPE lattice_ingest_dlq_total counter\nlattice_ingest_dlq_total %d\n", m.dlq.Load())
	fmt.Fprintf(w, "# TYPE lattice_ingest_batches_total counter\nlattice_ingest_batches_total %d\n", m.batches.Load())
	fmt.Fprintf(w, "# TYPE lattice_ingest_retries_total counter\nlattice_ingest_retries_total %d\n", m.retries.Load())
	fmt.Fprintf(w, "# TYPE lattice_ingest_lag_seconds gauge\nlattice_ingest_lag_seconds %.3f\n", float64(m.lagMillis.Load())/1000)
}

type bulkIndexer interface {
	BulkUpsert(context.Context, []search.Document) ([]opensearch.BulkOutcome, error)
}

func main() {
	brokers := kafka.ParseBrokers(env("KAFKA_BROKERS", "localhost:9092"))
	topic := env("KAFKA_TOPIC", "lattice.documents")
	group := env("KAFKA_GROUP", "lattice-indexer")
	dataDir := env("LATTICE_DATA_DIR", ".lattice-data")
	var backend search.Backend
	var localIndex *search.Index
	var err error
	if remoteURL := os.Getenv("OPENSEARCH_URL"); remoteURL != "" {
		indexName := env("OPENSEARCH_INDEX", "lattice-documents")
		var embedder embedding.Embedder
		if embeddingURL := os.Getenv("EMBEDDING_URL"); embeddingURL != "" {
			embedder = embedding.New(embeddingURL)
		}
		remoteBackend := opensearch.NewBackendWithEmbedder(opensearch.New(remoteURL), indexName, embedder)
		waitContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := remoteBackend.WaitForIndex(waitContext); err != nil {
			cancel()
			log.Fatal(err)
		}
		cancel()
		readyContext, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := remoteBackend.Ready(readyContext); err != nil {
			readyCancel()
			log.Fatal(err)
		}
		readyCancel()
		backend = remoteBackend
	} else {
		localIndex, err = search.OpenPersistentIndex(dataDir, 3)
		if err != nil {
			log.Fatal(err)
		}
		defer localIndex.Close()
		backend = localIndex
	}
	consumer := kafka.NewConsumer(brokers, topic, group)
	defer consumer.Close()
	dlq := kafka.NewProducer(brokers, topic+".dlq")
	defer dlq.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	indexerMetrics := &metrics{}
	metricsServer := &http.Server{Addr: env("LATTICE_INDEXER_ADDR", ":9091"), Handler: http.HandlerFunc(indexerMetrics.handler)}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()
	defer metricsServer.Close()

	backoff := 100 * time.Millisecond
	for {
		messages, fetchErr := fetchBatch(ctx, consumer)
		if fetchErr != nil {
			if errors.Is(fetchErr, context.Canceled) {
				return
			}
			log.Printf("fetch batch: %v", fetchErr)
			if !wait(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		indexerMetrics.batches.Add(1)
		if err := processBatch(ctx, consumer, dlq, backend, messages, indexerMetrics); err != nil {
			log.Printf("process batch: %v", err)
			indexerMetrics.retries.Add(1)
			if !wait(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

func fetchBatch(ctx context.Context, consumer *kafka.Consumer) ([]kafkaapi.Message, error) {
	first, err := consumer.Fetch(ctx)
	if err != nil {
		return nil, err
	}
	batch := []kafkaapi.Message{first}
	window := time.NewTimer(batchWindow)
	defer window.Stop()
	for len(batch) < maxBatchSize {
		fetchContext, cancel := context.WithCancel(ctx)
		finished := make(chan struct{})
		var message kafkaapi.Message
		var fetchErr error
		go func() {
			message, fetchErr = consumer.Fetch(fetchContext)
			close(finished)
		}()
		select {
		case <-finished:
			cancel()
			if fetchErr != nil {
				if ctx.Err() != nil {
					return batch, ctx.Err()
				}
				return batch, nil
			}
			batch = append(batch, message)
		case <-window.C:
			cancel()
			return batch, nil
		case <-ctx.Done():
			cancel()
			return batch, ctx.Err()
		}
	}
	return batch, nil
}

func processBatch(ctx context.Context, consumer *kafka.Consumer, dlq *kafka.Producer, backend search.Backend, messages []kafkaapi.Message, telemetry *metrics) error {
	if telemetry == nil {
		telemetry = &metrics{}
	}
	for _, message := range messages {
		if !message.Time.IsZero() {
			lag := time.Since(message.Time)
			if lag < 0 {
				lag = 0
			}
			telemetry.lagMillis.Store(lag.Milliseconds())
		}
	}
	validMessages := make([]kafkaapi.Message, 0, len(messages))
	documents := make([]search.Document, 0, len(messages))
	for _, message := range messages {
		var doc search.Document
		if err := json.Unmarshal(message.Value, &doc); err != nil {
			if err := deadLetterAndCommit(ctx, consumer, dlq, message, err); err != nil {
				return err
			}
			telemetry.dlq.Add(1)
			continue
		}
		if doc.ID == "" || doc.Title == "" || doc.Body == "" {
			if err := deadLetterAndCommit(ctx, consumer, dlq, message, errors.New("id, title, and body are required")); err != nil {
				return err
			}
			telemetry.dlq.Add(1)
			continue
		}
		validMessages = append(validMessages, message)
		documents = append(documents, doc)
	}
	if len(documents) == 0 {
		return nil
	}
	if bulk, ok := backend.(bulkIndexer); ok {
		outcomes, err := bulk.BulkUpsert(ctx, documents)
		if err != nil {
			return err
		}
		if len(outcomes) != len(documents) {
			return errors.New("bulk outcome count did not match input count")
		}
		for n, outcome := range outcomes {
			if outcome.Status == 0 || outcome.Status == 429 || outcome.Status >= 500 {
				return fmt.Errorf("retryable OpenSearch status %d for %s: %s", outcome.Status, outcome.ID, outcome.Error)
			}
			if outcome.Status >= 400 {
				if err := deadLetterAndCommit(ctx, consumer, dlq, validMessages[n], fmt.Errorf("OpenSearch status %d: %s", outcome.Status, outcome.Error)); err != nil {
					return err
				}
				telemetry.dlq.Add(1)
				continue
			}
			if err := consumer.Commit(ctx, validMessages[n]); err != nil {
				return err
			}
			telemetry.documents.Add(1)
		}
		return nil
	}
	for n, doc := range documents {
		if err := backend.Upsert(doc); err != nil {
			return err
		}
		if err := consumer.Commit(ctx, validMessages[n]); err != nil {
			return err
		}
		telemetry.documents.Add(1)
	}
	return nil
}

func deadLetterAndCommit(ctx context.Context, consumer *kafka.Consumer, dlq *kafka.Producer, message kafkaapi.Message, cause error) error {
	if err := publishDLQ(ctx, dlq, message.Key, message.Value, cause); err != nil {
		return err
	}
	return consumer.Commit(ctx, message)
}

func publishDLQ(ctx context.Context, producer *kafka.Producer, key, value []byte, cause error) error {
	payload, err := json.Marshal(map[string]any{"key": string(key), "value": string(value), "error": cause.Error()})
	if err != nil {
		return err
	}
	if err := producer.Publish(ctx, key, payload); err != nil {
		return fmt.Errorf("publish DLQ: %w", err)
	}
	return nil
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current >= 5*time.Second {
		return 5 * time.Second
	}
	return current * 2
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
