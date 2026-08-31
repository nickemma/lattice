package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nickemma/lattice/internal/providers/embedding"
	"github.com/nickemma/lattice/internal/providers/kafka"
	"github.com/nickemma/lattice/internal/providers/opensearch"
	"github.com/nickemma/lattice/internal/providers/redis"
	"github.com/nickemma/lattice/internal/search"
	"github.com/nickemma/lattice/internal/server"
)

func main() {
	address := os.Getenv("LATTICE_ADDR")
	if address == "" {
		address = ":8080"
	}
	dataDir := os.Getenv("LATTICE_DATA_DIR")
	if dataDir == "" {
		dataDir = ".lattice-data"
	}
	var api *server.Server
	var resultCache *redis.Cache
	if rawURL := os.Getenv("REDIS_URL"); rawURL != "" {
		var err error
		resultCache, err = redis.New(rawURL, 30*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		defer resultCache.Close()
	}
	if remoteURL := os.Getenv("OPENSEARCH_URL"); remoteURL != "" {
		indexName := os.Getenv("OPENSEARCH_INDEX")
		if indexName == "" {
			indexName = "lattice-documents"
		}
		var embedder embedding.Embedder
		if embeddingURL := os.Getenv("EMBEDDING_URL"); embeddingURL != "" {
			embedder = embedding.New(embeddingURL)
		}
		backend := opensearch.NewBackendWithEmbedder(opensearch.New(remoteURL), indexName, embedder)
		waitContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := backend.WaitForIndex(waitContext); err != nil {
			cancel()
			log.Fatal(err)
		}
		cancel()
		var publisher server.Publisher
		if brokerAddresses := os.Getenv("KAFKA_BROKERS"); brokerAddresses != "" {
			producer := kafka.NewProducer(kafka.ParseBrokers(brokerAddresses), envOr("KAFKA_TOPIC", "lattice.documents"))
			defer producer.Close()
			publisher = producer
		}
		api = server.NewWithBackendAndPublisherAndCache(backend, publisher, resultCache)
	} else {
		index, err := search.OpenPersistentIndex(dataDir, 3)
		if err != nil {
			log.Fatal(err)
		}
		defer index.Close()
		api = server.New(index)
	}
	log.Printf("lattice listening on %s", address)
	httpServer := &http.Server{Addr: address, Handler: api.Handler()}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-stop
	if err := httpServer.Close(); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
