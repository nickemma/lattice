package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	resultCache, closeCache, err := newResultCache(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer closeCache()
	if remoteURL := os.Getenv("OPENSEARCH_URL"); remoteURL != "" {
		indexName := os.Getenv("OPENSEARCH_INDEX")
		if indexName == "" {
			indexName = "lattice-documents"
		}
		var embedder embedding.Embedder
		if embeddingURL := os.Getenv("EMBEDDING_URL"); embeddingURL != "" {
			embedder = embedding.New(embeddingURL)
		}
		client := opensearch.New(remoteURL)
		client.Username = os.Getenv("OPENSEARCH_USERNAME")
		client.Password = os.Getenv("OPENSEARCH_PASSWORD")
		caFile := os.Getenv("OPENSEARCH_CA_FILE")
		clientCertFile := os.Getenv("OPENSEARCH_CLIENT_CERT_FILE")
		clientKeyFile := os.Getenv("OPENSEARCH_CLIENT_KEY_FILE")
		if caFile != "" || clientCertFile != "" || clientKeyFile != "" {
			if err := client.ConfigureTLS(caFile, clientCertFile, clientKeyFile); err != nil {
				log.Fatal(err)
			}
		}
		alias := os.Getenv("OPENSEARCH_ALIAS")
		var backend *opensearch.Backend
		if alias == "" {
			backend = opensearch.NewBackendWithEmbedder(client, indexName, embedder)
		} else {
			backend = opensearch.NewBackendWithAliasAndEmbedder(client, indexName, alias, embedder)
		}
		applyIndexLayout(backend)
		waitContext, cancel := context.WithTimeout(context.Background(), dependencyTimeout())
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
		if err := serve(httpServer); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-stop
	if err := httpServer.Close(); err != nil {
		log.Fatal(err)
	}
}

func serve(server *http.Server) error {
	certFile := os.Getenv("LATTICE_TLS_CERT_FILE")
	keyFile := os.Getenv("LATTICE_TLS_KEY_FILE")
	if certFile == "" && keyFile == "" {
		return server.ListenAndServe()
	}
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("both LATTICE_TLS_CERT_FILE and LATTICE_TLS_KEY_FILE are required")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13} //nolint:gosec -- TLS 1.3 is the deployment floor.
	caFile := os.Getenv("LATTICE_TLS_CA_FILE")
	if caFile != "" {
		ca, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("read LATTICE_TLS_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return fmt.Errorf("parse LATTICE_TLS_CA_FILE")
		}
		config.RootCAs = pool
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	server.TLSConfig = config
	return server.ListenAndServeTLS(certFile, keyFile)
}

// newResultCache returns the shared query-result cache, or a genuinely nil
// server.Cache when Redis is not configured.
//
// The return type is the interface rather than *redis.Cache on purpose. A nil
// *redis.Cache stored in a server.Cache interface is not a nil interface: the
// server's `s.Cache != nil` guards would all pass, and /readyz and every query
// would dereference it. That crashed the readiness probe of any deployment that
// configured OpenSearch without Redis.
func newResultCache(rawURL string) (server.Cache, func() error, error) {
	noop := func() error { return nil }
	if rawURL == "" {
		return nil, noop, nil
	}
	cache, err := redis.New(rawURL, 30*time.Second)
	if err != nil {
		return nil, noop, err
	}
	return cache, cache.Close, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func dependencyTimeout() time.Duration {
	const fallback = 10 * time.Minute
	raw := os.Getenv("LATTICE_DEPENDENCY_TIMEOUT")
	if raw == "" {
		return fallback
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		log.Printf("invalid LATTICE_DEPENDENCY_TIMEOUT %q; using %s", raw, fallback)
		return fallback
	}
	return duration
}

// applyIndexLayout lets an operator declare the shard and replica counts used
// when LATTICE creates its index. The partial-results experiment sets
// OPENSEARCH_REPLICAS=0 so that stopping a data node genuinely removes a shard
// from the cluster instead of being routed around.
func applyIndexLayout(backend *opensearch.Backend) {
	rawShards := os.Getenv("OPENSEARCH_SHARDS")
	rawReplicas := os.Getenv("OPENSEARCH_REPLICAS")
	if rawShards == "" && rawReplicas == "" {
		return
	}
	shards := envPositiveInt("OPENSEARCH_SHARDS", rawShards, 3)
	replicas := envPositiveInt("OPENSEARCH_REPLICAS", rawReplicas, 1)
	if shards < 1 {
		log.Fatalf("OPENSEARCH_SHARDS must be at least 1, got %q", rawShards)
	}
	backend.SetIndexLayout(shards, replicas)
	log.Printf("lattice index layout: shards=%d replicas=%d", shards, replicas)
}

// envPositiveInt fails loudly rather than falling back on a typo: an index
// created with the wrong layout is not something a later run can correct.
func envPositiveInt(name, raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		log.Fatalf("%s must be a non-negative integer, got %q", name, raw)
	}
	return value
}
