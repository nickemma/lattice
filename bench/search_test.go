package bench

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nickemma/lattice/internal/search"
)

func BenchmarkHybridSearch(b *testing.B) {
	index := search.NewIndex(3)
	documents := 10000
	if value := os.Getenv("LATTICE_BENCH_DOCS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			documents = parsed
		}
	}
	for n := 0; n < documents; n++ {
		index.Upsert(search.Document{
			ID:    fmt.Sprintf("doc-%06d", n),
			Title: "distributed systems document",
			Body:  "replication consensus storage search and retrieval",
		})
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_ = index.Search(ctx, fmt.Sprintf("distributed consensus %d", n), 0, 10)
		cancel()
	}
}
