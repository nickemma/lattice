package search

import (
	"context"
	"testing"
	"time"
)

func TestHybridSearchAndCoverage(t *testing.T) {
	i := NewIndex(3)
	i.Upsert(Document{ID: "1", Title: "Raft leader election", Body: "Consensus chooses a leader."})
	i.Upsert(Document{ID: "2", Title: "Storage", Body: "An LSM tree flushes SSTables."})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := i.Search(ctx, "how does consensus choose a coordinator", 0, 10)
	if len(result.Results) == 0 || !result.Coverage.Complete {
		t.Fatalf("search result = %+v", result)
	}
	i.SetShardAvailable(1, false)
	result = i.Search(ctx, "raft", 0, 10)
	if result.Coverage.Complete || result.Coverage.ShardsAnswered >= result.Coverage.ShardsQueried {
		t.Fatalf("expected incomplete coverage: %+v", result.Coverage)
	}
}

func TestDeadlineReturnsPartialResults(t *testing.T) {
	i := NewIndex(2)
	i.Upsert(Document{ID: "1", Title: "fast result", Body: "search"})
	i.Upsert(Document{ID: "2", Title: "another fast result", Body: "search"})
	i.SetShardDelay(1, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := i.Search(ctx, "search", 0, 10)
	if result.Coverage.Complete || len(result.Results) == 0 {
		t.Fatalf("expected partial results: %+v", result)
	}
}
