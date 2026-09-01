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

func TestSearchFiltersRequireAllTags(t *testing.T) {
	i := NewIndex(2)
	i.Upsert(Document{ID: "one", Title: "Raft", Body: "consensus", Tags: []string{"distributed", "consensus"}})
	i.Upsert(Document{ID: "two", Title: "Raft", Body: "consensus", Tags: []string{"distributed"}})
	result := i.SearchWithFilters(context.Background(), "consensus", []string{"distributed", "consensus"}, 0, 10)
	if len(result.Results) != 1 || result.Results[0].Document.ID != "one" {
		t.Fatalf("filtered result = %+v", result.Results)
	}
}

func TestSearchCursorContinuesStablePage(t *testing.T) {
	i := NewIndex(1)
	for _, id := range []string{"one", "two", "three"} {
		if err := i.Upsert(Document{ID: id, Title: "consensus", Body: "consensus"}); err != nil {
			t.Fatal(err)
		}
	}
	first := i.SearchWithFilters(context.Background(), "consensus", nil, 0, 1)
	if len(first.Results) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}
	second := i.SearchWithFiltersCursor(context.Background(), "consensus", nil, first.NextCursor, 1)
	if len(second.Results) != 1 || second.Results[0].Document.ID == first.Results[0].Document.ID {
		t.Fatalf("second page = %+v", second)
	}
	if _, err := DecodeCursor("not-a-cursor"); err == nil {
		t.Fatal("expected malformed cursor error")
	}
}

func TestIncompleteSearchDoesNotAdvertiseCursor(t *testing.T) {
	i := NewIndex(2)
	if err := i.Upsert(Document{ID: "one", Title: "consensus", Body: "consensus"}); err != nil {
		t.Fatal(err)
	}
	i.SetShardDelay(1, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	response := i.SearchWithFilters(ctx, "consensus", nil, 0, 1)
	if response.Coverage.Complete {
		t.Fatalf("expected incomplete response: %+v", response.Coverage)
	}
	if response.NextCursor != "" {
		t.Fatalf("incomplete response advertised cursor: %q", response.NextCursor)
	}
}
