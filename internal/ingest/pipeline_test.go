package ingest

import (
	"testing"

	"github.com/nickemma/lattice/internal/search"
)

func TestIndexerCommitsOffsetsAndDeadLetters(t *testing.T) {
	broker := NewBroker()
	index := search.NewIndex(2)
	indexer := NewIndexer(broker, index)
	broker.Publish(search.Document{ID: "ok", Title: "Good", Body: "document"})
	broker.Publish(search.Document{ID: "bad", Title: "", Body: "invalid"})
	broker.Publish(search.Document{ID: "after", Title: "After", Body: "continues"})
	indexer.ProcessAvailable()
	if index.Count() != 2 {
		t.Fatalf("indexed count = %d", index.Count())
	}
	if len(indexer.DLQ()) != 1 {
		t.Fatalf("dlq count = %d", len(indexer.DLQ()))
	}
	indexer.ProcessAvailable()
	if indexer.Processed() != 2 {
		t.Fatalf("processed count = %d", indexer.Processed())
	}
}
