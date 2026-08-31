// Package ingest models the Kafka-to-indexer contract. The in-memory broker is
// used for local development; the same event and offset semantics are used by
// the future Kafka adapter.
package ingest

import (
	"sync"

	"github.com/nickemma/lattice/internal/search"
)

type Event struct {
	Offset   uint64
	Document search.Document
}

type DeadLetter struct {
	Event  Event  `json:"event"`
	Reason string `json:"reason"`
}

type Broker struct {
	mu     sync.Mutex
	next   uint64
	events []Event
}

func NewBroker() *Broker { return &Broker{} }

func (b *Broker) Publish(doc search.Document) Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	event := Event{Offset: b.next, Document: doc}
	b.next++
	b.events = append(b.events, event)
	return event
}

func (b *Broker) EventsFrom(offset uint64) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	var result []Event
	for _, event := range b.events {
		if event.Offset >= offset {
			result = append(result, event)
		}
	}
	return result
}

type Indexer struct {
	mu        sync.Mutex
	broker    *Broker
	index     search.Backend
	next      uint64
	dlq       []DeadLetter
	processed uint64
}

func NewIndexer(broker *Broker, index search.Backend) *Indexer {
	return &Indexer{broker: broker, index: index}
}

func (i *Indexer) ProcessAvailable() {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, event := range i.broker.EventsFrom(i.next) {
		if event.Offset < i.next {
			continue
		}
		if err := validate(event.Document); err != nil {
			i.dlq = append(i.dlq, DeadLetter{Event: event, Reason: err.Error()})
			i.next = event.Offset + 1
			continue
		}
		if err := i.index.Upsert(event.Document); err != nil {
			continue
		}
		i.processed++
		i.next = event.Offset + 1
	}
}

func (i *Indexer) DLQ() []DeadLetter {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]DeadLetter(nil), i.dlq...)
}

func (i *Indexer) Processed() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.processed
}

func validate(doc search.Document) error {
	if doc.ID == "" || doc.Title == "" || doc.Body == "" {
		return &validationError{"id, title, and body are required"}
	}
	return nil
}

type validationError struct{ message string }

func (e *validationError) Error() string { return e.message }
