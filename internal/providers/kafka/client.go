// Package kafka provides the production Kafka boundary. The local ingest
// broker is deliberately kept separate so unit and offline integration tests
// do not require a running Kafka cluster.
package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	broker string
}

func NewProducer(brokers []string, topic string) *Producer {
	broker := ""
	if len(brokers) > 0 {
		broker = brokers[0]
	}
	return &Producer{broker: broker, writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 25 * time.Millisecond,
	}}
}

func ParseBrokers(value string) []string {
	var result []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			result = append(result, broker)
		}
	}
	return result
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value, Time: time.Now().UTC()})
}

func (p *Producer) Ready(ctx context.Context) error {
	if p.broker == "" {
		return context.Canceled
	}
	dialer := &kafka.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", p.broker)
	if err != nil {
		return err
	}
	return connection.Close()
}

func (p *Producer) Close() error { return p.writer.Close() }

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, group string) *Consumer {
	return &Consumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0, // explicit commit after indexing or DLQ
	})}
}

func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, message kafka.Message) error {
	return c.reader.CommitMessages(ctx, message)
}

func (c *Consumer) Close() error { return c.reader.Close() }
