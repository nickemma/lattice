package kafka

import "testing"

func TestParseBrokers(t *testing.T) {
	brokers := ParseBrokers(" kafka-1:9092, kafka-2:9092 ,, ")
	if len(brokers) != 2 || brokers[0] != "kafka-1:9092" || brokers[1] != "kafka-2:9092" {
		t.Fatalf("brokers = %#v", brokers)
	}
}
