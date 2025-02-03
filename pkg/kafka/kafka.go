package kafka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Writer struct {
	kafkaConfig kafka.ConfigMap
	producer    *kafka.Producer
	topic       string
}

type Opts struct {
	Topic     string
	SAKey     string
	SASecret  string
	Bootstrap string
}

func getConfig(opts Opts) kafka.ConfigMap {
	kafkaConfig := make(map[string]kafka.ConfigValue)
	kafkaConfig["sasl.username"] = opts.SAKey
	kafkaConfig["sasl.password"] = opts.SASecret
	kafkaConfig["bootstrap.servers"] = opts.Bootstrap
	kafkaConfig["security.protocol"] = "SASL_SSL"
	kafkaConfig["sasl.mechanisms"] = "PLAIN"
	kafkaConfig["client.id"] = "github.com/thz/senstore client"
	kafkaConfig["session.timeout.ms"] = 45000
	return kafkaConfig
}

func NewWriter(opts Opts) *Writer {

	w := &Writer{
		kafkaConfig: getConfig(opts),
		topic:       opts.Topic,
	}
	return w
}

func (w *Writer) Produce(ts time.Time, data []byte) error {
	err := w.maybeCreateProducer()
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}

	w.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &w.topic, Partition: kafka.PartitionAny},
		Key:            []byte("readings"),
		Value:          data,
		TimestampType:  kafka.TimestampCreateTime,
		Timestamp:      ts,
	}, nil)
	return nil
}

func (w *Writer) maybeCreateProducer() error {
	if w.producer != nil {
		return nil
	}
	p, err := kafka.NewProducer(&w.kafkaConfig)
	if err != nil {
		return err
	}
	w.producer = p
	return nil
}
