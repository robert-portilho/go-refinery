package kafka

import (
	"context"
	"fmt"
	"time"

	"datapipeline/pkg/pipeline"

	"github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	reader *kafka.Reader
}

func NewKafkaSource(brokers []string, topic string, groupID string) *KafkaSource {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &KafkaSource{
		reader: r,
	}
}

func (k *KafkaSource) Read(ctx context.Context) (pipeline.Message, error) {
	// FetchMessage does not commit offsets automatically
	m, err := k.reader.FetchMessage(ctx)
	if err != nil {
		return pipeline.Message{}, err
	}

	return pipeline.Message{
		ID: string(m.Key),
		// Inicialmente colocamos o payload bruto em um campo "raw"
		// O JSON Processor será responsável por parsear isso
		Data: map[string]interface{}{
			"raw": m.Value,
		},
		Metadata: map[string]string{
			"topic":     m.Topic,
			"partition": fmt.Sprintf("%d", m.Partition),
			"offset":    fmt.Sprintf("%d", m.Offset),
			"timestamp": m.Time.Format(time.RFC3339),
		},
		OriginalMessage: m,
	}, nil
}

func (k *KafkaSource) Commit(ctx context.Context, msgs []pipeline.Message) error {
	var kafkaMsgs []kafka.Message
	for _, msg := range msgs {
		if km, ok := msg.OriginalMessage.(kafka.Message); ok {
			kafkaMsgs = append(kafkaMsgs, km)
		}
	}
	if len(kafkaMsgs) > 0 {
		return k.reader.CommitMessages(ctx, kafkaMsgs...)
	}
	return nil
}

func (k *KafkaSource) Close() error {
	return k.reader.Close()
}
