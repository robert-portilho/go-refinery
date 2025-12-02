package pipeline

import (
	"context"
)

// Message represents the data flowing through the pipeline.
type Message struct {
	ID              string
	Data            map[string]interface{}
	Metadata        map[string]string
	OriginalMessage interface{} // Holds the original message object (e.g., kafka.Message) for committing
}

// Source reads data from an external system.
type Source interface {
	Read(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msgs []Message) error
	Close() error
}

// Processor transforms, filters, or enriches data.
type Processor interface {
	Process(msg Message) (Message, error)
}

// Sink writes data to an external system.
type Sink interface {
	Write(ctx context.Context, msg Message) error
	WriteBatch(ctx context.Context, msgs []Message) error
	Close() error
}
