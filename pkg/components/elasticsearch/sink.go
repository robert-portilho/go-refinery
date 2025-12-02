package elasticsearch

import (
	"bytes"
	"context"
	"datapipeline/pkg/pipeline"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticsearchSink struct {
	client *elasticsearch.Client
	index  string
}

func NewElasticsearchSink(addresses []string, index, username, password string) (*ElasticsearchSink, error) {
	cfg := elasticsearch.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating the client: %s", err)
	}

	// Verify connection
	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error: %s", res.String())
	}

	return &ElasticsearchSink{
		client: es,
		index:  index,
	}, nil
}

func (s *ElasticsearchSink) Write(ctx context.Context, msg pipeline.Message) error {
	data, err := json.Marshal(msg.Data)
	if err != nil {
		return fmt.Errorf("error marshaling document: %s", err)
	}

	res, err := s.client.Index(
		s.index,
		bytes.NewReader(data),
		s.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("error indexing document: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error indexing document: %s", res.String())
	}

	return nil
}

func (s *ElasticsearchSink) WriteBatch(ctx context.Context, msgs []pipeline.Message) error {
	var buf bytes.Buffer
	for _, msg := range msgs {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s" } }%s`, s.index, "\n"))
		data, err := json.Marshal(msg.Data)
		if err != nil {
			log.Printf("Error marshaling message %s: %v", msg.ID, err)
			continue
		}
		data = append(data, "\n"...)
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	if buf.Len() == 0 {
		return nil
	}

	res, err := s.client.Bulk(bytes.NewReader(buf.Bytes()), s.client.Bulk.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("error performing bulk index: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error performing bulk index: %s", res.String())
	}

	return nil
}

func (s *ElasticsearchSink) Close() error {
	return nil
}
