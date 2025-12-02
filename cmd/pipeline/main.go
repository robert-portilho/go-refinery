package main

import (
	"context"
	"datapipeline/pkg/components/elasticsearch"
	"datapipeline/pkg/components/kafka"
	"datapipeline/pkg/components/processors"
	"datapipeline/pkg/components/sqlserver"
	"datapipeline/pkg/config"
	"datapipeline/pkg/pipeline"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 1. Create Source
	source, err := createSource(cfg.Pipeline.Source)
	if err != nil {
		log.Fatalf("Failed to create source: %v", err)
	}

	// 2. Create Processors
	var procs []pipeline.Processor
	for _, pCfg := range cfg.Pipeline.Processors {
		p, err := createProcessor(pCfg)
		if err != nil {
			log.Fatalf("Failed to create processor %s: %v", pCfg.Type, err)
		}
		procs = append(procs, p)
	}

	// 3. Create Sink
	sink, err := createSink(cfg.Pipeline.Sink)
	if err != nil {
		log.Fatalf("Failed to create sink: %v", err)
	}

	// 4. Create and Run Engine
	// Set defaults if not provided in config
	if cfg.Pipeline.BatchSize == 0 {
		cfg.Pipeline.BatchSize = 1000
	}
	if cfg.Pipeline.BatchTimeout == 0 {
		cfg.Pipeline.BatchTimeout = 1 * time.Second // Default 1s
	}

	engine := pipeline.NewEngine(source, procs, sink, cfg.Pipeline.WorkerCount, cfg.Pipeline.BatchSize, cfg.Pipeline.BatchTimeout)
	defer engine.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Println("Shutting down...")
		cancel()
	}()

	log.Println("Pipeline started...")
	if err := engine.Run(ctx); err != nil {
		log.Printf("Pipeline stopped with error: %v", err)
	}
	log.Println("Pipeline finished.")
}

func createSource(cfg config.ComponentConfig) (pipeline.Source, error) {
	switch cfg.Type {
	case "kafka":
		brokers := getStringSlice(cfg.Config, "brokers")
		topic := getString(cfg.Config, "topic")
		groupID := getString(cfg.Config, "group_id")
		return kafka.NewKafkaSource(brokers, topic, groupID), nil
	default:
		return nil, fmt.Errorf("unknown source type: %s", cfg.Type)
	}
}

func createProcessor(cfg config.ComponentConfig) (pipeline.Processor, error) {
	return processors.CreateProcessor(cfg.Type, cfg.Config)
}

func createSink(cfg config.ComponentConfig) (pipeline.Sink, error) {
	switch cfg.Type {
	case "sqlserver":
		dsn := getString(cfg.Config, "dsn")
		table := getString(cfg.Config, "table")

		var fields []sqlserver.FieldMapping
		if fList, ok := cfg.Config["fields"].([]interface{}); ok {
			for _, f := range fList {
				if fMap, ok := f.(map[string]interface{}); ok {
					fields = append(fields, sqlserver.FieldMapping{
						Source: getString(fMap, "source"),
						Target: getString(fMap, "target"),
					})
				}
			}
		}
		return sqlserver.NewSQLServerSink(dsn, table, fields)
	case "elasticsearch":
		addresses := getStringSlice(cfg.Config, "addresses")
		index := getString(cfg.Config, "index")
		username := getString(cfg.Config, "username")
		password := getString(cfg.Config, "password")
		return elasticsearch.NewElasticsearchSink(addresses, index, username, password)
	default:
		return nil, fmt.Errorf("unknown sink type: %s", cfg.Type)
	}
}

// Helpers
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getStringSlice(m map[string]interface{}, key string) []string {
	var res []string
	if v, ok := m[key]; ok {
		if slice, ok := v.([]interface{}); ok {
			for _, item := range slice {
				if s, ok := item.(string); ok {
					res = append(res, s)
				}
			}
		}
	}
	return res
}
