package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type PipelineConfig struct {
	Source       ComponentConfig   `yaml:"source"`
	Processors   []ComponentConfig `yaml:"processors"`
	Sink         ComponentConfig   `yaml:"sink"`
	WorkerCount  int               `yaml:"worker_count"`
	BatchSize    int               `yaml:"batch_size"`
	BatchTimeout time.Duration     `yaml:"batch_timeout"`
}

type ComponentConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
