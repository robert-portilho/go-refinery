package processors

import (
	"datapipeline/pkg/pipeline"
	"fmt"
)

// ProcessorFactoryFunc is a function that creates a new Processor instance.
type ProcessorFactoryFunc func(config map[string]interface{}) (pipeline.Processor, error)

var processorRegistry = make(map[string]ProcessorFactoryFunc)

// RegisterProcessor registers a new processor type with its factory function.
// This should be called in the init() function of each processor implementation.
func RegisterProcessor(processorType string, factory ProcessorFactoryFunc) {
	if _, exists := processorRegistry[processorType]; exists {
		panic(fmt.Sprintf("processor type '%s' is already registered", processorType))
	}
	processorRegistry[processorType] = factory
}

// CreateProcessor creates a new processor instance based on the type and configuration.
func CreateProcessor(processorType string, config map[string]interface{}) (pipeline.Processor, error) {
	factory, exists := processorRegistry[processorType]
	if !exists {
		return nil, fmt.Errorf("unknown processor type: %s", processorType)
	}
	return factory(config)
}
