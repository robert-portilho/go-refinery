package processors

import (
	"datapipeline/pkg/pipeline"
	"encoding/json"
	"fmt"
)

func init() {
	RegisterProcessor("json_parser", NewJSONParser)
}

// --- JSON Parser ---

type JSONParser struct{}

func NewJSONParser(config map[string]interface{}) (pipeline.Processor, error) {
	return &JSONParser{}, nil
}

func (p *JSONParser) Process(msg pipeline.Message) (pipeline.Message, error) {
	raw, ok := msg.Data["raw"]
	if !ok {
		return msg, fmt.Errorf("field 'raw' not found in message data")
	}

	bytes, ok := raw.([]byte)
	if !ok {
		// Tenta string se não for byte
		str, ok := raw.(string)
		if ok {
			bytes = []byte(str)
		} else {
			return msg, fmt.Errorf("field 'raw' is not []byte or string")
		}
	}

	var data map[string]interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return msg, fmt.Errorf("failed to parse json: %w", err)
	}

	// Merge parsed data into message data, potentially overwriting 'raw' if needed,
	// but usually we want to keep raw or just expand.
	// Let's expand into the root of Data
	for k, v := range data {
		msg.Data[k] = v
	}

	return msg, nil
}
