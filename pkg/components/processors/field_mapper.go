package processors

import (
	"datapipeline/pkg/pipeline"
)

func init() {
	RegisterProcessor("rename_field", NewFieldMapper)
}

// --- Field Mapper ---

type FieldMapper struct {
	Mapping map[string]string // OldName -> NewName
}

func NewFieldMapper(config map[string]interface{}) (pipeline.Processor, error) {
	mapping := make(map[string]string)
	if m, ok := config["mapping"].(map[string]interface{}); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				mapping[k] = s
			}
		}
	}
	return &FieldMapper{Mapping: mapping}, nil
}

func (p *FieldMapper) Process(msg pipeline.Message) (pipeline.Message, error) {
	for oldName, newName := range p.Mapping {
		if val, ok := msg.Data[oldName]; ok {
			msg.Data[newName] = val
			delete(msg.Data, oldName)
		}
	}
	return msg, nil
}
