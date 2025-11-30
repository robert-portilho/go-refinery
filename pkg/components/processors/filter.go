package processors

import (
	"datapipeline/pkg/pipeline"
	"fmt"
)

func init() {
	RegisterProcessor("filter", NewFilter)
}

// --- Filter ---

type Filter struct {
	Field    string
	Operator string
	Value    interface{}
}

func NewFilter(config map[string]interface{}) (pipeline.Processor, error) {
	field := getString(config, "field")
	operator := getString(config, "operator")
	value := config["value"]
	return &Filter{Field: field, Operator: operator, Value: value}, nil
}

func (p *Filter) Process(msg pipeline.Message) (pipeline.Message, error) {
	val, ok := msg.Data[p.Field]
	if !ok {
		// Se o campo não existe, o que fazer? Por padrão, vamos deixar passar ou falhar?
		// Vamos assumir que se o campo não existe, o filtro falha (descarta mensagem).
		return msg, fmt.Errorf("filter field '%s' missing", p.Field)
	}

	// Simplificação: suportando apenas string equality e float comparison por enquanto
	switch p.Operator {
	case "==":
		if val != p.Value {
			return msg, fmt.Errorf("filter condition failed: %v != %v", val, p.Value)
		}
	case ">":
		// Assumindo float64 (padrão do JSON decode para números)
		vFloat, ok1 := val.(float64)
		tFloat, ok2 := p.Value.(float64)
		if ok1 && ok2 {
			if !(vFloat > tFloat) {
				return msg, fmt.Errorf("filter condition failed: %v <= %v", vFloat, tFloat)
			}
		}
	}

	return msg, nil
}
