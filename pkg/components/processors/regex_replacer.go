package processors

import (
	"datapipeline/pkg/pipeline"
	"regexp"
)

func init() {
	RegisterProcessor("regex_replace", NewRegexReplacer)
}

// --- Regex Replacer ---

type RegexReplacer struct {
	Field       string
	Pattern     *regexp.Regexp
	Replacement string
}

func NewRegexReplacer(config map[string]interface{}) (pipeline.Processor, error) {
	field := getString(config, "field")
	pattern := getString(config, "pattern")
	replacement := getString(config, "replacement")

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return &RegexReplacer{
		Field:       field,
		Pattern:     re,
		Replacement: replacement,
	}, nil
}

func (p *RegexReplacer) Process(msg pipeline.Message) (pipeline.Message, error) {
	val := GetValue(msg.Data, p.Field)
	if val != nil {
		if strVal, ok := val.(string); ok {
			newVal := p.Pattern.ReplaceAllString(strVal, p.Replacement)
			if err := SetValue(msg.Data, p.Field, newVal); err != nil {
				return msg, err
			}
		}
	}
	return msg, nil
}

// Helper function to safely get string from map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
