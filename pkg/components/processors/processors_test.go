package processors

import (
	"datapipeline/pkg/pipeline"
	"testing"
)

func TestProcessors(t *testing.T) {
	// 1. Test JSON Parser
	t.Run("JSONParser", func(t *testing.T) {
		p := &JSONParser{}
		msg := pipeline.Message{
			Data: map[string]interface{}{
				"raw": []byte(`{"name": "John", "age": 30}`),
			},
		}
		res, err := p.Process(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Data["name"] != "John" {
			t.Errorf("expected name John, got %v", res.Data["name"])
		}
		// Check number conversion (json unmarshal uses float64)
		if res.Data["age"] != 30.0 {
			t.Errorf("expected age 30, got %v", res.Data["age"])
		}
	})

	// 2. Test Field Mapper
	t.Run("FieldMapper", func(t *testing.T) {
		p := &FieldMapper{
			Mapping: map[string]string{"old": "new"},
		}
		msg := pipeline.Message{
			Data: map[string]interface{}{"old": "value"},
		}
		res, _ := p.Process(msg)
		if _, ok := res.Data["old"]; ok {
			t.Error("old field should be removed")
		}
		if res.Data["new"] != "value" {
			t.Error("new field should have value")
		}
	})

	// 3. Test Regex Replacer
	t.Run("RegexReplacer", func(t *testing.T) {
		p, _ := NewRegexReplacer(map[string]interface{}{
			"field":       "email",
			"pattern":     "(.*)@(.*)",
			"replacement": "***@$2",
		})
		msg := pipeline.Message{
			Data: map[string]interface{}{"email": "john@example.com"},
		}
		res, _ := p.Process(msg)
		if res.Data["email"] != "***@example.com" {
			t.Errorf("expected masked email, got %v", res.Data["email"])
		}
	})

	// 4. Test Filter
	t.Run("Filter", func(t *testing.T) {
		p := &Filter{Field: "age", Operator: ">", Value: 18.0}

		// Pass
		msg1 := pipeline.Message{Data: map[string]interface{}{"age": 20.0}}
		_, err1 := p.Process(msg1)
		if err1 != nil {
			t.Errorf("should pass: %v", err1)
		}

		// Fail
		msg2 := pipeline.Message{Data: map[string]interface{}{"age": 10.0}}
		_, err2 := p.Process(msg2)
		if err2 == nil {
			t.Error("should fail")
		}
	})
}
