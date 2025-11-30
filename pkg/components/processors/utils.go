package processors

import (
	"fmt"
	"strings"
)

// GetValue retrieves a value from a nested map using dot notation.
func GetValue(data map[string]interface{}, path string) interface{} {
	keys := strings.Split(path, ".")
	var current interface{} = data

	for _, key := range keys {
		if m, ok := current.(map[string]interface{}); ok {
			if val, exists := m[key]; exists {
				current = val
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	return current
}

// SetValue sets a value in a nested map using dot notation.
// It creates intermediate maps if they don't exist.
func SetValue(data map[string]interface{}, path string, value interface{}) error {
	keys := strings.Split(path, ".")
	if len(keys) == 0 {
		return fmt.Errorf("empty path")
	}

	var current map[string]interface{} = data
	for i := 0; i < len(keys)-1; i++ {
		key := keys[i]
		if val, exists := current[key]; exists {
			if m, ok := val.(map[string]interface{}); ok {
				current = m
			} else {
				return fmt.Errorf("path segment '%s' is not a map", key)
			}
		} else {
			// Create new map
			newMap := make(map[string]interface{})
			current[key] = newMap
			current = newMap
		}
	}

	current[keys[len(keys)-1]] = value
	return nil
}
