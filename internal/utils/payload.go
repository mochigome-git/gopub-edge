package utils

import (
	"fmt"
	"strings"
	"sync"
)

type JsonPayloads map[string]any

type SafeJsonPayloads struct {
	mu   sync.RWMutex
	data JsonPayloads
}

func NewSafeJsonPayloads() *SafeJsonPayloads {
	return &SafeJsonPayloads{
		data: make(JsonPayloads),
	}
}

func (s *SafeJsonPayloads) Get(key string) (any, bool) {
	s.mu.RLock() // Lock for reading
	defer s.mu.RUnlock()
	val, exists := s.data[key]
	return val, exists
}

func (s *SafeJsonPayloads) Set(key string, value any) {
	s.mu.Lock() // Lock for writing
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *SafeJsonPayloads) Delete(key string) {
	s.mu.Lock() // Lock for writing
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *SafeJsonPayloads) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.data {
		delete(s.data, k)
	}
}

func (s *SafeJsonPayloads) GetBool(key string) (bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.data[key]
	if !exists {
		return false, false
	}

	switch v := val.(type) {
	case bool:
		return v, true
	case string:
		switch strings.TrimSpace(strings.ToLower(v)) {
		case "true", "1", "yes", "y", "on":
			return true, true
		case "false", "0", "no", "n", "off":
			return false, true
		default:
			return false, false
		}
	case int:
		return v != 0, true
	case int64:
		return v != 0, true
	case float64:
		return v != 0, true
	default:
		return false, false
	}
}

func (s *SafeJsonPayloads) GetFloat64(key string) (float64, bool) {
	s.mu.RLock() // Lock for reading
	defer s.mu.RUnlock()
	val, exists := s.data[key]
	if !exists {
		return 0, false
	}
	f, ok := val.(float64)
	return f, ok
}

// func (s *SafeJsonPayloads) GetString(key string) (string, bool) {
// 	s.mu.RLock() // Lock for reading
// 	defer s.mu.RUnlock()
// 	if val, ok := s.data[key]; ok {
// 		strVal, ok := val.(string)
// 		return strVal, ok
// 	}
// 	return "", false
// }

func (s *SafeJsonPayloads) GetString(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		trimmedKey := strings.TrimSpace(key)
		for k := range s.data {
			if strings.EqualFold(trimmedKey, strings.TrimSpace(k)) {
				val = s.data[k]
				ok = true
				break
			}
		}
	}

	if !ok {
		return "", false
	}

	switch v := val.(type) {
	case string:
		return v, true
	case fmt.Stringer:
		return v.String(), true
	case []byte:
		return string(v), true
	case int, int64, float64, bool:
		return fmt.Sprint(v), true
	default:
		return "", false
	}
}

func (s *SafeJsonPayloads) GetData() map[string]any {
	s.mu.RLock() // Lock for reading
	defer s.mu.RUnlock()

	copyMap := make(map[string]any, len(s.data))
	for k, v := range s.data {
		copyMap[k] = v
	}
	return copyMap
}

func (s *SafeJsonPayloads) GetDC(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.data[key]
	if !exists {
		return nil, false
	}

	// Perform a shallow copy for basic types or use deep copy logic for composite types
	switch v := val.(type) {
	case map[string]any:
		copyMap := make(map[string]any, len(v))
		for k, val := range v {
			copyMap[k] = val // note: values aren't deep-copied
		}
		return copyMap, true
	case []any:
		copySlice := make([]any, len(v))
		copy(copySlice, v)
		return copySlice, true
	default:
		// For basic types (int, float64, string, bool, etc.), return as is
		return v, true
	}
}

func (s *SafeJsonPayloads) Range(fn func(key string, val any)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.data {
		fn(k, v)
	}
}

func (s *SafeJsonPayloads) Data() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return a copy to avoid race
	copy := make(map[string]any, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}
	return copy
}

func (s *SafeJsonPayloads) GetAny(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.data[key]; ok {
		return v, true
	}
	for k := range s.data {
		if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(k)) {
			return s.data[k], true
		}
	}
	return nil, false
}
