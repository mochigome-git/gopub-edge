package utils

import (
	"testing"
)

func TestSafeJsonPayloads_SetAndGet(t *testing.T) {
	payloads := NewSafeJsonPayloads()

	key := "testKey"
	value := "testValue"

	payloads.Set(key, value)
	got, exists := payloads.Get(key)

	if !exists {
		t.Errorf("Expected key '%s' to exist", key)
	}
	if got != value {
		t.Errorf("Expected value '%v', got '%v'", value, got)
	}
}

func TestSafeJsonPayloads_Delete(t *testing.T) {
	payloads := NewSafeJsonPayloads()

	key := "toDelete"
	payloads.Set(key, 123)

	payloads.Delete(key)
	_, exists := payloads.Get(key)

	if exists {
		t.Errorf("Expected key '%s' to be deleted", key)
	}
}

func TestSafeJsonPayloads_GetFloat64(t *testing.T) {
	payloads := NewSafeJsonPayloads()

	key := "pi"
	value := 3.14
	payloads.Set(key, value)

	f, ok := payloads.GetFloat64(key)
	if !ok || f != value {
		t.Errorf("Expected float64 value %v, got %v", value, f)
	}

	// Check type mismatch
	payloads.Set("wrongType", "notFloat")
	_, ok = payloads.GetFloat64("wrongType")
	if ok {
		t.Error("Expected type assertion to float64 to fail")
	}
}

func TestSafeJsonPayloads_GetString(t *testing.T) {
	payloads := NewSafeJsonPayloads()

	key := "greeting"
	value := "hello"
	payloads.Set(key, value)

	s, ok := payloads.GetString(key)
	if !ok || s != value {
		t.Errorf("Expected string value '%v', got '%v'", value, s)
	}

	// Check type mismatch
	payloads.Set("wrongType", 42)
	s, ok = payloads.GetString("wrongType")
	if !ok || s != "42" {
		t.Errorf("Expected '42', got '%v'", s)
	}

}

func TestSafeJsonPayloads_GetData(t *testing.T) {
	payloads := NewSafeJsonPayloads()
	payloads.Set("a", 1)
	payloads.Set("b", "test")

	data := payloads.GetData()
	if len(data) != 2 {
		t.Errorf("Expected 2 items in map, got %d", len(data))
	}
	if data["a"] != 1 || data["b"] != "test" {
		t.Errorf("Unexpected data returned: %v", data)
	}
}

func TestSafeJsonPayloads_GetDC(t *testing.T) {
	payloads := NewSafeJsonPayloads()

	// Setup original data
	originalMap := map[string]interface{}{"key": "value"}
	originalSlice := []interface{}{1, 2, 3}
	payloads.Set("map", originalMap)
	payloads.Set("slice", originalSlice)
	payloads.Set("number", 42)
	payloads.Set("text", "hello")

	// Test map copy
	gotMap, ok := payloads.GetDC("map")
	if !ok {
		t.Fatal("Expected to find key 'map'")
	}
	copiedMap, ok := gotMap.(map[string]interface{})
	if !ok {
		t.Fatal("Expected map[string]interface{} type")
	}
	copiedMap["key"] = "changed"
	if originalMap["key"] == "changed" {
		t.Error("Original map was modified — deep copy failed")
	}

	// Test slice copy
	gotSlice, ok := payloads.GetDC("slice")
	if !ok {
		t.Fatal("Expected to find key 'slice'")
	}
	copiedSlice, ok := gotSlice.([]interface{})
	if !ok {
		t.Fatal("Expected []interface{} type")
	}
	copiedSlice[0] = 999
	if originalSlice[0] == 999 {
		t.Error("Original slice was modified — deep copy failed")
	}

	// Test primitive types
	if val, ok := payloads.GetDC("number"); !ok || val != 42 {
		t.Error("Expected number value 42")
	}
	if val, ok := payloads.GetDC("text"); !ok || val != "hello" {
		t.Error("Expected string value 'hello'")
	}

	// Test missing key
	if val, ok := payloads.GetDC("missing"); ok || val != nil {
		t.Error("Expected missing key to return nil, false")
	}
}
