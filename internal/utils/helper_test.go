package utils

import (
	"os"
	"reflect"
	"testing"
)

// ---- Test for reverseString ----
func TestReverseString(t *testing.T) {
	input := "abc123"
	expected := "321cba"

	result := reverseString(input)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// ---- Test for GetKeyTransformationsFromEnv ----
func TestGetKeyTransformationsFromEnv(t *testing.T) {
	os.Setenv("KEY_TRANSFORMATION_TEST", "d100")
	os.Setenv("KEY_TRANSFORMATION_EXAMPLE", "d101")

	result := GetKeyTransformationsFromEnv("KEY_TRANSFORMATION_")

	expected := map[string]string{
		"TEST":    "d100",
		"EXAMPLE": "d101",
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// ---- Test for parseTriggerKey ----
func TestParseTriggerKey(t *testing.T) {
	input := "trigger1,4,trigger2,7"
	result := ParseTriggerKey(input)
	expected := []TriggerKey{
		{"trigger1", "4"},
		{"trigger2", "7"},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestCompareAndUpdateNestedMap(t *testing.T) {
	tests := []struct {
		name         string
		parentMap    map[string]map[string]interface{}
		parentKey    string
		updateData   map[string]interface{}
		keysToCheck  []string
		initialPrev  float64
		expectedMap  map[string]map[string]interface{}
		expectedPrev float64
	}{
		{
			name: "Updates larger values only",
			parentMap: map[string]map[string]interface{}{
				"machine1": {"ch1_weighing": 120.0},
			},
			parentKey: "machine1",
			updateData: map[string]interface{}{
				"ch1_weighing": 200.0,
			},
			keysToCheck: []string{"ch1_weighing"},
			initialPrev: 110.0,
			expectedMap: map[string]map[string]interface{}{
				"machine1": {"ch1_weighing": 200.0},
			},
			expectedPrev: 200.0,
		},
		{
			name: "No updates because all new values are smaller",
			parentMap: map[string]map[string]interface{}{
				"machine2": {"ch2_weighing": 400.0},
			},
			parentKey: "machine2",
			updateData: map[string]interface{}{
				"ch2_weighing": 350.0,
			},
			keysToCheck: []string{"ch2_weighing"},
			initialPrev: 100.0,
			expectedMap: map[string]map[string]interface{}{
				"machine2": {"ch2_weighing": 400.0},
			},
			expectedPrev: 400.0,
		},
		{
			name: "Skip non-float values and zeros",
			parentMap: map[string]map[string]interface{}{
				"machine3": {"ch1_weighing": 50.0},
			},
			parentKey: "machine3",
			updateData: map[string]interface{}{
				"ch1_weighing": 0.0,
			},
			keysToCheck: []string{"ch1_weighing", "ch2_weighing"},
			initialPrev: 10.0,
			expectedMap: map[string]map[string]interface{}{
				"machine3": {"ch1_weighing": 50.0},
			},
			expectedPrev: 50.0,
		},
		{
			name: "PrevWeight is 0, sets from existing and updates",
			parentMap: map[string]map[string]interface{}{
				"machine6": {"ch1_weighing": 80.0},
			},
			parentKey: "machine6",
			updateData: map[string]interface{}{
				"ch1_weighing": 90.0,
			},
			keysToCheck: []string{"ch1_weighing"},
			initialPrev: 0.0,
			expectedMap: map[string]map[string]interface{}{
				"machine6": {"ch1_weighing": 90.0},
			},
			expectedPrev: 90.0,
		},
		{
			name: "Parent key not found in parentMap",
			parentMap: map[string]map[string]interface{}{
				"machine5": {"ch1_weighing": 70.0},
			},
			parentKey: "machineX", // wrong key
			updateData: map[string]interface{}{
				"ch1_weighing": 90.0,
			},
			keysToCheck: []string{"ch1_weighing"},
			initialPrev: 30.0,
			expectedMap: map[string]map[string]interface{}{
				"machine5": {"ch1_weighing": 70.0},
			},
			expectedPrev: 30.0,
		},
		{
			name: "No matching keys in updateData",
			parentMap: map[string]map[string]interface{}{
				"machine4": {"ch1_weighing": 50.0},
			},
			parentKey: "machine4",
			updateData: map[string]interface{}{
				"chX_weighing": 100.0,
			},
			keysToCheck: []string{"ch1_weighing"},
			initialPrev: 20.0,
			expectedMap: map[string]map[string]interface{}{
				"machine4": {"ch1_weighing": 50.0},
			},
			expectedPrev: 50.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function with the provided inputs
			prev := tt.initialPrev
			CompareAndUpdateNestedMap(tt.parentMap, tt.parentKey, tt.updateData, tt.keysToCheck, &prev)

			// Simplified comparison of the expected results
			assertEqual(t, tt.parentMap, tt.expectedMap, "parentMap mismatch")
			assertEqual(t, prev, tt.expectedPrev, "prevWeightValue mismatch")
		})
	}
}

// Helper function to compare results and report errors
func assertEqual(t *testing.T, got, want interface{}, message string) {
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s. Got %+v, want %+v", message, got, want)
	}
}
