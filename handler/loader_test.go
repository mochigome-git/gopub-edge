package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

// Mocking the SafeJsonPayloads
type MockSafeJsonPayloads struct {
	mock.Mock
}

func (m *MockSafeJsonPayloads) Set(key string, value interface{}) {
	m.Called(key, value)
}

func (m *MockSafeJsonPayloads) GetData() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func TestPrettyPrintJSONWithTime(t *testing.T) {
	// Sample data for testing prettyPrintJSONWithTime
	data := map[string]interface{}{
		"device": "device_1",
		"value":  "value_1",
	}

	// Test the function with a map[string]interface{} type
	startTime := time.Now()
	prettyPrintJSONWithTime(data, time.Since(startTime))

	// Test with SafeJsonPayloads (mock)
	mockJsonPayloads := new(MockSafeJsonPayloads)
	mockJsonPayloads.On("GetData").Return(data)

	// Test the function with SafeJsonPayloads type
	prettyPrintJSONWithTime(mockJsonPayloads, time.Since(startTime))
}
