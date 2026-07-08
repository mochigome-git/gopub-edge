package config

import (
	"os"
	"testing"
)

// TestLoadEnv verifies that environment variables are correctly loaded from a file
func TestLoadEnv(t *testing.T) {
	// Create a temporary .env file for testing
	fileName := ".env.test"
	content := "TEST_ENV_KEY=env_value"

	err := os.WriteFile(fileName, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test .env file: %v", err)
	}
	defer os.Remove(fileName) // Clean up after test

	// Load the test env file
	Load(fileName)

	// Check the loaded value
	value := os.Getenv("TEST_ENV_KEY")
	if value != "env_value" {
		t.Errorf("Expected 'env_value', got '%s'", value)
	}
}
