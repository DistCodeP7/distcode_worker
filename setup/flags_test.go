package setup

import (
	"flag"
	"os"
	"testing"
)

func TestParseFlagsDefualts(t *testing.T) {
	image, workers, capacity := ParseFlags()

	if image != "golang:1.25" {
		t.Errorf("Expected default image 'golang:1.25', got '%s'", image)
	}
	if workers != 4 {
		t.Errorf("Expected default workers 4, got %d", workers)
	}
	if capacity != 30 {
		t.Errorf("Expected default capacity 30, got %d", capacity)
	}
}

func TestParseFlagsWithCustomValues(t *testing.T) {
	// Simulate command-line arguments
	flag.CommandLine = flag.NewFlagSet("test", flag.ExitOnError)
	// Save original os.Args and restore after test
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"cmd", "-i", "customimage:latest", "-w", "10", "-c", "50"}

	image, workers, capacity := ParseFlags()

	if image != "customimage:latest" {
		t.Errorf("Expected image 'customimage:latest', got '%s'", image)
	}
	if workers != 10 {
		t.Errorf("Expected workers 10, got %d", workers)
	}
	if capacity != 50 {
		t.Errorf("Expected capacity 50, got %d", capacity)
	}
}
