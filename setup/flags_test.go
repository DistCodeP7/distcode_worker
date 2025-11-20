package setup

import (
	"flag"
	"os"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	workerImage, controllerImage, workers, capacity := ParseFlags()

	expectedWorkers := 4

	if workerImage != "ghcr.io/distcodep7/dsnet:latest" {
		t.Errorf("Expected default image 'ghcr.io/distcodep7/dsnet:latest', got '%s'", workerImage)
	}
	if controllerImage != "ghcr.io/distcodep7/dsnet-controller:latest" {
		t.Errorf("Expected default controller image 'ghcr.io/distcodep7/dsnet-controller:latest', got '%s'", controllerImage)
	}
	if workers != expectedWorkers {
		t.Errorf("Expected default workers 4, got %d", workers)
	}
	if capacity != expectedWorkers*2 {
		t.Errorf("Expected default capacity 2 * %d, got %d", expectedWorkers, capacity)
	}
}

func TestParseFlagsWithCustomValues(t *testing.T) {
	// Simulate command-line arguments
	flag.CommandLine = flag.NewFlagSet("test", flag.ExitOnError)
	// Save original os.Args and restore after test
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"cmd", "-iw", "customworkerimage:latest", "-ic", "customcontrollerimage:latest", "-w", "10", "-c", "50"}

	workerImage, controllerImage, workers, capacity := ParseFlags()

	if workerImage != "customworkerimage:latest" {
		t.Errorf("Expected image 'customworkerimage:latest', got '%s'", workerImage)
	}
	if controllerImage != "customcontrollerimage:latest" {
		t.Errorf("Expected controller image 'customcontrollerimage:latest', got '%s'", controllerImage)
	}
	if workers != 10 {
		t.Errorf("Expected workers 10, got %d", workers)
	}
	if capacity != 50 {
		t.Errorf("Expected capacity 50, got %d", capacity)
	}
}
