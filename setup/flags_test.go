package setup

import (
	"flag"
	"os"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	workerImage, workers := ParseFlags()

	if workerImage != "ghcr.io/distcodep7/dsnet:latest" {
		t.Errorf("Expected default image 'ghcr.io/distcodep7/dsnet:latest', got '%s'", workerImage)
	}
	if workers != defaultNumWorkers {
		t.Errorf("Expected default workers %d, got %d", defaultNumWorkers, workers)
	}

}

func TestParseFlagsWithCustomValues(t *testing.T) {
	// Simulate command-line arguments
	flag.CommandLine = flag.NewFlagSet("test", flag.ExitOnError)
	// Save original os.Args and restore after test
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"cmd", "-iw", "customworkerimage:latest", "-w", "10"}

	workerImage, workers := ParseFlags()

	if workerImage != "customworkerimage:latest" {
		t.Errorf("Expected image 'customworkerimage:latest', got '%s'", workerImage)
	}
	if workers != 10 {
		t.Errorf("Expected workers 10, got %d", workers)
	}
}
