package setup

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestHandleSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleSignals(cancel)

	// Simulate sending an interrupt signal to this process
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("Failed to find process: %v", err)
	}
	if err := p.Signal(os.Interrupt); err != nil {
		t.Fatalf("Failed to send interrupt signal: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("Context was not cancelled after sending interrupt signal")
	}
}

func TestHandleSignals_SIGTERM(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleSignals(cancel)

	// Send SIGTERM to the current process
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("failed to find process: %v", err)
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("context was not cancelled after SIGTERM")
	}
}
