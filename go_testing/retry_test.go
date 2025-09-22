package go_testing

import (
	"errors"
	"testing"

	"github.com/DistCodeP7/distcode_worker/worker"
)

func TestWorkerRetries(t *testing.T) {
	attempts := 0
	fakeProcess := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		return nil
	}

	err := worker.WithRetry(fakeProcess, 5)
	if err != nil {
		t.Errorf("expected success after retries, got error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestWorkerPermanentFailure(t *testing.T) {
	attempts := 0
	fakeProcess := func() error {
		attempts++
		return errors.New("permanent error")
	}

	err := worker.WithRetry(fakeProcess, 3)
	if err == nil {
		t.Errorf("expected error but got nil")
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestWorkerRetryWithZeroAttempts(t *testing.T) {
	attempts := 0
	fakeProcess := func() error {
		attempts++
		return errors.New("error")
	}

	err := worker.WithRetry(fakeProcess, 0)
	if err == nil {
		t.Error("expected error with zero retries")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}
