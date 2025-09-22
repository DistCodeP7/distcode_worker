package go_testing

import (
	"testing"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
)

func TestWorkerProcessing(t *testing.T) {
	tests := []struct {
		name      string
		msg       types.Message
		shouldErr bool
	}{
		{"valid payload", types.Message{ID: 1, Payload: "hello"}, false},
		{"invalid payload", types.Message{ID: 2, Payload: ""}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := worker.Process(tc.msg)
			if tc.shouldErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.shouldErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
