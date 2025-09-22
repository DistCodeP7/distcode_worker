package go_testing

import (
	"testing"

	"github.com/DistCodeP7/distcode_worker/types"
)

func TestMessageValidation(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		shouldErr bool
	}{
		{"valid message", `{"id":1,"payload":"hello"}`, false},
		{"missing field", `{"payload":"hello"}`, true},
		{"wrong type", `{"id":"not-an-int","payload":"hello"}`, true},
		{"empty json", `{}`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := types.ParseMessage([]byte(tc.input))
			if tc.shouldErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.shouldErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
