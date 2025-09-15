package worker

import (
	"strings"
	"testing"
)

func TestRunSubmissionTable(t *testing.T) {
	tests := []struct {
		name           string
		code           string
		expectedStdout string
		expectedStderr string
		expectErr      bool
	}{
		{
			name: "HelloWorld",
			code: `package main
import "fmt"
func main() {
	fmt.Println("Hello test!")
}`,
			expectedStdout: "Hello test!\n",
			expectedStderr: "",
			expectErr:      false,
		},
		{
			name: "Fibonacci10",
			code: `package main
import "fmt"
func fibonacci(n int) []int {
	fibs := make([]int, n)
	fibs[0], fibs[1] = 0, 1
	for i := 2; i < n; i++ {
		fibs[i] = fibs[i-1] + fibs[i-2]
	}
	return fibs
}
func main() {
	n := 10
	for i, val := range fibonacci(n) {
		fmt.Printf("%d:%d\n", i, val)
	}
}`,
			expectedStdout: "0:0\n1:1\n2:1\n3:2\n4:3\n5:5\n6:8\n7:13\n8:21\n9:34\n",
			expectedStderr: "",
			expectErr:      false,
		},
		{
			name: "ConcurrencyWorkers",
			code: `package main
import (
	"fmt"
	"time"
)
func worker(id int, ch chan int) {
	for i := 0; i < 5; i++ {
		time.Sleep(time.Millisecond * 10)
		fmt.Printf("Worker %d:%d\n", id, i)
		ch <- i
	}
}
func main() {
	ch := make(chan int)
	for w := 1; w <= 3; w++ {
		go worker(w, ch)
	}
	for i := 0; i < 15; i++ {
		<-ch
	}
	fmt.Println("All workers done")
}`,
			expectedStdout: "All workers done\n", // Only final line; intermediate order may vary
			expectedStderr: "",
			expectErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := RunSubmission(tt.code)

			if (err != nil) != tt.expectErr {
				t.Fatalf("expected error: %v, got: %v", tt.expectErr, err)
			}

			// For concurrency workers, we only check that the final line exists
			if tt.name == "ConcurrencyWorkers" {
				if !strings.Contains(stdout, tt.expectedStdout) {
					t.Errorf("expected stdout to contain %q, got %q", tt.expectedStdout, stdout)
				}
			} else {
				if stdout != tt.expectedStdout {
					t.Errorf("expected stdout %q, got %q", tt.expectedStdout, stdout)
				}
			}

			if stderr != tt.expectedStderr {
				t.Errorf("expected stderr %q, got %q", tt.expectedStderr, stderr)
			}
		})
	}
}
