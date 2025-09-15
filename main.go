package main

import (
	"fmt"
	"log"

	"github.com/DistCodeP7/distcode_worker/worker"
)

func main() {
	code := `package main

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
	fmt.Printf("First %d Fibonacci numbers:\n", n)
	for i, val := range fibonacci(n) {
		fmt.Printf("%d: %d\n", i, val)
	}
}`

	stdout, stderr, err := worker.RunSubmission(code)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println("STDOUT:\n", stdout)
	fmt.Println("STDERR:\n", stderr)
}
