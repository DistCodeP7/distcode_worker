package setup

import "flag"

// ParseFlags parses command-line flags for configuring the worker image, number of workers, and jobs channel capacity.
// It returns the Docker image name, the number of worker goroutines, and the jobs channel capacity as specified by the flags.
// Flags:
//
//	-i string: The Docker image to use for workers (default "golang:1.25").
//	-w int: The number of worker goroutines to start (default 4).
//	-c int: The capacity of the jobs channel (default 30).
func ParseFlags() (string, int, int) {
	// Recieve the image name from command line flag e.g `go run main.go -i golang:1.25`
	workerImageName := flag.String("i", "golang:1.25", "The Docker image to use for workers")
	numWorkers := flag.Int("w", 4, "The number of worker goroutines to start")
	jobsCapacity := flag.Int("c", 30, "The capacity of the jobs channel")

	flag.Parse()
	return *workerImageName, *numWorkers, *jobsCapacity
}
