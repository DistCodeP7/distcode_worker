package setup

import "flag"

// ParseFlags parses command-line flags for configuring the worker image, number of workers, and jobs channel capacity.
// It returns the Docker image name, the number of worker goroutines, and the jobs channel capacity as specified by the flags.
// Flags:
//
//	-i string: The Docker image to use for workers (default "golang:1.25").
//	-w int: The number of worker goroutines to start (default 4).
//	-c int: The capacity of the jobs channel (default 2 * workers).
func ParseFlags() (string, int, int) {
	workerImageName := flag.String("i", "golang:1.25", "The Docker image to use for workers")
	numWorkers := flag.Int("w", 4, "The number of worker goroutines to start")
	jobsCapacityFlag := flag.Int("c", -1, "The capacity of the jobs channel (optional)")

	flag.Parse()
	jobsCapacity := *jobsCapacityFlag
	if jobsCapacity == -1 {
		jobsCapacity = 2 * *numWorkers
	}

	return *workerImageName, *numWorkers, jobsCapacity
}
