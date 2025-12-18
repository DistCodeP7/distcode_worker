package setup

import "flag"

const (
	defaultWorkerImage = "ghcr.io/distcodep7/dsnet:latest"
	defaultNumWorkers  = 10
)

// ParseFlags parses command-line flags for configuring the worker image, number of workers, and jobs channel capacity.
// It returns the Docker image name, the number of worker goroutines, and the jobs channel capacity as specified by the flags.
// Flags:
//
//	-iWorker string: The Docker image to use for workers (default "golang:1.25").
//	-iController string: The Docker image to use for the DSNet controller.
//	-w int: The number of worker goroutines to start (default 4).
//	-c int: The capacity of the jobs channel (default 2 * workers).
func ParseFlags() (string, int) {
	workerImageName := flag.String("iw", defaultWorkerImage, "The Docker image to use for workers")
	numWorkers := flag.Int("w", defaultNumWorkers, "The number of worker goroutines to start")

	if *workerImageName == "" {
		panic("DSNet worker image not found")
	}

	flag.Parse()
	return *workerImageName, *numWorkers
}
