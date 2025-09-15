package main

import (
	"context"
	"fmt"
	"log"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

func main() {
    ctx := context.Background()

    cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
    if err != nil {
        log.Fatalf("Error creating Docker client: %v", err)
    }

    // Use the correct ListOptions from image package
    images, err := cli.ImageList(ctx, image.ListOptions{})
    if err != nil {
        log.Fatalf("Error listing images: %v", err)
    }

    if len(images) == 0 {
        fmt.Println("No Docker images found locally.")
        return
    }

    fmt.Println("Local Docker images:")
    for _, img := range images {
        tags := img.RepoTags
        if len(tags) == 0 {
            tags = []string{"<none>:<none>"}
        }
        for _, tag := range tags {
            fmt.Printf(" - %s (ID: %.12s) Size: %.2f MB\n", tag, img.ID, float64(img.Size)/(1024*1024))
        }
    }
}
