package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

func runSubmission(code string) (string, string, error) {
	ctx := context.Background()

	log.Println("Step 1: Creating temp dir")
	tmpDir := filepath.Join(os.Getenv("HOME"), "docker-work")
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	codePath := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return "", "", err
	}
	log.Printf("Step 2: Wrote code to %s\n", codePath)

	log.Println("Step 3: Connecting to Docker")
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", "", err
	}

	log.Println("Step 4: Checking image")
	_, err = cli.ImageInspect(ctx, "golang:1.22")
	if err != nil {
		log.Println("Image not found locally, pulling...")
		cmd := exec.Command("docker", "pull", "golang:1.22")
		if err := cmd.Run(); err != nil {
			return "", "", fmt.Errorf("failed to pull image: %v", err)
		} else {
			log.Println("Image pulled successfully")
		}
	}

	log.Println("Step 5: Creating container")
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:      "golang:1.22",
		Cmd:        []string{"go", "run", "main.go"},
		Tty:        false,
		WorkingDir: "/app",
	}, &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: tmpDir, // fixed folder
				Target: "/app",
			},
		},
		Resources: container.Resources{
			Memory:   512 * 1024 * 1024,
			NanoCPUs: 1_000_000_000,
		},
	}, nil, nil, "")

	if err != nil {
		return "", "", err
	}

	defer func() {
		log.Println("Cleaning up container")
		_ = cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
	}()

	log.Println("Step 6: Starting container")
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", "", err
	}

	log.Println("Step 7: Waiting for container to finish")
	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", "", err
		}
	case <-statusCh:
		log.Println("Container finished execution")
	case <-time.After(30 * time.Second):
		log.Println("Execution timeout, killing container")
		_ = cli.ContainerKill(ctx, resp.ID, "SIGKILL")
		return "", "", fmt.Errorf("execution timeout")
	}

	log.Println("Step 8: Fetching logs")
	logs, err := cli.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", "", err
	}
	defer logs.Close()

	out, err := io.ReadAll(logs)
	if err != nil {
		return "", "", err
	}

	log.Println("Step 9: Returning output")
	return string(out), "", nil
}

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

	stdout, stderr, err := runSubmission(code)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println("STDOUT:\n", stdout)
	fmt.Println("STDERR:\n", stderr)
}
