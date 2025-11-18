package worker

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// RunEphemeralJob creates a temporary host directory with the submitted files,
// starts a short-lived container that bind-mounts that directory into the
// package path, runs the package test, streams stdout/stderr into the provided
// channels, and cleans up the container and temp dir. It accepts the submitted
// files as a slice of strings (no filenames). Each submitted string will be
// written to a numbered file (submission_1.go, submission_2.go ...) inside the
// package directory so the test runs with these files in-package. It returns an
// error if the test process exits non-zero or other failures occur.
func RunEphemeralJob(ctx context.Context, dockerCli *client.Client, workerImage string, problemDir string, files []string, stdoutCh, stderrCh chan string, timeoutSeconds int) error {
	// prepare temp dir
	hostPath, err := os.MkdirTemp("", "job-mount-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	// cleanup temp dir at the end
	defer os.RemoveAll(hostPath)

	// We will create a minimal workspace containing the repository module file
	// and the algorithms package directory. This ensures `go test` inside the
	// container can resolve the module and local package imports.
	// Create algorithms/<problemDir> inside hostPath
	pkgPath := filepath.Join(hostPath, "algorithms", problemDir)
	if err := os.MkdirAll(pkgPath, 0755); err != nil {
		return fmt.Errorf("failed to create pkg dir: %w", err)
	}

	// Try to locate the repository's go.mod on the host so we can copy it into the temp workspace.
	repoGoModPath := ""
	cwd, _ := os.Getwd()
	searchDir := cwd
	for {
		candidate := filepath.Join(searchDir, "go.mod")
		if _, err := os.Stat(candidate); err == nil {
			repoGoModPath = candidate
			break
		}
		parent := filepath.Dir(searchDir)
		if parent == searchDir {
			break
		}
		searchDir = parent
	}

	if repoGoModPath != "" {
		data, err := ioutil.ReadFile(repoGoModPath)
		if err != nil {
			return fmt.Errorf("failed to read repo go.mod: %w", err)
		}
		if err := ioutil.WriteFile(filepath.Join(hostPath, "go.mod"), data, 0644); err != nil {
			return fmt.Errorf("failed to write go.mod into temp workspace: %w", err)
		}
	} else {
		// If we couldn't find a go.mod, create a minimal one. This is a fallback
		// and may fail if tests import module-local paths; prefer copying the real one.
		minimal := "module tempjob\n\n go 1.20\n"
		if err := ioutil.WriteFile(filepath.Join(hostPath, "go.mod"), []byte(minimal), 0644); err != nil {
			return fmt.Errorf("failed to write minimal go.mod into temp workspace: %w", err)
		}
	}

	// Copy existing package files from the repository algorithms/<problemDir> into the temp workspace
	// so tests have the supporting code. If the repo path doesn't exist, continue — tests may still fail.
	repoPkgDir := filepath.Join(searchDir, "algorithms", problemDir)
	if fi, err := os.Stat(repoPkgDir); err == nil && fi.IsDir() {
		entries, err := ioutil.ReadDir(repoPkgDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				src := filepath.Join(repoPkgDir, e.Name())
				dst := filepath.Join(pkgPath, e.Name())
				data, err := ioutil.ReadFile(src)
				if err != nil {
					return fmt.Errorf("failed to read repo package file %s: %w", src, err)
				}
				if err := ioutil.WriteFile(dst, data, 0644); err != nil {
					return fmt.Errorf("failed to copy package file %s: %w", dst, err)
				}
			}
		}
	}

	// Determine target package name by inspecting copied package files (if any).
	targetPkgName := problemDir
	if entries, err := ioutil.ReadDir(pkgPath); err == nil {
		pkgRe := regexp.MustCompile(`^\s*package\s+(\w+)`)
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".go" {
				continue
			}
			data, err := ioutil.ReadFile(filepath.Join(pkgPath, e.Name()))
			if err != nil {
				continue
			}
			if m := pkgRe.FindSubmatch(data); m != nil {
				targetPkgName = string(m[1])
				break
			}
		}
	}

	// Write each submitted file into the package directory using a generated filename.
	// Normalize package declarations in submissions so they match the target package.
	pkgDeclRe := regexp.MustCompile(`(?m)^\s*package\s+\w+`) // multiline
	for i, content := range files {
		var fname string
		if len(files) == 1 {
			fname = "client.go"
		} else {
			fname = fmt.Sprintf("submission_%d.go", i+1)
		}

		// Ensure the submission uses the target package name.
		if pkgDeclRe.MatchString(content) {
			content = pkgDeclRe.ReplaceAllString(content, "package "+targetPkgName)
		} else {
			content = fmt.Sprintf("package %s\n\n%s", targetPkgName, content)
		}

		target := filepath.Join(pkgPath, fname)
		if err := ioutil.WriteFile(target, []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", target, err)
		}
	}

	containerConfig := &container.Config{
		Image:      workerImage,
		Cmd:        []string{"sleep", "infinity"},
		Tty:        false,
		WorkingDir: "/app",
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			// Mount the entire temp workspace at /app so go.mod and other repo files are visible.
			{Type: mount.TypeBind, Source: hostPath, Target: "/app"},
			{Type: mount.TypeVolume, Source: "go-build-cache", Target: "/root/.cache/go-build"},
		},
		Resources: container.Resources{
			CPUShares:      512,
			NanoCPUs:       500_000_000,
			Memory:         256 * 1024 * 1024,
			PidsLimit:      utils.PtrInt64(50),
			MemorySwap:     512 * 1024 * 1024,
			OomKillDisable: utils.PtrBool(false),
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},
				{Name: "nofile", Soft: 1024, Hard: 1024},
			},
		},
	}

	resp, err := dockerCli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}
	containerID := resp.ID

	// Ensure container removal
	defer func() {
		_ = dockerCli.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
	}()

	if err := dockerCli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	// Create exec instance to run go test
	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "test", "./algorithms/" + problemDir, "-run", "TestCentralizedMutex", "-v", "option=1"},
		AttachStdout: true,
		AttachStderr: true,
		WorkingDir:   "/app",
	}

	execID, err := dockerCli.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	hijackedResp, err := dockerCli.ContainerExecAttach(ctx, execID.ID, container.ExecStartOptions{Detach: false, Tty: false})
	if err != nil {
		return fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	// stream output
	done := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(
			newChannelWriter(stdoutCh),
			newChannelWriter(stderrCh),
			hijackedResp.Reader,
		)
		done <- err
	}()

	// Wait for exec to finish or context timeout
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("failed to stream output: %w", err)
		}
	}

	inspectResp, err := dockerCli.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("test process exited with code %d", inspectResp.ExitCode)
	}

	// stop container (will be removed by deferred remove)
	_ = dockerCli.ContainerStop(context.Background(), containerID, container.StopOptions{Timeout: nil})

	return nil
}
