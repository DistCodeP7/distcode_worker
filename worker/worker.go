package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"time"

	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type Worker struct {
	containerID string
	dockerCli   *client.Client
	hostPath    string
}

type WorkerInterface interface {
	ID() string
	ConnectToNetwork(ctx context.Context, networkName, alias string) error
	DisconnectFromNetwork(ctx context.Context, networkName string) error
	Stop(ctx context.Context) error
	ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error
	// ExecuteTest writes the submitted files into a per-job directory under the worker hostPath,
	// runs `go test` for the given problemDir inside the running container, streams stdout/stderr
	// to the provided channels, and cleans up the per-job files when finished.
	ExecuteTest(ctx context.Context, jobUID string, problemDir string, files []string, stdoutCh, stderrCh chan string) error
}

var _ WorkerInterface = (*Worker)(nil)

func NewWorker(ctx context.Context, cli *client.Client, workerImageName string) (*Worker, error) {
	log.Println("Initializing a new worker...")

	hostPath, err := os.MkdirTemp("", "docker-worker-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	containerConfig := &container.Config{
		Image:      workerImageName,
		Cmd:        []string{"sleep", "infinity"},
		Tty:        false,
		WorkingDir: "/app",
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: hostPath,
				// Mount worker hostPath at /app so we can run `go test` inside worker containers
				// against code placed under this directory.
				Target: "/app",
			},
			{
				Type:   mount.TypeVolume,
				Source: "go-build-cache",
				Target: "/root/.cache/go-build",
			},
		},
		Resources: container.Resources{
			CPUShares:      512,
			NanoCPUs:       500_000_000,          // 0.5 CPU
			Memory:         256 * 1024 * 1024,    // 256MB
			PidsLimit:      utils.PtrInt64(50),   // max 50 processes
			MemorySwap:     512 * 1024 * 1024,    // 512MB - gVisor needs swap > memory
			OomKillDisable: utils.PtrBool(false), // enable OOM killer
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},        // 30s CPU limit
				{Name: "nofile", Soft: 1024, Hard: 1024}, // max open files
			},
		},
	}

	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		os.RemoveAll(hostPath)
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	log.Printf("Starting container %s...", resp.ID[:12])
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		os.RemoveAll(hostPath)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	worker := &Worker{
		containerID: resp.ID,
		dockerCli:   cli,
		hostPath:    hostPath,
	}

	log.Printf("Worker initialized with container %s", worker.containerID[:12])
	return worker, nil
}

func (w *Worker) ID() string {
	return w.containerID
}

func (w *Worker) ConnectToNetwork(ctx context.Context, networkName, alias string) error {
	return w.dockerCli.NetworkConnect(ctx, networkName, w.containerID, &network.EndpointSettings{
		Aliases: []string{alias},
	})
}

func (w *Worker) DisconnectFromNetwork(ctx context.Context, networkName string) error {
	return w.dockerCli.NetworkDisconnect(ctx, networkName, w.containerID, true)
}

func (w *Worker) Stop(ctx context.Context) error {
	log.Printf("Stopping and removing container %s", w.containerID[:12])

	if err := w.dockerCli.ContainerStop(ctx, w.containerID, container.StopOptions{Timeout: nil}); err != nil {
		log.Printf("Warning: failed to gracefully stop container %s: %v", w.containerID[:12], err)
	}

	if err := w.dockerCli.ContainerRemove(ctx, w.containerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}

	if err := os.RemoveAll(w.hostPath); err != nil {
		return fmt.Errorf("failed to remove host path: %w", err)
	}

	return nil
}

func (w *Worker) ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error {
	codePath := filepath.Join(w.hostPath, "main.go")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return fmt.Errorf("failed to write code to file: %w", err)
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "run", "/app/main.go"},
		AttachStdout: true,
		AttachStderr: true,
		WorkingDir:   "/app",
	}

	execID, err := w.dockerCli.ContainerExecCreate(ctx, w.containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	hijackedResp, err := w.dockerCli.ContainerExecAttach(ctx, execID.ID, container.ExecStartOptions{
		Detach: false,
		Tty:    false,
	})
	if err != nil {
		return fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	stdoutWriter := newChannelWriter(stdoutCh)
	stderrWriter := newChannelWriter(stderrCh)
	done := make(chan error, 1)

	go func() {
		_, err := stdcopy.StdCopy(
			stdoutWriter,
			stderrWriter,
			hijackedResp.Reader,
		)
		stdoutWriter.Flush()
		stderrWriter.Flush()
		done <- err
	}()

	select {
	case <-ctx.Done():
		log.Printf("Job cancelled, stopping current execution in container %s", w.containerID)
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("failed to stream output: %w", err)
		}
	}

	inspectResp, err := w.dockerCli.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	return nil
}

// ExecuteTest writes submitted files into a per-job directory under the worker host path,
// normalizes package declarations to match the package in the repo, runs `go test` inside
// the container, streams output back on stdoutCh/stderrCh, and removes the per-job dir.
func (w *Worker) ExecuteTest(ctx context.Context, jobUID string, problemDir string, files []string, stdoutCh, stderrCh chan string) error {
	jobDir := filepath.Join(w.hostPath, "jobs", jobUID)
	pkgPath := filepath.Join(jobDir, "algorithms", problemDir)
	if err := os.MkdirAll(pkgPath, 0755); err != nil {
		return fmt.Errorf("failed to create job pkg dir: %w", err)
	}

	// Try to locate repository root (where go.mod lives) using multiple strategies
	findGoMod := func(dir string) (string, bool) {
		cand := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(cand); err == nil {
			return cand, true
		}
		return "", false
	}

	repoRoot := ""
	// 1) start from cwd
	if cwd, err := os.Getwd(); err == nil {
		if _, ok := findGoMod(cwd); ok {
			repoRoot = cwd
		}
	}
	// 2) try the source file location (this file) and walk up
	if repoRoot == "" {
		if _, file, _, ok := runtime.Caller(0); ok {
			dir := filepath.Dir(file)
			for {
				if _, ok := findGoMod(dir); ok {
					repoRoot = dir
					break
				}
				parent := filepath.Dir(dir)
				if parent == dir {
					break
				}
				dir = parent
			}
		}
	}
	// 2b) try executable path then walk up (covers cases where runtime.Caller isn't helpful)
	if repoRoot == "" {
		if exe, err := os.Executable(); err == nil {
			dir := filepath.Dir(exe)
			for {
				if _, ok := findGoMod(dir); ok {
					repoRoot = dir
					break
				}
				parent := filepath.Dir(dir)
				if parent == dir {
					break
				}
				dir = parent
			}
		}
	}
	// 3) environment override
	if repoRoot == "" {
		if env := os.Getenv("REPO_ROOT"); env != "" {
			if _, ok := findGoMod(env); ok {
				repoRoot = env
			}
		}
	}

	if repoRoot != "" {
		if data, err := ioutil.ReadFile(filepath.Join(repoRoot, "go.mod")); err == nil {
			_ = os.MkdirAll(jobDir, 0755)
			_ = ioutil.WriteFile(filepath.Join(jobDir, "go.mod"), data, 0644)
		}
	}

	// Logging for diagnostics: report where we think the repo root is and if go.mod was written.
	if repoRoot == "" {
		log.Printf("ExecuteTest: repo root not found; jobDir=%s", jobDir)
	} else {
		log.Printf("ExecuteTest: using repo root %s; jobDir=%s", repoRoot, jobDir)
	}

	// Copy repository package files into job pkgPath if available
	repoPkgDir := filepath.Join(repoRoot, "algorithms", problemDir)
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

	// Determine target package name
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

	// Normalize and write submitted files
	pkgDeclRe := regexp.MustCompile(`(?m)^\s*package\s+\w+`)
	for i, content := range files {
		var fname string
		if len(files) == 1 {
			fname = "client.go"
		} else {
			fname = fmt.Sprintf("submission_%d.go", i+1)
		}
		if pkgDeclRe.MatchString(content) {
			content = pkgDeclRe.ReplaceAllString(content, "package "+targetPkgName)
		} else {
			content = fmt.Sprintf("package %s\n\n%s", targetPkgName, content)
		}
		if err := ioutil.WriteFile(filepath.Join(pkgPath, fname), []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to write submission file: %w", err)
		}
	}

	// Before creating the exec, verify that the jobDir contents are visible
	// (go.mod and at least one .go file) inside the hostPath. This helps
	// mitigate intermittent races where the container's view of the bind
	// mount isn't immediately consistent with the host filesystem.
	verifyReady := func() bool {
		// check go.mod
		if _, err := os.Stat(filepath.Join(jobDir, "go.mod")); err != nil {
			return false
		}
		// check at least one .go file in the package dir
		ents, err := ioutil.ReadDir(pkgPath)
		if err != nil {
			return false
		}
		for _, e := range ents {
			if e.IsDir() {
				continue
			}
			if filepath.Ext(e.Name()) == ".go" {
				return true
			}
		}
		return false
	}

	// Retry a few times with a small backoff to allow mount propagation.
	attempts := 5
	ready := false
	for i := 0; i < attempts; i++ {
		if verifyReady() {
			ready = true
			break
		}
		log.Printf("ExecuteTest: jobDir not ready yet (attempt %d/%d): %s", i+1, attempts, jobDir)
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		log.Printf("ExecuteTest: proceeding even though jobDir may be incomplete: %s", jobDir)
	}

	// Also verify from inside the container that the files are visible on the bind mount.
	// This avoids races where the host filesystem shows the files but the container's
	// view of the bind mount is not yet updated.
	containerCheckCmd := fmt.Sprintf("test -f /app/jobs/%s/go.mod && ls /app/jobs/%s/algorithms/%s/*.go >/dev/null 2>&1", jobUID, jobUID, problemDir)
	readyInside := false
	for i := 0; i < attempts; i++ {
		checkExec := container.ExecOptions{
			Cmd:          []string{"sh", "-c", containerCheckCmd},
			AttachStdout: true,
			AttachStderr: true,
			WorkingDir:   "/app",
		}
		// Use a short context for the check so it doesn't block the overall jobCtx
		shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
		execID, err := w.dockerCli.ContainerExecCreate(shortCtx, w.containerID, checkExec)
		if err == nil {
			hijacked, attachErr := w.dockerCli.ContainerExecAttach(shortCtx, execID.ID, container.ExecStartOptions{Detach: false, Tty: false})
			if attachErr == nil {
				// Drain output
				_, _ = stdcopy.StdCopy(io.Discard, io.Discard, hijacked.Reader)
				hijacked.Close()
				if insp, inspErr := w.dockerCli.ContainerExecInspect(shortCtx, execID.ID); inspErr == nil && insp.ExitCode == 0 {
					readyInside = true
				}
			}
		}
		shortCancel()
		if readyInside {
			break
		}
		log.Printf("ExecuteTest: container doesn't see job files yet (attempt %d/%d) for job %s", i+1, attempts, jobUID)
		time.Sleep(100 * time.Millisecond)
	}
	if !readyInside {
		log.Printf("ExecuteTest: proceeding even though container may not see job files for job %s", jobUID)
	}

	// Create exec to run go test
	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "test", "./algorithms/" + problemDir, "-run", "TestCentralizedMutex", "-v", "count=1"},
		AttachStdout: true,
		AttachStderr: true,
		// Run the test with the job directory as the working directory so the
		// per-job go.mod we wrote into jobDir is discovered by the go toolchain.
		WorkingDir: filepath.Join("/app", "jobs", jobUID),
	}

	execID, err := w.dockerCli.ContainerExecCreate(ctx, w.containerID, execConfig)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	hijackedResp, err := w.dockerCli.ContainerExecAttach(ctx, execID.ID, container.ExecStartOptions{Detach: false, Tty: false})
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	// Stream output
	stdoutWriter := newChannelWriter(stdoutCh)
	stderrWriter := newChannelWriter(stderrCh)
	done := make(chan error, 1)

	go func() {
		_, err := stdcopy.StdCopy(stdoutWriter, stderrWriter, hijackedResp.Reader)
		stdoutWriter.Flush()
		stderrWriter.Flush()
		done <- err
	}()

	select {
	case <-ctx.Done():
		_ = os.RemoveAll(jobDir)
		return ctx.Err()
	case err := <-done:
		if err != nil {
			_ = os.RemoveAll(jobDir)
			return fmt.Errorf("failed to stream output: %w", err)
		}
	}

	inspectResp, err := w.dockerCli.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	// Cleanup job files
	_ = os.RemoveAll(jobDir)

	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("test process exited with code %d", inspectResp.ExitCode)
	}

	return nil
}

type channelWriter struct {
	ch  chan string
	buf bytes.Buffer
}

func newChannelWriter(ch chan string) *channelWriter {
	return &channelWriter{ch: ch}
}

// Write implements the io.Writer interface for channelWriter.
// It writes the provided byte slice to an internal buffer, splitting the input at newline characters.
// For each complete line (ending with '\n'), it sends the buffered string to the associated channel and resets the buffer.
func (cw *channelWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i == -1 {
			cw.buf.Write(p)
			total += len(p)
			break
		}
		cw.buf.Write(p[:i])
		cw.ch <- cw.buf.String()
		cw.buf.Reset()
		p = p[i+1:]
		total += i + 1
	}
	return total, nil
}

func (cw *channelWriter) Flush() {
	if cw.buf.Len() > 0 {
		cw.ch <- cw.buf.String()
		cw.buf.Reset()
	}
}
