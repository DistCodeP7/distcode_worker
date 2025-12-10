package jobsession

import (
	"io"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

func TestNewLogWriter_SendsAndBuffers(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 10)
	job := types.Job{JobUID: uuid.New(), UserID: "user1"}
	s := NewJobSession(job, out)

	w := s.NewLogWriter("worker-1")
	wc, ok := w.(io.WriteCloser)
	if !ok {
		t.Fatalf("expected io.WriteCloser from NewLogWriter")
	}
	defer wc.Close()

	n, err := wc.Write([]byte("hello\n"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 6 {
		t.Fatalf("unexpected write length: got %d want %d", n, 6)
	}

	select {
	case ev := <-out:
		if ev.Type != types.TypeLog {
			t.Fatalf("expected TypeLog event, got %v", ev.Type)
		}
		if ev.Log == nil {
			t.Fatalf("log event is nil")
		}
		if ev.Log.WorkerID != "worker-1" {
			t.Fatalf("unexpected worker id: %s", ev.Log.WorkerID)
		}
		if ev.Log.Message != "hello\n" {
			t.Fatalf("unexpected message: %q", ev.Log.Message)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for log event")
	}

	logs := s.GetBufferedLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 buffered log, got %d", len(logs))
	}
	if logs[0].Message != "hello\n" {
		t.Fatalf("buffered log message mismatch: %q", logs[0].Message)
	}
}

func TestNewLogWriter_ExceedsMaxBytes_LeadsToWriteError(t *testing.T) {

	out := make(chan types.StreamingJobEvent, 100)
	job := types.Job{JobUID: uuid.New(), UserID: "user3"}
	s := NewJobSession(job, out)

	go func() {
		for range out {
		}
	}()

	w := s.NewLogWriter("worker-3")
	wc, ok := w.(io.WriteCloser)
	if !ok {
		t.Fatalf("expected io.WriteCloser from NewLogWriter")
	}
	defer wc.Close()

	const linesToWrite = MaxLogBuffer + 100

	for i := 0; i < linesToWrite; i++ {
		_, err := wc.Write([]byte("123456789\n"))
		if err != nil {
			if err == ErrLogLimitExceeded {
				return
			}
			t.Fatalf("unexpected write error at iteration %d: %v", i, err)
		}
	}

	t.Fatalf("expected ErrLogLimitExceeded but successfully wrote %d lines", linesToWrite)
}

func TestNewLogWriter_ExceedsMaxLines_LeadsToWriteError(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 10000)
	job := types.Job{JobUID: uuid.New(), UserID: "user3"}
	s := NewJobSession(job, out)

	w := s.NewLogWriter("worker-3")
	wc, ok := w.(io.WriteCloser)
	if !ok {
		t.Fatalf("expected io.WriteCloser from NewLogWriter")
	}

	const linesToWrite = MaxLogBuffer + 1
	for i := range linesToWrite {
		_, err := wc.Write([]byte("\n"))
		if err != nil {
			if err == ErrLogLimitExceeded {
				_ = wc.Close()
				return
			}
			t.Fatalf("unexpected write error at iteration %d: %v", i, err)
		}
	}
}

func TestSetPhase_SendsStatusEvent(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 1)
	job := types.Job{JobUID: uuid.New(), UserID: "user-setphase"}
	s := NewJobSession(job, out)

	s.SetPhase(types.PhaseRunning, "starting")

	select {
	case ev := <-out:
		if ev.Type != types.TypeStatus {
			t.Fatalf("expected TypeStatus, got %v", ev.Type)
		}
		if ev.Status == nil {
			t.Fatalf("Status event missing payload")
		}
		if ev.Status.Phase != string(types.PhaseRunning) {
			t.Fatalf("unexpected phase: %s", ev.Status.Phase)
		}
		if ev.Status.Message != "starting" {
			t.Fatalf("unexpected status message: %q", ev.Status.Message)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for status event")
	}
}

func TestGetBufferedLogs_ReturnsCopy(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 10)
	job := types.Job{JobUID: uuid.New(), UserID: "user-copy"}
	s := NewJobSession(job, out)

	w := s.NewLogWriter("worker-x")
	w.Write([]byte("hello\n"))
	time.Sleep(20 * time.Millisecond)

	logs := s.GetBufferedLogs()
	if len(logs) != 1 {
		t.Fatalf("expected one log in buffer, got %d", len(logs))
	}

	logs[0].Message = "tampered"

	logs2 := s.GetBufferedLogs()
	if logs2[0].Message == "tampered" {
		t.Fatalf("internal buffer modified through returned slice")
	}
}

func TestFinishSuccess_SendsResultEvent(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 1)
	job := types.Job{JobUID: uuid.New(), UserID: "user-success"}
	s := NewJobSession(job, out)

	artifacts := JobArtifacts{
		TestResults:     []dt.TestResult{{Name: "T1"}},
		NodeMessageLogs: nil,
	}

	start := s.StartTime()
	time.Sleep(1 * time.Millisecond)
	s.FinishSuccess(artifacts)

	select {
	case ev := <-out:
		if ev.Type != types.TypeResult {
			t.Fatalf("expected TypeResult, got %v", ev.Type)
		}
		if ev.Result == nil {
			t.Fatalf("missing Result payload")
		}
		if ev.Result.Outcome != types.OutcomeSuccess {
			t.Fatalf("expected OutcomeSuccess")
		}
		if ev.Result.DurationMs <= 0 {
			t.Fatalf("expected positive duration got %d", ev.Result.DurationMs)
		}
		if ev.Result.TestResults[0].Name != "T1" {
			t.Fatalf("test results not forwarded")
		}
		if !start.Before(time.Now()) {
			t.Fatalf("start time not set correctly")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for Result event")
	}
}

func TestFinishFail_SendsFailureResultEvent(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 1)
	job := types.Job{JobUID: uuid.New(), UserID: "user-fail"}
	s := NewJobSession(job, out)

	artifacts := JobArtifacts{
		TestResults:     []dt.TestResult{{Name: "T2"}},
		NodeMessageLogs: nil,
	}

	s.FinishFail(artifacts, types.OutcomeTimeout, io.ErrClosedPipe, "worker-z")

	if s.GetOutcome() != types.OutcomeTimeout {
		t.Fatalf("outcome not updated to failure")
	}

	select {
	case ev := <-out:
		if ev.Type != types.TypeResult {
			t.Fatalf("expected TypeResult")
		}
		if ev.Result == nil {
			t.Fatalf("missing Result payload")
		}
		if ev.Result.Outcome != types.OutcomeTimeout {
			t.Fatalf("unexpected outcome in result")
		}
		if ev.Result.Error == "" {
			t.Fatalf("expected error message")
		}
		if ev.Result.FailedWorkerID != "worker-z" {
			t.Fatalf("unexpected failed worker: %s", ev.Result.FailedWorkerID)
		}
		if ev.Result.TestResults[0].Name != "T2" {
			t.Fatalf("test results not propagated")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for failure result event")
	}
}

func TestNewLogWriter_MultipleLinesInSingleWrite(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 10)
	job := types.Job{JobUID: uuid.New(), UserID: "user-multiline"}
	s := NewJobSession(job, out)

	w := s.NewLogWriter("worker-4")
	w.Write([]byte("a\nb\nc\n"))
	time.Sleep(20 * time.Millisecond)

	var count int
	for {
		select {
		case ev := <-out:
			if ev.Type == types.TypeLog {
				count++
			}
		default:
			goto end
		}
	}
end:

	if count != 3 {
		t.Fatalf("expected 3 log events, got %d", count)
	}

	logs := s.GetBufferedLogs()
	if len(logs) != 3 {
		t.Fatalf("expected 3 buffered logs, got %d", len(logs))
	}
}

func TestNewLogWriter_EmptyLine(t *testing.T) {
	out := make(chan types.StreamingJobEvent, 1)
	job := types.Job{JobUID: uuid.New(), UserID: "user-emptyline"}
	s := NewJobSession(job, out)

	w := s.NewLogWriter("worker-5")
	w.Write([]byte("\n"))
	time.Sleep(20 * time.Millisecond)

	select {
	case ev := <-out:
		if ev.Log.Message != "\n" {
			t.Fatalf("expected empty line message, got %q", ev.Log.Message)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for log event")
	}
}
