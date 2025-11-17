package worker

import (
	"context"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func newTestAggregator(clock clockwork.Clock, resultsChannel chan<- types.StreamingJobEvent, metricsChannel chan<- types.StreamingJobEvent) *EventAggregator {
	return &EventAggregator{
		resultsChannel: resultsChannel,
		metricsChannel: metricsChannel,
		clock:          clock,
		eventBuf:       make([]types.StreamingEvent, 0),
	}
}

func TestFlushRemainingEventsWithMessageInBuffer(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 1)
	metricsChan := make(chan types.StreamingJobEvent, 1)
	clock := clockwork.NewFakeClock()
	aggregator := newTestAggregator(clock, resultsChan, metricsChan)
	job := types.JobRequest{
		ProblemId: 1,
		UserId:    "42",
	}

	aggregator.eventBuf = append(aggregator.eventBuf, types.StreamingEvent{
		Kind:    "stdout",
		Message: utils.PtrString("Hello, World!"),
	})

	aggregator.flushRemainingEvents(job)

	select {
	case result := <-resultsChan:
		if result.ProblemId != job.ProblemId || result.UserId != job.UserId {
			t.Errorf("Unexpected job result: %+v", result)
		}
		if len(result.Events) != 1 {
			t.Errorf("Expected 1 event, got %d", len(result.Events))
		}
		if result.Events[0].Message == nil || *result.Events[0].Message != "Hello, World!" {
			t.Errorf("Unexpected event message: %v", result.Events[0].Message)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for result")
	}
}

func TestFlushRemainingEventsWithNoMessageInBuffer(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 1)
	metricsChan := make(chan types.StreamingJobEvent, 1)
	clock := clockwork.NewFakeClock()
	aggregator := newTestAggregator(clock, resultsChan, metricsChan)
	job := types.JobRequest{
		ProblemId: 1,
		UserId:    "42",
	}

	aggregator.flushRemainingEvents(job)

	select {
	case result := <-resultsChan:
		if result.ProblemId != job.ProblemId || result.UserId != job.UserId {
			t.Errorf("Unexpected job result: %+v", result)
		}
		if len(result.Events) != 0 {
			t.Errorf("Expected 0 event as buffer is empty, got %d", len(result.Events))
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for result")
	}
}

func TestPeriodicFlushWithMessage(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 1)
	metricsChan := make(chan types.StreamingJobEvent, 1)
	clock := clockwork.NewFakeClock()
	aggregator := newTestAggregator(clock, resultsChan, metricsChan)
	job := types.JobRequest{
		ProblemId: 1,
		UserId:    "42",
	}

	ctx := t.Context()
	interval := 10 * time.Millisecond
	cancel, _ := aggregator.startPeriodicFlush(ctx, job, interval)
	defer cancel()

	aggregator.muEvents.Lock()
	aggregator.eventBuf = append(aggregator.eventBuf, types.StreamingEvent{
		Kind:    "stdout",
		Message: utils.PtrString("Periodic Event"),
	})
	aggregator.muEvents.Unlock()

	clock.Advance(interval + 1*time.Millisecond)

	select {
	case result := <-resultsChan:
		if result.ProblemId != job.ProblemId || result.UserId != job.UserId {
			t.Errorf("Unexpected job result: %+v", result)
		}
		if len(result.Events) != 1 {
			t.Errorf("Expected 1 event, got %d", len(result.Events))
		}
		if result.Events[0].Message == nil || *result.Events[0].Message != "Periodic Event" {
			t.Errorf("Unexpected event message: %v", result.Events[0].Message)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for result")
	}
}

func TestPeriodicFlushWithNoMessage(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 1)
	metricsChan := make(chan types.StreamingJobEvent, 1)
	clock := clockwork.NewFakeClock()
	aggregator := newTestAggregator(clock, resultsChan, metricsChan)
	job := types.JobRequest{
		ProblemId: 1,
		UserId:    "42",
	}

	ctx := t.Context()
	interval := 10 * time.Millisecond
	cancel, _ := aggregator.startPeriodicFlush(ctx, job, interval)
	defer cancel()
	clock.Advance(interval + 1*time.Millisecond)
	select {
	case result := <-resultsChan:
		t.Error("Did not expect any result, but got:", result)
	default:
	}
}

type fakeWorker struct {
	id     string
	stdout []string
	stderr []string
	err    error
}

func (fw *fakeWorker) ID() string { return fw.id }

func (fw *fakeWorker) ExecuteCode(
	ctx context.Context,
	code string,
	stdoutCh, stderrCh chan string) error {
	for _, line := range fw.stdout {
		stdoutCh <- line
	}
	for _, line := range fw.stderr {
		stderrCh <- line
	}
	return fw.err
}

func (w *fakeWorker) ConnectToNetwork(ctx context.Context, networkName, alias string) error {
	return nil
}

func (w *fakeWorker) DisconnectFromNetwork(ctx context.Context, networkName string) error {
	return nil
}

func (w *fakeWorker) Stop(ctx context.Context) error {
	return nil
}

func TestWorkerLogStreamingAndPeriodicFlushIntegration(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 2)
	metricsChan := make(chan types.StreamingJobEvent, 2)
	clock := clockwork.NewFakeClock()
	aggregator := newTestAggregator(clock, resultsChan, metricsChan)

	job := types.JobRequest{
		ProblemId: 123,
		UserId:    "456",
	}

	ctx := t.Context()
	interval := 10 * time.Millisecond
	cancel, _ := aggregator.startPeriodicFlush(ctx, job, interval)
	defer cancel()

	worker := &fakeWorker{
		id:     "worker-1",
		stdout: []string{"out1", "out2"},
		stderr: []string{"err1"},
		err:    nil,
	}

	code := "print('Hello, World!')"
	aggregator.startWorkerLogStreaming(ctx, worker, code, nil, job)

	assert.Eventually(t, func() bool {
		aggregator.muEvents.Lock()
		defer aggregator.muEvents.Unlock()
		return len(aggregator.eventBuf) > 0
	}, 1*time.Second, 10*time.Millisecond)

	clock.Advance(interval + 1*time.Millisecond)

	select {
	case result := <-resultsChan:
		assert.NotNil(t, result)

		messageMap := make(map[string]bool)
		for _, event := range result.Events {
			if event.Message != nil {
				messageMap[*event.Message] = true
			}
		}
		assert.Contains(t, messageMap, "out1")
		assert.Contains(t, messageMap, "out2")
		assert.Contains(t, messageMap, "err1")
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for result")
	}
}
