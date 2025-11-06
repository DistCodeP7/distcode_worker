package worker

import (
	"context"
	"time"

	"github.com/DistCodeP7/distcode_worker/metric"
	"github.com/docker/docker/client"
)

type WrappedWorker struct {
	*Worker
}

func NewWrappedWorker(ctx context.Context, cli *client.Client, workerImageName string) (*WrappedWorker, error) {
	w, err := NewWorker(ctx, cli, workerImageName)
	if err != nil {
		return nil, err
	}
	return &WrappedWorker{Worker: w}, nil
}

func (ww *WrappedWorker) ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error {
	startT := time.Now()
	err := ww.Worker.ExecuteCode(ctx, code, stdoutCh, stderrCh)
	endT := time.Now()

	go metric.RecordMetric(ctx, ww.Worker.ID(), ctx.Value("jobID").(string), startT, endT)
	return err
}