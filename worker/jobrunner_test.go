package worker

import (
	"context"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

func TestJobRun_CanceledByUser_DefaultFalse(t *testing.T) {
	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 1,
	}
	session := &jobsession.JobSessionLogger{}
	testUnit := WorkUnit{}
	subUnits := []WorkUnit{}

	jr := NewJobRun(context.Background(), job, testUnit, subUnits, session)

	if jr.CanceledByUser() {
		t.Errorf("Expected CanceledByUser to be false by default")
	}
}

func TestJobRun_CanceledByUser_SetTrue(t *testing.T) {
	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 1,
	}
	session := &jobsession.JobSessionLogger{}
	testUnit := WorkUnit{}
	subUnits := []WorkUnit{}

	jr := NewJobRun(context.Background(), job, testUnit, subUnits, session)

	jr.Cancel(true)
	if !jr.CanceledByUser() {
		t.Errorf("Expected CanceledByUser to be true after Cancel(true)")
	}
}

func TestJobRun_CanceledByUser_SetFalse(t *testing.T) {
	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 1,
	}
	session := &jobsession.JobSessionLogger{}
	testUnit := WorkUnit{}
	subUnits := []WorkUnit{}

	jr := NewJobRun(context.Background(), job, testUnit, subUnits, session)

	jr.Cancel(false)

	// Allow some time for cancel to propagate if needed
	time.Sleep(10 * time.Millisecond)

	if jr.CanceledByUser() {
		t.Errorf("Expected CanceledByUser to be false after Cancel(false)")
	}
}
