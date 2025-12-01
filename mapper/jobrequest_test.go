package mapper

import (
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

func TestIncomingJobRequest_ToDomain(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID:  validUUID,
		UserId:  "user123",
		Timeout: 30,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				Alias:        "test",
				BuildCommand: "go test",
				EntryCommand: "./test",
				TestFiles: map[string]string{
					"test.go": "package main",
				},
				Envs: []types.EnvironmentVariable{
					{Key: "TEST_ENV", Value: "1"},
				},
			},
			Submission: types.SubmissionConfig{
				SubmissionCode: map[string]string{
					"main.go": "package main",
				},
				BuildCommand: "go build",
				EntryCommand: "./main",
				GlobalEnvs: []types.EnvironmentVariable{
					{Key: "GLOBAL_ENV", Value: "glob"},
					{Key: "LOCAL_ENV", Value: "suck"},
				},
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{
						Alias: "replica1",
						Envs: []types.EnvironmentVariable{
							{Key: "LOCAL_ENV", Value: "loc"},
						},
					},
				},
			},
		},
	}

	job, err := ConvertToJobRequest(&req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// UUID parsing
	if job.JobUID.String() != validUUID {
		t.Errorf("UUID not mapped correctly")
	}

	// Timeout
	if job.Timeout != 30*time.Second {
		t.Errorf("expected timeout 30s, got: %v", job.Timeout)
	}

	// Two nodes (test + replica)
	if len(job.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(job.Nodes))
	}

	testNode := job.Nodes[0]
	if testNode.Alias != "test" ||
		testNode.BuildCommand != "go test" ||
		testNode.EntryCommand != "./test" {
		t.Error("test container incorrectly mapped")
	}

	if _, ok := testNode.Files["test.go"]; !ok {
		t.Error("test files not mapped correctly")
	}

	// Replica node check
	replica := job.Nodes[1]
	if replica.Alias != "replica1" ||
		replica.BuildCommand != "go build" ||
		replica.EntryCommand != "./main" {
		t.Error("submission replica incorrectly mapped")
	}

	if _, ok := replica.Files["main.go"]; !ok {
		t.Error("replica files not mapped correctly")
	}

	// Environment merge: GLOBAL_ENV + LOCAL_ENV
	envs := map[string]string{}
	for _, e := range replica.Envs {
		envs[e.Key] = e.Value
	}
	if envs["GLOBAL_ENV"] != "glob" || envs["LOCAL_ENV"] != "loc" {
		t.Error("env merge incorrect")
	}
}

func TestIncomingJobRequest_ToDomain_NoReplicas(t *testing.T) {
	req := types.JobRequest{
		JobUID: uuid.New().String(),
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{},
			Submission: types.SubmissionConfig{
				ReplicaConfigs: []types.IncomingReplicaConfig{},
			},
		},
	}

	_, err := ConvertToJobRequest(&req)
	if err == nil {
		t.Fatal("expected error for missing replicas")
	}
}

func TestIncomingJobRequest_ToDomain_TooManyReplicas(t *testing.T) {
	tooManyReplicas := MaxReplicaConfigs + 1
	replicas := make([]types.IncomingReplicaConfig, tooManyReplicas)

	for i := range replicas {
		replicas[i] = types.IncomingReplicaConfig{Alias: "replica"}
	}

	req := types.JobRequest{
		JobUID: uuid.New().String(),
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{},
			Submission: types.SubmissionConfig{
				ReplicaConfigs: replicas,
			},
		},
	}

	_, err := ConvertToJobRequest(&req)
	if err == nil {
		t.Fatal("expected error for too many replicas")
	}
}
func TestConvertToJobRequest_InvalidUUID(t *testing.T) {
	req := types.JobRequest{
		JobUID: "invalid-uuid",
		Nodes: types.ContainerConfigs{
			Submission: types.SubmissionConfig{
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{Alias: "replica1"},
				},
			},
		},
	}

	_, err := ConvertToJobRequest(&req)
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestConvertToJobRequest_MultipleReplicas(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID:  validUUID,
		UserId:  "user456",
		Timeout: 60,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				Alias: "test-container",
			},
			Submission: types.SubmissionConfig{
				SubmissionCode: map[string]string{
					"app.go": "package main",
				},
				BuildCommand: "go build -o app",
				EntryCommand: "./app",
				GlobalEnvs: []types.EnvironmentVariable{
					{Key: "GLOBAL_VAR", Value: "global"},
				},
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{
						Alias: "replica1",
						Envs: []types.EnvironmentVariable{
							{Key: "REPLICA_ID", Value: "1"},
						},
					},
					{
						Alias: "replica2",
						Envs: []types.EnvironmentVariable{
							{Key: "REPLICA_ID", Value: "2"},
						},
					},
				},
			},
		},
	}

	job, err := ConvertToJobRequest(&req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(job.Nodes) != 3 {
		t.Fatalf("expected 3 nodes (1 test + 2 replicas), got %d", len(job.Nodes))
	}

	if job.Nodes[1].Alias != "replica1" || job.Nodes[2].Alias != "replica2" {
		t.Error("replica aliases not set correctly")
	}
}

func TestConvertToJobRequest_EmptyFiles(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID: validUUID,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				TestFiles: map[string]string{},
			},
			Submission: types.SubmissionConfig{
				SubmissionCode: map[string]string{},
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{Alias: "replica1"},
				},
			},
		},
	}

	job, err := ConvertToJobRequest(&req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(job.Nodes[0].Files) != 0 || len(job.Nodes[1].Files) != 0 {
		t.Error("expected empty file maps")
	}
}
