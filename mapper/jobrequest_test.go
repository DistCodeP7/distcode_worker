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
				Alias:        "testing",
				BuildCommand: "go testing",
				EntryCommand: "./testing",
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

	// 1 test container + 1 replica
	if len(job.SubmissionNodes)+1 != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(job.SubmissionNodes)+1)
	}

	testNode := job.TestNode
	if testNode.Alias != "testing" ||
		testNode.BuildCommand != "go testing" ||
		testNode.EntryCommand != "./testing" {
		t.Error("test container incorrectly mapped")
	}

	if _, ok := testNode.Files["test.go"]; !ok {
		t.Error("test files not mapped correctly")
	}

	// Replica node check
	replica := job.SubmissionNodes[0]
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

	if len(job.SubmissionNodes)+1 != 3 {
		t.Fatalf("expected 3 nodes (1 test + 2 replicas), got %d", len(job.SubmissionNodes)+1)
	}

	if job.SubmissionNodes[0].Alias != "replica1" || job.SubmissionNodes[1].Alias != "replica2" {
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

	if len(job.TestNode.Files) != 0 || len(job.SubmissionNodes[0].Files) != 0 {
		t.Error("expected empty file maps")
	}
}
func TestCreatePeerAliasEnv(t *testing.T) {
	tests := []struct {
		name     string
		replicas []types.IncomingReplicaConfig
		expected types.EnvironmentVariable
	}{
		{
			name: "single replica",
			replicas: []types.IncomingReplicaConfig{
				{Alias: "replica1"},
			},
			expected: types.EnvironmentVariable{
				Key:   "PEERS",
				Value: "replica1",
			},
		},
		{
			name: "multiple replicas",
			replicas: []types.IncomingReplicaConfig{
				{Alias: "replica1"},
				{Alias: "replica2"},
				{Alias: "replica3"},
			},
			expected: types.EnvironmentVariable{
				Key:   "PEERS",
				Value: "replica1,replica2,replica3",
			},
		},
		{
			name:     "empty replicas",
			replicas: []types.IncomingReplicaConfig{},
			expected: types.EnvironmentVariable{
				Key:   "PEERS",
				Value: "",
			},
		},
		{
			name: "replicas with special characters",
			replicas: []types.IncomingReplicaConfig{
				{Alias: "replica-1"},
				{Alias: "replica_2"},
				{Alias: "replica.3"},
			},
			expected: types.EnvironmentVariable{
				Key:   "PEERS",
				Value: "replica-1,replica_2,replica.3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createPeerAliasEnv(tt.replicas)

			if result.Key != tt.expected.Key {
				t.Errorf("expected key %q, got %q", tt.expected.Key, result.Key)
			}

			if result.Value != tt.expected.Value {
				t.Errorf("expected value %q, got %q", tt.expected.Value, result.Value)
			}
		})
	}
}
func TestConvertToJobRequest_PeersEnvInNodes(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID:  validUUID,
		UserId:  "user123",
		Timeout: 30,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				Alias: "test-container",
				Envs: []types.EnvironmentVariable{
					{Key: "TEST_VAR", Value: "test"},
				},
			},
			Submission: types.SubmissionConfig{
				GlobalEnvs: []types.EnvironmentVariable{
					{Key: "GLOBAL_VAR", Value: "global"},
				},
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{
						Alias: "replica1",
						Envs: []types.EnvironmentVariable{
							{Key: "LOCAL_VAR", Value: "local1"},
						},
					},
					{
						Alias: "replica2",
						Envs: []types.EnvironmentVariable{
							{Key: "LOCAL_VAR", Value: "local2"},
						},
					},
					{Alias: "replica3"},
				},
			},
		},
	}

	job, err := ConvertToJobRequest(&req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedPeersValue := "replica1,replica2,replica3"

	// Test container should have PEERS env
	testNode := job.TestNode
	testEnvs := make(map[string]string)
	for _, env := range testNode.Envs {
		testEnvs[env.Key] = env.Value
	}
	if testEnvs["PEERS"] != expectedPeersValue {
		t.Errorf("test container PEERS env incorrect: expected %q, got %q", expectedPeersValue, testEnvs["PEERS"])
	}
	if testEnvs["TEST_VAR"] != "test" {
		t.Errorf("test container original env lost: expected 'test', got %q", testEnvs["TEST_VAR"])
	}

	// Each replica should have PEERS env
	for i := 1; i < len(job.SubmissionNodes); i++ {
		replicaNode := job.SubmissionNodes[i]
		replicaEnvs := make(map[string]string)
		for _, env := range replicaNode.Envs {
			replicaEnvs[env.Key] = env.Value
		}
		if replicaEnvs["PEERS"] != expectedPeersValue {
			t.Errorf("replica %d PEERS env incorrect: expected %q, got %q", i, expectedPeersValue, replicaEnvs["PEERS"])
		}
	}
}

func TestConvertToJobRequest_PeersEnvSingleReplica(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID: validUUID,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				Alias: "test",
			},
			Submission: types.SubmissionConfig{
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{Alias: "only-replica"},
				},
			},
		},
	}

	job, err := ConvertToJobRequest(&req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedPeersValue := "only-replica"

	// Check test container
	testEnvs := make(map[string]string)
	for _, env := range job.TestNode.Envs {
		testEnvs[env.Key] = env.Value
	}
	if testEnvs["PEERS"] != expectedPeersValue {
		t.Errorf("test container PEERS env incorrect: expected %q, got %q", expectedPeersValue, testEnvs["PEERS"])
	}

	// Check replica
	replicaEnvs := make(map[string]string)
	for _, env := range job.SubmissionNodes[0].Envs {
		replicaEnvs[env.Key] = env.Value
	}
	if replicaEnvs["PEERS"] != expectedPeersValue {
		t.Errorf("replica PEERS env incorrect: expected %q, got %q", expectedPeersValue, replicaEnvs["PEERS"])
	}
}

func TestConvertToJobRequest_PeersEnvOverridesBehavior(t *testing.T) {
	validUUID := uuid.New().String()

	req := types.JobRequest{
		JobUID: validUUID,
		Nodes: types.ContainerConfigs{
			TestContainer: types.TestContainerConfig{
				Alias: "test",
				Envs: []types.EnvironmentVariable{
					{Key: "PEERS", Value: "should-be-overridden"},
				},
			},
			Submission: types.SubmissionConfig{
				GlobalEnvs: []types.EnvironmentVariable{
					{Key: "PEERS", Value: "global-peers"},
				},
				ReplicaConfigs: []types.IncomingReplicaConfig{
					{
						Alias: "replica1",
						Envs: []types.EnvironmentVariable{
							{Key: "PEERS", Value: "local-peers"},
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

	expectedPeersValue := "replica1"

	// Test container: PEERS from createPeerAliasEnv should override original
	testEnvs := make(map[string]string)
	for _, env := range job.TestNode.Envs {
		testEnvs[env.Key] = env.Value
	}
	if testEnvs["PEERS"] != expectedPeersValue {
		t.Errorf("test container PEERS should be overridden: expected %q, got %q", expectedPeersValue, testEnvs["PEERS"])
	}

	// Replica: PEERS from createPeerAliasEnv should override all others
	replicaEnvs := make(map[string]string)
	for _, env := range job.SubmissionNodes[0].Envs {
		replicaEnvs[env.Key] = env.Value
	}
	if replicaEnvs["PEERS"] != expectedPeersValue {
		t.Errorf("replica PEERS should be overridden: expected %q, got %q", expectedPeersValue, replicaEnvs["PEERS"])
	}
}
