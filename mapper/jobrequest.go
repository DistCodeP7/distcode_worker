package mapper

import (
	"fmt"
	"strings"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

// CloneFileMap creates a deep copy of a string-to-string map and converts it to a FileMap.
// It takes an original map[string]string and returns a new types.FileMap where each key
// is converted to types.Path and each value is converted to types.SourceCode.
func CloneFileMap(original map[string]string) types.FileMap {
	clone := make(types.FileMap)
	for k, v := range original {
		clone[types.Path(k)] = types.SourceCode(v)
	}
	return clone
}

// MergeEnvs combines multiple slices of environment variables, with later slices
// taking precedence over earlier ones for duplicate keys.
// Returns a new slice containing all unique environment variables.
func MergeEnvs(envSlices ...[]types.EnvironmentVariable) []types.EnvironmentVariable {
	envMap := make(map[string]string)
	for _, slice := range envSlices {
		for _, env := range slice {
			envMap[env.Key] = env.Value
		}
	}

	mergedEnvs := make([]types.EnvironmentVariable, 0, len(envMap))
	for k, v := range envMap {
		mergedEnvs = append(mergedEnvs, types.EnvironmentVariable{Key: k, Value: v})
	}

	return mergedEnvs
}

// createPeerAliasEnv creates an environment variable containing a comma-separated
// list of peer aliases from the provided replica configurations. The resulting
// environment variable has the key "PEERS".
func createPeerAliasEnv(replicas []types.IncomingReplicaConfig) types.EnvironmentVariable {
	peers := make([]string, len(replicas))
	for i, replica := range replicas {
		peers[i] = replica.Alias
	}

	return types.EnvironmentVariable{
		Key:   "PEERS",
		Value: strings.Join(peers, ","),
	}
}

var MaxReplicaConfigs = 5

// ConvertToJobRequest converts a JobRequest into a Job by validating replica configurations,
// parsing the job UID, and creating node specifications for test containers and submission replicas.
func ConvertToJobRequest(req *types.JobRequest) (*types.Job, error) {
	if len(req.Nodes.Submission.ReplicaConfigs) == 0 {
		return nil, fmt.Errorf("at least one replica config is required in submission")
	}
	if len(req.Nodes.Submission.ReplicaConfigs) > MaxReplicaConfigs {
		return nil, fmt.Errorf("number of replica configs exceeds the maximum allowed (%d)", MaxReplicaConfigs)
	}

	jobUID, err := uuid.Parse(req.JobUID)
	if err != nil {
		return nil, err
	}

	peersEnv := createPeerAliasEnv(req.Nodes.Submission.ReplicaConfigs)

	// Test Container Node
	testNode := types.NodeSpec{
		Alias:        req.Nodes.TestContainer.Alias,
		Files:        CloneFileMap(req.Nodes.TestContainer.TestFiles),
		BuildCommand: req.Nodes.TestContainer.BuildCommand,
		EntryCommand: req.Nodes.TestContainer.EntryCommand,
		Envs: MergeEnvs(
			req.Nodes.TestContainer.Envs,
			[]types.EnvironmentVariable{peersEnv},
			[]types.EnvironmentVariable{{Key: "ID", Value: req.Nodes.TestContainer.Alias}},
		),
	}

	var nodes []types.NodeSpec

	// Submission Replicas
	for _, replica := range req.Nodes.Submission.ReplicaConfigs {
		mergedEnvs := MergeEnvs(
			req.Nodes.Submission.GlobalEnvs,
			replica.Envs,
			[]types.EnvironmentVariable{peersEnv},
			[]types.EnvironmentVariable{{Key: "ID", Value: replica.Alias}},
		)

		nodes = append(nodes, types.NodeSpec{
			Alias:        replica.Alias,
			Files:        CloneFileMap(req.Nodes.Submission.SubmissionCode),
			BuildCommand: req.Nodes.Submission.BuildCommand,
			EntryCommand: req.Nodes.Submission.EntryCommand,
			Envs:         mergedEnvs,
		})
	}

	return &types.Job{
		JobUID:          jobUID,
		UserID:          req.UserId,
		Timeout:         time.Duration(req.Timeout) * time.Second,
		TestNode:        testNode,
		SubmissionNodes: nodes,
		SubmittedAt:     req.SubmittedAt,
		ProblemID:       req.ProblemID,
	}, nil
}
