package mapper

import (
	"fmt"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

func CloneFileMap(original map[string]string) types.FileMap {
	clone := make(types.FileMap)
	for k, v := range original {
		clone[types.Path(k)] = types.SourceCode(v)
	}
	return clone
}

func MergeEnvs(globalEnvs, replicaEnvs []types.EnvironmentVariable) []types.EnvironmentVariable {
	envMap := make(map[string]string)

	for _, env := range globalEnvs {
		envMap[env.Key] = env.Value
	}

	for _, env := range replicaEnvs {
		envMap[env.Key] = env.Value
	}

	mergedEnvs := make([]types.EnvironmentVariable, 0, len(envMap))
	for k, v := range envMap {
		mergedEnvs = append(mergedEnvs, types.EnvironmentVariable{Key: k, Value: v})
	}

	return mergedEnvs
}

var (
	MaxReplicaConfigs = 5
)

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

	nodes := []types.NodeSpec{}

	// Test Container Node
	testNode := types.NodeSpec{
		Alias:        req.Nodes.TestContainer.Alias,
		Files:        CloneFileMap(req.Nodes.TestContainer.TestFiles),
		BuildCommand: req.Nodes.TestContainer.BuildCommand,
		EntryCommand: req.Nodes.TestContainer.EntryCommand,
		Envs:         req.Nodes.TestContainer.Envs,
	}
	nodes = append(nodes, testNode)

	for _, replica := range req.Nodes.Submission.ReplicaConfigs {
		nodes = append(nodes, types.NodeSpec{
			Alias:        replica.Alias,
			Files:        CloneFileMap(req.Nodes.Submission.SubmissionCode),
			BuildCommand: req.Nodes.Submission.BuildCommand,
			EntryCommand: req.Nodes.Submission.EntryCommand,
			Envs:         MergeEnvs(req.Nodes.Submission.GlobalEnvs, replica.Envs),
		})
	}

	return &types.Job{
		JobUID:  jobUID,
		UserId:  req.UserId,
		Timeout: time.Duration(req.Timeout) * time.Second,
		Nodes:   nodes,
	}, nil

}
