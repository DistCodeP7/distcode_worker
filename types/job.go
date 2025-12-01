package types

import (
	"time"

	"github.com/google/uuid"
)

type JobRequest struct {
	JobUID  string           `json:"jobUid"`
	Nodes   ContainerConfigs `json:"nodes"`
	UserId  string           `json:"userId"`
	Timeout int              `json:"timeout"`
}

type ContainerConfigs struct {
	TestContainer TestContainerConfig `json:"testContainer"`
	Submission    SubmissionConfig    `json:"submission"`
}

type IncomingReplicaConfig struct {
	Alias string                `json:"alias"`
	Envs  []EnvironmentVariable `json:"envs"`
}

type SubmissionConfig struct {
	SubmissionCode map[string]string       `json:"submissionCode"`
	BuildCommand   string                  `json:"buildCommand"`
	EntryCommand   string                  `json:"entryCommand"`
	GlobalEnvs     []EnvironmentVariable   `json:"globalEnvs"`
	ReplicaConfigs []IncomingReplicaConfig `json:"replicaConfigs"`
}

type TestContainerConfig struct {
	Alias        string                `json:"alias"`
	TestFiles    map[string]string     `json:"testFiles"`
	BuildCommand string                `json:"buildCommand"`
	EntryCommand string                `json:"entryCommand"`
	Envs         []EnvironmentVariable `json:"envs"`
}

type Job struct {
	JobUID  uuid.UUID
	Nodes   []NodeSpec
	UserId  string
	Timeout time.Duration
}
