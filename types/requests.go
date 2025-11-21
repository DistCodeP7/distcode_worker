package types

import (
	"time"

	"github.com/google/uuid"
)

type JobRequest struct {
	JobUID    uuid.UUID
	ProblemId int
	Nodes     []NodeSpec
	UserId    string
	Timeout   time.Duration
}
