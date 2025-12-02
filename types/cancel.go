package types

import "github.com/google/uuid"

type CancelJobRequest struct {
	JobUID uuid.UUID
}
