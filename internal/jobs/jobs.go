package jobs

import (
	"time"

	"github.com/google/uuid"
)

type Claimed struct {
	ID           uuid.UUID
	Fence        int64
	Type         string
	Payload      []byte
	AttemptCount int
	DeadlineAt   time.Time
}

type ClaimRequest struct {
	QueueID   uuid.UUID
	WorkerID  uuid.UUID
	FreeSlots int
	Lease     time.Duration
	Types     []string
}
