package models

import "time"

// DLQMessage represents a failed or malformed message routed to the Dead Letter Queue.
type DLQMessage struct {
	OriginalMessage string `json:"original_message"`
	Error           string `json:"error"`
	FailedAt        string `json:"failed_at"`
	ProcessorID     string `json:"processor_id"`
}

// NewDLQMessage creates a new DLQMessage with the current UTC timestamp.
func NewDLQMessage(originalMessage, reason, processorID string) *DLQMessage {
	if processorID == "" {
		processorID = "processor-1"
	}
	return &DLQMessage{
		OriginalMessage: originalMessage,
		Error:           reason,
		FailedAt:        time.Now().UTC().Format(time.RFC3339),
		ProcessorID:     processorID,
	}
}
