package utils

import (
	"fmt"
	"time"
)

func NewRunID(workflowID string) string {
	return fmt.Sprintf("%s-%d", workflowID, time.Now().UTC().UnixNano())
}
