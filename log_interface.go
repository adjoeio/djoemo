package djoemo

import (
	"context"
)

// LogInterface provides an interface for logging
type LogInterface interface {
	// WithContext adds context to logger
	WithContext(ctx context.Context) LogInterface
	// WithFields adds fields from map string interface to logger
	WithFields(fields map[string]interface{}) LogInterface
	// Info logs info
	Info(message string)
	// warn logs warning
	Warn(message string)
	// error logs error
	Error(message string)
}
