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
	// Infof logs info
	Infof(format string, args ...interface{})
	// Warnf logs warning
	Warnf(format string, args ...interface{})
	// Errorf logs error
	Errorf(format string, args ...interface{})
}
