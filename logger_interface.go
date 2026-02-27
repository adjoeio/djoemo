package djoemo

import (
	"context"
)

// LogInterface provides an interface for logging
//
//go:generate mockgen -source=logger_interface.go -destination=./mock/log_interface.go -package=mock .
type LogInterface interface {
	// WithContext adds context to logger
	WithContext(ctx context.Context) LogInterface
	// WithField adds fields from map string interface to logger
	WithField(key string, value any) LogInterface
	// WithFields adds fields from map string interface to logger
	WithFields(fields map[string]any) LogInterface
	// Info logs info
	Info(message string)
	// warn logs warning
	Warn(message string)
	// error logs error
	Error(message string)
}
