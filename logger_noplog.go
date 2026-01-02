package djoemo

import (
	"context"
)

var nolog = &nopLog{}

func NewNopLog() LogInterface {
	return nolog
}

// nopLog logger to turn off logging.
type nopLog struct{}

// WithContext adds context to logger
func (l *nopLog) WithContext(ctx context.Context) LogInterface {
	return l
}

// WithFields adds fields from map string interface to logger
func (l *nopLog) WithFields(fields map[string]any) LogInterface {
	return l
}

// WithFields adds field with value to logger
func (l *nopLog) WithField(key string, value any) LogInterface {
	return l
}

// info logs info
func (l nopLog) Info(message string) {}

// warn logs warning
func (l nopLog) Warn(message string) {}

// info logs info
func (l nopLog) Error(message string) {}
