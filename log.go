package djoemo

import (
	"context"
)

type logger struct {
	log LogInterface
}

// info logs info
func (l logger) info(ctx context.Context, table string, message string) {
	l.log.WithFields(map[string]interface{}{TableName: table}).WithContext(ctx).Info(message)
}

// warn logs warning
func (l logger) warn(ctx context.Context, table string, message string) {
	l.log.WithFields(map[string]interface{}{TableName: table}).WithContext(ctx).Warn(message)
}

// error logs error
func (l logger) error(ctx context.Context, table string, message string) {
	l.log.WithFields(map[string]interface{}{TableName: table}).WithContext(ctx).Error(message)
}

//nopLog logger to turn off logging.
type nopLog struct{}

// WithContext adds context to logger
func (l nopLog) WithContext(ctx context.Context) LogInterface {
	return l
}

// WithFields adds fields from map string interface to logger
func (l nopLog) WithFields(fields map[string]interface{}) LogInterface {
	return l
}

// info logs info
func (l nopLog) Info(message string) {}

// warn logs warning
func (l nopLog) Warn(message string) {}

// info logs info
func (l nopLog) Error(message string) {}
