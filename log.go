package djoemo

import (
	"context"
)

type logger struct {
	log LogInterface
}

// Infof logs info
func (l logger) Info(table string, message string, ctx context.Context) {
	if l.log == nil {
		return
	}
	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Info(message)
}

// Warnf logs warning
func (l logger) Warn(table string, message string, ctx context.Context) {
	if l.log == nil {
		return
	}

	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Warn(message)
}

// Errorf logs error
func (l logger) Error(table string, message string, ctx context.Context) {
	if l.log == nil {
		return
	}

	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Error(message)
}
