package djoemo

import (
	"context"
)

type logger struct {
	log LogInterface
}

// Infof logs info
func (l logger) Infof(table string, format string, ctx context.Context, args ...interface{}) {
	if l.log == nil {
		return
	}
	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Infof(format, args)
}

// Warnf logs warning
func (l logger) Warnf(table string, format string, ctx context.Context, args ...interface{}) {
	if l.log == nil {
		return
	}

	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Warnf(format, args)
}

// Errorf logs error
func (l logger) Errorf(table string, format string, ctx context.Context, args ...interface{}) {
	if l.log == nil {
		return
	}

	log := l.log.WithFields(map[string]interface{}{TableName: table})
	if ctx != nil {
		log = log.WithContext(ctx)
	}

	log.Errorf(format, args)
}
