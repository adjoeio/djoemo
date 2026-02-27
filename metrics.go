package djoemo

import (
	"context"
	"sync"
	"time"
)

// MetricsInterface provides an interface for metrics publisher
//
//go:generate mockgen -source=metrics.go -destination=./mock/metrics_interface.go -package=mock .\
type MetricsInterface interface {
	Record(ctx context.Context, caller string, key KeyInterface, duration time.Duration, success bool)
}

const (
	labelSource = "source"

	StatusSuccess = "success"
	StatusFailure = "failure"

	OpCommit = "commit"
	OpUpdate = "update"
	OpRead   = "read"
	OpDelete = "delete"
)

type customMetricsLabelsContextKey int

const customLabelsCtxKey customMetricsLabelsContextKey = iota

type customLabels struct {
	sync.RWMutex
	Labels map[string]string
}

func AddMetrics(ctx context.Context, key, value string) context.Context {
	labels, ok := ctx.Value(customLabelsCtxKey).(*customLabels)
	if !ok {
		labels = &customLabels{
			Labels: make(map[string]string),
		}
		ctx = context.WithValue(ctx, customLabelsCtxKey, labels)
	}

	labels.Lock()
	defer labels.Unlock()
	labels.Labels[key] = value

	return ctx
}

// WithSourceLabel is a label to tag buisness logic as default metrics are aggregated for CURD operations to reduce cardinality
func WithSourceLabel(ctx context.Context, value string) context.Context {
	return AddMetrics(ctx, labelSource, value)
}

func GetLabelsFromContext(ctx context.Context) map[string]string {
	customLabels, ok := ctx.Value(customLabelsCtxKey).(*customLabels)
	if !ok || customLabels == nil {
		return nil
	}
	return customLabels.Labels
}

func New() *Metrics {
	return &Metrics{}
}

type Metrics struct {
	metrics []MetricsInterface
}

func (m *Metrics) Add(metric MetricsInterface) {
	m.metrics = append(m.metrics, metric)
}

func (m *Metrics) Record(ctx context.Context, op string, key KeyInterface, duration time.Duration, success bool) {
	for _, metric := range m.metrics {
		metric.Record(ctx, op, key, duration, success)
	}
}

func (m *Metrics) RecordMultiple(ctx context.Context, op string, key []KeyInterface, duration time.Duration, success bool) {
	for _, key := range key {
		m.Record(ctx, op, key, duration, success)
	}
}
