package metrics

import (
	"context"
	"time"

	prometheusmetrics "github.com/adjoeio/djoemo/metrics/prometheus"
	"github.com/adjoeio/djoemo/model"
)

// MetricsInterface provides an interface for metrics publisher
//
//go:generate mockgen -source=metrics.go -destination=../mock/metrics_interface.go -package=mock .\
type MetricsInterface interface {
	Record(ctx context.Context, caller string, key model.Key, duration time.Duration, err *error)
}

func New() *Metrics {
	return &Metrics{}
}

type Metrics struct {
	metrics []MetricsInterface
}

func (m *Metrics) WithPrometheus() *Metrics {
	prommetrics := prometheusmetrics.New()
	m.Add(prommetrics)
	return m
}

func (m *Metrics) Add(metric MetricsInterface) {
	m.metrics = append(m.metrics, metric)
}

func (m *Metrics) Record(ctx context.Context, caller string, key model.Key, duration time.Duration, err *error) {
	for _, metric := range m.metrics {
		metric.Record(ctx, caller, key, duration, err)
	}
}

func (m *Metrics) RecordMultiple(ctx context.Context, caller string, key []model.Key, duration time.Duration, err *error) {
	for _, key := range key {
		m.Record(ctx, caller, key, duration, err)
	}
}
