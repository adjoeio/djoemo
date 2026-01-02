package prometheusmetrics

import (
	"context"
	"strings"
	"time"

	metricsModel "github.com/adjoeio/djoemo/metrics/model"
	"github.com/adjoeio/djoemo/model"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	registry *prometheus.Registry
	// cfg           config.PrometheusMetrics
	queryCount    map[string]*prometheus.CounterVec
	queryDuration map[string]*prometheus.HistogramVec
}

func (m *metrics) newCounter(caller string) *prometheus.CounterVec {
	opts := prometheus.CounterOpts{
		// Namespace: m.cfg.Namespace,
		// Subsystem: m.cfg.Subsystem,
		Name: strings.ToLower(caller),
		Help: "counter for function " + caller,
	}
	counter := prometheus.NewCounterVec(opts, []string{statusLabel})
	m.registry.MustRegister(counter)
	return counter
}

func (m *metrics) newHistogramVec(caller string) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{
		// Namespace: m.cfg.Namespace,
		// Subsystem: m.cfg.Subsystem,
		Name: strings.ToLower(caller),
		Help: "histogram duration for function " + caller,
		// WARNING: reduce the buckets after initial analysis
		Buckets: []float64{4, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 120},
	}
	// WARNING: add high cardinality labels like sdkhash, etc with caution
	histogram := prometheus.NewHistogramVec(opts, []string{statusLabel})
	m.registry.MustRegister(histogram)
	return histogram
}

const (
	statusLabel = "status"
	callerLabel = "caller" // NOTE: used separate metrics for now
	sourceLabel = "source"
	tableLabel  = "table"
)

func New() *metrics {
	m := &metrics{
		queryCount:    make(map[string]*prometheus.CounterVec),
		queryDuration: make(map[string]*prometheus.HistogramVec),
	}
	return m
}

func (m *metrics) Record(ctx context.Context, caller string, key model.Key, duration time.Duration, err *error) {
	if m.queryCount[caller] == nil || m.queryDuration[caller] == nil {
		m.queryCount[caller] = m.newCounter(caller)
		m.queryDuration[caller] = m.newHistogramVec(caller)
	}

	status := metricsModel.StatusSuccess
	if err != nil && *err != nil {
		status = metricsModel.StatusFailure
	}

	labels := prometheus.Labels{statusLabel: status}
	if key.TableName() != "" {
		labels[tableLabel] = strings.ToLower(key.TableName())
	}
	labels = m.fromContext(ctx, labels)

	m.queryCount[caller].With(labels).Inc()
	m.queryDuration[caller].With(labels).Observe(float64(duration))
}

func (m *metrics) fromContext(ctx context.Context, labels prometheus.Labels) prometheus.Labels {
	if ctx == nil {
		return labels
	}
	if source, ok := ctx.Value(metricsModel.ContextKeySource).(string); ok {
		labels[sourceLabel] = source
	}
	return labels
}
