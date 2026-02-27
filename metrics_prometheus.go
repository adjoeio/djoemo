package djoemo

import (
	"context"
	"maps"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type prometheusmetrics struct {
	registry      *prometheus.Registry
	mu            sync.RWMutex
	queryCount    map[string]*prometheus.CounterVec
	queryDuration map[string]*prometheus.HistogramVec
}

var metricLabelNames = []string{statusLabel, tableLabel, sourceLabel}

func (m *prometheusmetrics) newCounter(caller string) *prometheus.CounterVec {
	opts := prometheus.CounterOpts{
		Name: strings.ToLower(caller),
		Help: "counter for function " + caller,
	}
	counter := prometheus.NewCounterVec(opts, metricLabelNames)
	if err := m.registry.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.CounterVec)
		}
		panic(err)
	}
	return counter
}

func (m *prometheusmetrics) newHistogramVec(caller string) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{
		Name:    strings.ToLower(caller) + "_duration_seconds",
		Help:    "histogram duration for function " + caller + " in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2.5, 5),
	}
	// WARNING: add high cardinality labels like sdkhash, etc with caution
	histogram := prometheus.NewHistogramVec(opts, metricLabelNames)
	if err := m.registry.Register(histogram); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.HistogramVec)
		}
		panic(err)
	}
	return histogram
}

const (
	statusLabel = "status"
	callerLabel = "caller" // NOTE: used separate metrics for now
	sourceLabel = "source"
	tableLabel  = "table"
)

func NewPrometheusMetrics(registry *prometheus.Registry) *prometheusmetrics {
	m := &prometheusmetrics{
		registry:      registry,
		queryCount:    make(map[string]*prometheus.CounterVec),
		queryDuration: make(map[string]*prometheus.HistogramVec),
	}
	return m
}

func (m *prometheusmetrics) Record(ctx context.Context, caller string, key KeyInterface, duration time.Duration, success bool) {
	m.mu.RLock()
	counter, counterOk := m.queryCount[caller]
	histogram, histogramOk := m.queryDuration[caller]
	m.mu.RUnlock()

	if !counterOk || !histogramOk {
		m.mu.Lock()
		if m.queryCount[caller] == nil {
			m.queryCount[caller] = m.newCounter(caller)
		}
		if m.queryDuration[caller] == nil {
			m.queryDuration[caller] = m.newHistogramVec(caller)
		}
		counter = m.queryCount[caller]
		histogram = m.queryDuration[caller]
		m.mu.Unlock()
	}

	status := StatusFailure
	if success {
		status = StatusSuccess
	}

	table := ""
	if key != nil && key.TableName() != "" {
		table = strings.ToLower(key.TableName())
	}

	labels := prometheus.Labels{
		statusLabel: status,
		tableLabel:  table,
	}
	maps.Copy(labels, GetLabelsFromContext(ctx))
	if labels[sourceLabel] == "" {
		// Set to the filename of the calling function rather than the caller string
		if _, file, _, ok := runtime.Caller(2); ok {
			// Extract just the file name, not the full path
			_, filename := path.Split(file)
			labels[sourceLabel] = filename
		} else {
			labels[sourceLabel] = "unknown"
		}
	}

	counter.With(labels).Inc()
	histogram.With(labels).Observe(duration.Seconds())
}
