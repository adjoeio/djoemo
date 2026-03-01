package djoemo

import (
	"context"
	"fmt"
	"log"
	"maps"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusConfig holds configuration for Prometheus metrics.
type PrometheusConfig struct {
	// Namespace is the metric namespace (e.g., "adjoe").
	Namespace string
	// Subsystem is the metric subsystem (e.g., "djoemo").
	Subsystem string
	// HistogramBuckets defines the histogram bucket boundaries in seconds.
	// If nil, defaults to ExponentialBuckets(0.001, 2.5, 12) (1ms to ~60s).
	HistogramBuckets []float64
	// ConstLabels are labels added to all metrics.
	ConstLabels prometheus.Labels
	// Log is an optional logger for panic recovery. If nil, uses standard log.
	Log LogInterface
}

// DefaultPrometheusConfig returns a config with sensible defaults.
func DefaultPrometheusConfig() *PrometheusConfig {
	return &PrometheusConfig{
		Namespace:        "adjoe",
		Subsystem:        "djoemo",
		HistogramBuckets: prometheus.ExponentialBuckets(0.001, 2.5, 12),
		Log:              NewNopLog(),
	}
}

type prometheusmetrics struct {
	registry      *prometheus.Registry
	cfg           *PrometheusConfig
	mu            sync.RWMutex
	queryCount    map[string]*prometheus.CounterVec
	queryDuration map[string]*prometheus.HistogramVec
}

var metricLabelNames = []string{statusLabel, tableLabel, sourceLabel}

func (m *prometheusmetrics) newCounter(caller string) *prometheus.CounterVec {
	opts := prometheus.CounterOpts{
		Namespace:   m.cfg.Namespace,
		Subsystem:   m.cfg.Subsystem,
		Name:        strings.ToLower(caller),
		Help:        "counter for function " + caller,
		ConstLabels: m.cfg.ConstLabels,
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
	buckets := m.cfg.HistogramBuckets
	if len(buckets) == 0 {
		buckets = prometheus.ExponentialBuckets(0.001, 2.5, 12)
	}
	opts := prometheus.HistogramOpts{
		Namespace:   m.cfg.Namespace,
		Subsystem:   m.cfg.Subsystem,
		Name:        strings.ToLower(caller) + "_duration_seconds",
		Help:        "histogram duration for function " + caller + " in seconds",
		Buckets:     buckets,
		ConstLabels: m.cfg.ConstLabels,
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

// NewPrometheusMetrics creates Prometheus metrics with default config.
func NewPrometheusMetrics(registry *prometheus.Registry, cfg *PrometheusConfig) *prometheusmetrics {
	if cfg == nil {
		cfg = DefaultPrometheusConfig()
	}
	m := &prometheusmetrics{
		registry:      registry,
		cfg:           cfg,
		queryCount:    make(map[string]*prometheus.CounterVec),
		queryDuration: make(map[string]*prometheus.HistogramVec),
	}
	return m
}

func (m *prometheusmetrics) Record(ctx context.Context, caller string, key KeyInterface, duration time.Duration, success bool) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("prometheus metrics Record panic recovered: caller=%q panic=%v", caller, r)
			if m.cfg.Log != nil {
				m.cfg.Log.WithContext(ctx).Error(msg)
			} else {
				log.Printf("djoemo: %s", msg)
			}
		}
	}()

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
