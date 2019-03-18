package djoemo

import "context"

// MetricsInterface provides an interface for metrics publisher
type MetricsInterface interface {
	// WithContext adds context to logger
	WithContext(ctx context.Context) MetricsInterface
	// Publish publishes metrics
	Publish(key string, metricName string, metricValue float64) error
}
