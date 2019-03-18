package djoemo

import "context"

const (
	// MetricNameSavedItemsCount save count metrics key
	MetricNameSavedItemsCount = "ItemsSavedCount"
	// MetricNameUpdatedItemsCount update count metrics key
	MetricNameUpdatedItemsCount = "ItemsUpdatedCount"
	// MetricNameDeleteItemsCount delete count metrics key
	MetricNameDeleteItemsCount = "ItemsDeleteCount"
)

type metrics struct {
	metrics MetricsInterface
}

// Publish publishes metrics
func (m metrics) Publish(ctx context.Context, key string, metricName string, metricValue float64) error {
	if m.metrics == nil {
		return nil
	}

	return m.metrics.WithContext(ctx).Publish(key, metricName, metricValue)
}
