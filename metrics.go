package djoemo

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
func (m metrics) Publish(key string, metricName string, metricValue float64) error {
	if m.metrics != nil {
		return m.metrics.Publish(key, metricName, metricValue)
	}
	return nil
}
