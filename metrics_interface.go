package djoemo

// MetricsInterface provides an interface for metrics publisher
type MetricsInterface interface {
	// Publish publishes metrics
	Publish(key string, metricName string, metricValue float64) error
}
