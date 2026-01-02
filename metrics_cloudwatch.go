package djoemo

import (
	"context"
	"time"
)

type cloudwatchmetrics struct{}

func (m *cloudwatchmetrics) Record(ctx context.Context, caller string, key KeyInterface, duration time.Duration, err error) {
	// TODO: implement legacy CloudWatch if required. Return err, use WithCloudWatch/Add, etc
	// m.Publish()
}

// Publish publishes metrics
func (m *cloudwatchmetrics) Publish(ctx context.Context, key string, metricName string, metricValue float64) {
}
