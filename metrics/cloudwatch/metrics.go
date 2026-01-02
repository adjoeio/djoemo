package cloudwatch

import (
	"context"
	"time"

	"github.com/adjoeio/djoemo/model"
)

type metrics struct{}

func (m *metrics) Record(ctx context.Context, caller string, key model.Key, duration time.Duration, err error) {
	// TODO: implement legacy CloudWatch if required. Return err, use WithCloudWatch/Add, etc
	// m.Publish()
}

// Publish publishes metrics
func (m *metrics) Publish(ctx context.Context, key string, metricName string, metricValue float64) {
}
