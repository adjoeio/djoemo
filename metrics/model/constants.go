package model

import "context"

const (
	// MetricNameSavedItemsCount save count metrics key
	MetricNameSavedItemsCount = "ItemsSavedCount"
	// MetricNameUpdatedItemsCount update count metrics key
	MetricNameUpdatedItemsCount = "ItemsUpdatedCount"
	// MetricNameDeleteItemsCount delete count metrics key
	MetricNameDeleteItemsCount = "ItemsDeleteCount"
	// MetricNameQueryItemsCount query count metrics key
	MetricNameQueryItemsCount = "ItemsQueryCount"
)

const (
	StatusSuccess = "success"
	StatusFailure = "failure"
)

type djoemoMetricsContextKey string

const (
	ContextKeySource djoemoMetricsContextKey = "source"
)

// Helper to inject the caller into context
func WithCaller(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, ContextKeySource, name)
}
