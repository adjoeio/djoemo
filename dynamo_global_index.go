package djoemo

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/guregu/dynamo"
	"github.com/prometheus/client_golang/prometheus"
)

// GlobalIndex models a global secondary index used in a query
type GlobalIndex struct {
	name         string
	dynamoClient *dynamo.DB
	log          LogInterface
	metrics      *Metrics
}

// WithLog enables logging; it accepts LogInterface as logger
func (gi *GlobalIndex) WithLog(log LogInterface) {
	gi.log = log
}

// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
func (gi *GlobalIndex) WithMetrics(metricsInterface MetricsInterface) {
	gi.metrics.Add(metricsInterface)
}

// WithPrometheusMetrics enables prometheus metrics
func (gi *GlobalIndex) WithPrometheusMetrics(registry *prometheus.Registry) GlobalIndexInterface {
	prommetrics := NewPrometheusMetrics(registry)
	gi.metrics.Add(prommetrics)
	return gi
}

// GetItemWithContext item; it needs a key interface that is used to get the table name, hash key, and the range key if it exists; output will be contained in item; context is optional param, which used to enable log with context
func (gi GlobalIndex) GetItemWithContext(ctx context.Context, key KeyInterface, item any) (bool, error) {
	var err error
	defer gi.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidKey(key); err != nil {
		return false, err
	}

	err = buildTableKeyCondition(gi.table(key.TableName()), key).Index(gi.name).OneWithContext(ctx, item)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			gi.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(ErrNoItemFound.Error())
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// GetItemsWithContext queries multiple items by key (hash key) and returns it in the slice of items items
func (gi GlobalIndex) GetItemsWithContext(ctx context.Context, key KeyInterface, items any) (bool, error) {
	var err error
	defer gi.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidKey(key); err != nil {
		return false, err
	}

	err = gi.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).Index(gi.name).AllWithContext(ctx, items)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			gi.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(ErrNoItemFound.Error())
			return false, nil
		}

		return false, err
	}

	val := reflect.ValueOf(items)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
		if val.Len() == 0 {
			return false, nil
		}
	}

	return true, nil
}

// GetItemsWithRangeWithContext queries multiple items by key (hash key) and returns it in the slice of items respecting the range key
func (gi GlobalIndex) GetItemsWithRangeWithContext(ctx context.Context, key KeyInterface, items any) (bool, error) {
	var err error
	defer gi.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidKey(key); err != nil {
		return false, err
	}

	err = buildTableKeyCondition(gi.table(key.TableName()), key).Index(gi.name).AllWithContext(ctx, items)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			gi.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(ErrNoItemFound.Error())
			return false, nil
		}

		return false, err
	}

	val := reflect.ValueOf(items)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
		if val.Len() == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (gi GlobalIndex) table(tableName string) dynamo.Table {
	return gi.dynamoClient.Table(tableName)
}

// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// context which used to enable log with context, the output will be given in items
// returns error in case of error
func (gi GlobalIndex) QueryWithContext(ctx context.Context, query QueryInterface, item any) (err error) {
	defer gi.recordMetrics(ctx, OpRead, query, &err)()

	if !IsPointerOFSlice(item) {
		return ErrInvalidPointerSliceType
	}
	if err = isValidKey(query); err != nil {
		return err
	}

	q := gi.table(query.TableName()).Get(*query.HashKeyName(), query.HashKey()).Index(gi.name)

	// by range
	if query.RangeKeyName() != nil && query.RangeKey() != nil {
		q = q.Range(*query.RangeKeyName(), dynamo.Operator(query.RangeOp()), query.RangeKey())
	}

	if limit := valueFromPtr(query.Limit()); limit > 0 {
		q = q.Limit(limit)
	}

	if query.Descending() {
		q = q.Order(dynamo.Descending)
	}

	err = q.AllWithContext(ctx, item)
	if err != nil {
		return err
	}

	return nil
}

func (gi GlobalIndex) recordMetrics(ctx context.Context, op string, key KeyInterface, err *error) func() {
	start := time.Now()
	return func() {
		gi.metrics.Record(ctx, op, key, time.Since(start), isOpSuccess(err))
	}
}
