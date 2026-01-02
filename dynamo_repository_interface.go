package djoemo

import (
	"context"

	"github.com/adjoeio/djoemo/logger"
	"github.com/adjoeio/djoemo/metrics"
	"github.com/adjoeio/djoemo/model"
)

//go:generate mockgen -source=dynamo_repository_interface.go -destination=./mock/dynamo_repository_interface.go -package=mock .

// RepositoryInterface provides an interface to enable mocking the AWS dynamodb repository
// for testing your code.
type RepositoryInterface interface {
	// WithLog enables logging; it accepts LogInterface as logger
	WithLog(log logger.LogInterface)

	// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
	WithMetrics(metricsInterface metrics.MetricsInterface)

	// GetItemWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetItemWithContext(ctx context.Context, key model.Key, item any) (bool, error)

	// SaveItemWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemWithContext(ctx context.Context, key model.Key, item any) error

	// UpdateWithContext updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update; context which used to enable log with context
	// returns error in case of error
	UpdateWithContext(ctx context.Context, expression UpdateExpression, key model.Key, values map[string]any) error

	// UpdateWithUpdateExpressions updates an item with update expressions defined at field level, enabling you to set
	// different update expressions for each field. The first key of the updateMap specifies the Update expression to use
	// for the expressions in the map
	UpdateWithUpdateExpressions(ctx context.Context, key model.Key, updateExpressions UpdateExpressions) error

	// UpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions defined at field level and returns
	// the item, as it appears after the update, enabling you to set different update expressions for each field. The first
	// key of the updateMap specifies the Update expression to use for the expressions in the map
	UpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key model.Key, item any, updateExpressions UpdateExpressions) error

	// ConditionalUpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions and a condition.
	// If the condition is met, the item will be updated and returned as it appears after the update.
	// The first key of the updateMap specifies the Update expression to use for the expressions in the map
	ConditionalUpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key model.Key, item any, updateExpressions UpdateExpressions, conditionExpression string, conditionArgs ...any) (conditionMet bool, err error)

	// DeleteItemWithContext item by its key; it accepts key of item to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemWithContext(ctx context.Context, key model.Key) error

	// SaveItemsWithContext batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemsWithContext(ctx context.Context, key model.Key, items any) error

	// DeleteItemsWithContext deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemsWithContext(ctx context.Context, key []model.Key) error

	// GetItemsWithContext by key; it accepts key of item to get it; context which used to enable log with context
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(ctx context.Context, key model.Key, out any) (bool, error)

	// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
	// context which used to enable log with context, the output will be given in items
	// returns error in case of error
	QueryWithContext(ctx context.Context, query QueryInterface, item any) error

	// GIndex returns index repository
	GIndex(name string) GlobalIndexInterface

	// OptimisticLockSaveWithContext saves an item if the version attribute on the server matches the version of the object
	OptimisticLockSaveWithContext(ctx context.Context, key model.Key, item any) (bool, error)

	// ScanIteratorWithContext returns an instance of an iterator that provides methods to use for scanning tables
	ScanIteratorWithContext(ctx context.Context, key model.Key, searchLimit int64) (IteratorInterface, error)

	// ConditionalUpdateWithContext updates an item if the passed expression and condition evaluates to true
	ConditionalUpdateWithContext(ctx context.Context, key model.Key, item any, expression string, expressionArgs ...any) (bool, error)

	// BatchGetItemsWithContext gets multiple items by their keys; it accepts a slice of keys (all from the same table)
	// and fills out (pointer to a slice) with any found items.
	// returns true if at least one item is found, returns false and nil if no items found, returns false and error in case of error
	BatchGetItemsWithContext(ctx context.Context, keys []model.Key, out any) (bool, error)
}
