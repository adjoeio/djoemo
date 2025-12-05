package djoemo

import (
	"context"
)

// RepositoryInterface provides an interface to enable mocking the AWS dynamodb repository
// for testing your code.
type RepositoryInterface interface {
	// WithLog enables logging; it accepts LogInterface as logger
	WithLog(log LogInterface)

	// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
	WithMetrics(metricsInterface MetricsInterface)

	// GetItem get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetItem(key KeyInterface, item interface{}) (bool, error)

	// SaveItem item; it accepts a key interface, that is used to get the table name; item is the item to be saved
	// returns error in case of error
	SaveItem(key KeyInterface, item interface{}) error

	// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update;
	// returns error in case of error
	Update(expression UpdateExpression, key KeyInterface, values map[string]interface{}) error

	// DeleteItem item by key; returns error in case of error
	DeleteItem(key KeyInterface) error

	// SaveItems batch save a slice of items by key; returns error in case of error
	SaveItems(key KeyInterface, items interface{}) error

	// DeleteItems deletes items matching the keys; returns error in case of error
	DeleteItems(key []KeyInterface) error

	// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItems(key KeyInterface, items interface{}) (bool, error)

	// GetItemWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetItemWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error)

	// SaveItemWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemWithContext(ctx context.Context, key KeyInterface, item interface{}) error

	// UpdateWithContext updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update; context which used to enable log with context
	// returns error in case of error
	UpdateWithContext(ctx context.Context, expression UpdateExpression, key KeyInterface, values map[string]interface{}) error

	// UpdateWithUpdateExpressions updates an item with update expressions defined at field level, enabling you to set
	// different update expressions for each field. The first key of the updateMap specifies the Update expression to use
	// for the expressions in the map
	UpdateWithUpdateExpressions(ctx context.Context, key KeyInterface, updateExpressions UpdateExpressions) error

	// ConditionalUpdateWithUpdateExpressions updates an item with update expressions and optional conditions defined at field level
	// if no conditions were provided within UpdateOption, a normal update will be performed
	ConditionalUpdateWithUpdateExpressions(ctx context.Context, key KeyInterface, updateExpressions UpdateExpressions, updateOptions ...UpdateOption) (bool, error)

	// UpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions defined at field level and returns
	// the item, as it appears after the update, enabling you to set different update expressions for each field. The first
	// key of the updateMap specifies the Update expression to use for the expressions in the map
	UpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key KeyInterface, item interface{}, updateExpressions UpdateExpressions) error

	// ConditionalUpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions and a condition.
	// If the condition is met, the item will be updated and returned as it appears after the update.
	// The first key of the updateMap specifies the Update expression to use for the expressions in the map
	ConditionalUpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key KeyInterface, item interface{}, updateExpressions UpdateExpressions, conditionExpression string, conditionArgs ...interface{}) (conditionMet bool, err error)

	// DeleteItemWithContext item by its key; it accepts key of item to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemWithContext(ctx context.Context, key KeyInterface) error

	// SaveItemsWithContext batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) error

	// DeleteItemsWithContext deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemsWithContext(ctx context.Context, key []KeyInterface) error

	// GetItemsWithContext by key; it accepts key of item to get it; context which used to enable log with context
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(ctx context.Context, key KeyInterface, out interface{}) (bool, error)

	// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
	// context which used to enable log with context, the output will be given in items
	// returns error in case of error
	QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) error

	// Query by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
	// returns error in case of error
	Query(query QueryInterface, item interface{}) error

	// GIndex returns index repository
	GIndex(name string) GlobalIndexInterface

	//OptimisticLockSaveWithContext saves an item if the version attribute on the server matches the version of the object
	OptimisticLockSaveWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error)

	//OptimisticLockSave ...
	OptimisticLockSave(key KeyInterface, item interface{}) (bool, error)

	//ScanIteratorWithContext returns an instance of an iterator that provides methods to use for scanning tables
	ScanIteratorWithContext(ctx context.Context, key KeyInterface, searchLimit int64) (IteratorInterface, error)

	//ConditionalUpdateWithContext updates an item if the passed expression and condition evaluates to true
	ConditionalUpdateWithContext(ctx context.Context, key KeyInterface, item interface{}, expression string, expressionArgs ...interface{}) (bool, error)

	//ConditionalUpdate updates an item if the passed expression and condition evaluates to true
	ConditionalUpdate(key KeyInterface, item interface{}, expression string, expressionArgs ...interface{}) (bool, error)
}
