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

	// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update; context which used to enable log with context
	// returns error in case of error
	UpdateWithContext(ctx context.Context, expression UpdateExpression, key KeyInterface, values map[string]interface{}) error

	// DeleteItem item by its key; it accepts key of item to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemWithContext(ctx context.Context, key KeyInterface) error

	// SaveItems batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) error

	// DeleteItems deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemsWithContext(ctx context.Context, key []KeyInterface) error

	// GetItems by key; it accepts key of item to get it; context which used to enable log with context
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(ctx context.Context, key KeyInterface, out interface{}) (bool, error)

	// GIndex returns index repository
	GIndex(name string) GlobalIndexInterface
}
