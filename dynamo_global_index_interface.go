package djoemo

import (
	"context"

	"github.com/adjoeio/djoemo/model"
)

//go:generate mockgen -source=dynamo_global_index_interface.go -destination=./mock/dynamo_global_index_interface.go -package=mock .

type GlobalIndexInterface interface {
	// GetItemWithContext get item from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
	// context which used to enable log with context; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetItemWithContext(ctx context.Context, key model.Key, item interface{}) (bool, error)

	// GetItemsWithContext by key from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
	// context which used to enable log with context, the output will be given in items
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(ctx context.Context, key model.Key, items interface{}) (bool, error)

	// GetItemsWithRangeWithContext same as GetItemsWithContext, but also respects range key
	GetItemsWithRangeWithContext(ctx context.Context, key model.Key, items interface{}) (bool, error)

	// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
	// context which used to enable log with context, the output will be given in items
	// returns error in case of error
	QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) error
}
