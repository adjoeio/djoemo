package djoemo

import (
	"context"
)

type GlobalIndexInterface interface {
	// GetWithContext get item from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
	// context which used to enable log with context; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetWithContext(key KeyInterface, item interface{}, ctx context.Context) (bool, error)

	// GetItemsWithContext by key from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
	// context which used to enable log with context, the output will be given in items
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(key KeyInterface, items interface{}, ctx context.Context) (bool, error)

	// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItems(key KeyInterface, items interface{}) (bool, error)

	// Get get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	Get(key KeyInterface, item interface{}) (bool, error)
}
