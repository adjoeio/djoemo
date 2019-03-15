package djoemo

import (
	"context"
	"github.com/guregu/dynamo"
)

// GlobalIndex models a global secondary index used in a query
type GlobalIndex struct {
	name         string
	dynamoClient *dynamo.DB
	log          logger
}

// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (gi GlobalIndex) GetItems(key KeyInterface, items interface{}) (bool, error) {
	return gi.GetItemsWithContext(key, items, nil)
}

// Get get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (gi GlobalIndex) Get(key KeyInterface, item interface{}) (bool, error) {
	return gi.GetWithContext(key, item, nil)
}

// Get item; it needs a key interface that is used to get the table name, hash key, and the range key if it exists; output will be contained in item; context is optional param, which used to enable log with context
func (gi GlobalIndex) GetWithContext(key KeyInterface, item interface{}, ctx context.Context) (bool, error) {

	if err := isValidKey(key); err != nil {
		gi.log.Error(key.TableName(), err.Error(), ctx)
		return false, err
	}

	// by hash
	query := gi.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		query = query.Range(*key.RangeKeyName(), dynamo.Equal, key.RangeKey())
	}

	err := query.Index(gi.name).One(item)
	if err != nil {
		if err == dynamo.ErrNotFound {
			gi.log.Info(key.TableName(), ErrNoItemFound.Error(), ctx)
			return false, nil
		}

		gi.log.Error(key.TableName(), err.Error(), ctx)
		return false, err
	}

	return true, nil

}

// GetItems queries multiple items by key (hash key) and returns it in the slice of items items
func (gi GlobalIndex) GetItemsWithContext(key KeyInterface, items interface{}, ctx context.Context) (bool, error) {

	if err := isValidKey(key); err != nil {
		gi.log.Error(key.TableName(), err.Error(), ctx)
		return false, err
	}

	err := gi.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).Index(gi.name).All(items)
	if err != nil {
		if err == dynamo.ErrNotFound {
			gi.log.Info(key.TableName(), ErrNoItemFound.Error(), ctx)
			return false, nil
		}

		gi.log.Error(key.TableName(), err.Error(), ctx)
		return false, err
	}

	return true, nil
}

func (gi GlobalIndex) table(tableName string) dynamo.Table {
	return gi.dynamoClient.Table(tableName)
}
