package djoemo

import (
	"context"
	"reflect"

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
	return gi.GetItemsWithContext(context.TODO(), key, items)
}

// GetItem get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (gi GlobalIndex) GetItem(key KeyInterface, item interface{}) (bool, error) {
	return gi.GetItemWithContext(context.TODO(), key, item)
}

// GetItemWithContext item; it needs a key interface that is used to get the table name, hash key, and the range key if it exists; output will be contained in item; context is optional param, which used to enable log with context
func (gi GlobalIndex) GetItemWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error) {

	if err := isValidKey(key); err != nil {
		gi.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	// by hash
	query := gi.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		query = query.Range(*key.RangeKeyName(), dynamo.Equal, key.RangeKey())
	}

	err := query.Index(gi.name).OneWithContext(ctx, item)
	if err != nil {
		if err == dynamo.ErrNotFound {
			gi.log.info(ctx, key.TableName(), ErrNoItemFound.Error())
			return false, nil
		}

		gi.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	return true, nil

}

// GetItemsWithContext queries multiple items by key (hash key) and returns it in the slice of items items
func (gi GlobalIndex) GetItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) (bool, error) {
	if err := isValidKey(key); err != nil {
		gi.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	err := gi.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).Index(gi.name).AllWithContext(ctx, items)
	if err != nil {
		if err == dynamo.ErrNotFound {
			gi.log.info(ctx, key.TableName(), ErrNoItemFound.Error())
			return false, nil
		}

		gi.log.error(ctx, key.TableName(), err.Error())
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
func (gi GlobalIndex) QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) error {

	if !IsPointerOFSlice(item) {
		return ErrInvalidPointerSliceType
	}
	if err := isValidKey(query); err != nil {
		gi.log.error(ctx, query.TableName(), err.Error())
		return err
	}

	q := gi.table(query.TableName()).Get(*query.HashKeyName(), query.HashKey()).Index(gi.name)

	// by range
	if query.RangeKeyName() != nil && query.RangeKey() != nil {
		q = q.Range(*query.RangeKeyName(), dynamo.Operator(query.RangeOp()), query.RangeKey())
	}

	err := q.AllWithContext(ctx, item)
	if err != nil {
		gi.log.error(ctx, query.TableName(), err.Error())
		return err
	}

	return nil
}

// Query by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// returns error in case of error
func (gi GlobalIndex) Query(query QueryInterface, item interface{}) error {
	return gi.QueryWithContext(context.TODO(), query, item)
}
