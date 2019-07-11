package djoemo

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/guregu/dynamo"
)

// Repository facade for github.com/guregu/djoemo
type Repository struct {
	dynamoClient *dynamo.DB
	log          logger
	metrics      metrics
}

// NewRepository factory method for djoemo repository
func NewRepository(dynamoClient dynamodbiface.DynamoDBAPI) RepositoryInterface {
	return &Repository{
		dynamoClient: dynamo.NewFromIface(dynamoClient),
		log:          logger{log: nopLog{}},
	}
}

// WithLog enables logging; it accepts LogInterface as logger
func (repository *Repository) WithLog(log LogInterface) {
	repository.log = logger{log: log}
}

// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
func (repository *Repository) WithMetrics(metricsInterface MetricsInterface) {
	repository.metrics = metrics{metrics: metricsInterface}
}

// GetItemWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (repository *Repository) GetItemWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error) {
	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	// by hash
	query := repository.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		query = query.Range(*key.RangeKeyName(), dynamo.Equal, key.RangeKey())
	}

	err := query.OneWithContext(ctx, item)
	if err != nil {
		if err == dynamo.ErrNotFound {
			repository.log.info(ctx, key.TableName(), ErrNoItemFound.Error())
			return false, nil
		}

		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	return true, nil
}

// SaveItemWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
// returns error in case of error
func (repository *Repository) SaveItemWithContext(ctx context.Context, key KeyInterface, item interface{}) error {
	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err := repository.table(key.TableName()).Put(item).RunWithContext(ctx)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = repository.metrics.Publish(ctx, key.TableName(), MetricNameSavedItemsCount, float64(1))
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
	}

	return nil
}

// UpdateWithContext updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update; context which used to enable log with context
// returns error in case of error
func (repository *Repository) UpdateWithContext(ctx context.Context, expression UpdateExpression, key KeyInterface, values map[string]interface{}) error {
	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	// by hash
	update := repository.table(key.TableName()).Update(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		update = update.Range(*key.RangeKeyName(), key.RangeKey())
	}

	for expr, value := range values {
		if expression == Set {
			update.Set(expr, value)
		}
		if expression == SetSet {
			update.SetSet(expr, value)
		}
		if expression == SetIfNotExists {
			update.SetIfNotExists(expr, value)
		}
		if expression == SetExpr {
			valueSlice, err := InterfaceToArrayOfInterface(value)
			if err != nil {
				repository.log.error(ctx, key.TableName(), err.Error())
				return err
			}
			update.SetExpr(expr, valueSlice...)
		}
	}

	err := update.RunWithContext(ctx)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = repository.metrics.Publish(ctx, key.TableName(), MetricNameUpdatedItemsCount, float64(1))
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
	}

	return nil
}

// DeleteItemWithContext item by its key; it accepts key of item to be deleted; context which used to enable log with context
// returns error in case of error
func (repository *Repository) DeleteItemWithContext(ctx context.Context, key KeyInterface) error {

	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}
	// by hash
	delete := repository.table(key.TableName()).Delete(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		delete = delete.Range(*key.RangeKeyName(), key.RangeKey())
	}

	err := delete.RunWithContext(ctx)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = repository.metrics.Publish(ctx, key.TableName(), MetricNameDeleteItemsCount, float64(1))
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
	}

	return nil
}

// SaveItemsWithContext batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
// returns error in case of error
func (repository *Repository) SaveItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) error {

	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	// by hash
	batch := repository.table(key.TableName()).Batch(*key.HashKeyName())
	// by hash & range
	if key.RangeKeyName() != nil {
		batch = repository.table(key.TableName()).Batch(*key.HashKeyName(), *key.RangeKeyName())
	}

	itemSlice, err := InterfaceToArrayOfInterface(items)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	count, err := batch.Write().Put(itemSlice...).RunWithContext(ctx)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = repository.metrics.Publish(ctx, key.TableName(), MetricNameSavedItemsCount, float64(count))
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
	}

	return nil
}

// DeleteItemsWithContext deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
// returns error in case of error
func (repository *Repository) DeleteItemsWithContext(ctx context.Context, keys []KeyInterface) error {
	if len(keys) == 0 {
		return nil
	}
	for i := 0; i < len(keys); i++ {
		if err := isValidKey(keys[i]); err != nil {
			repository.log.error(ctx, keys[i].TableName(), err.Error())
			return err
		}
	}

	// by hash
	batch := repository.table(keys[0].TableName()).Batch(*keys[0].HashKeyName())
	// by hash & range
	if keys[0].RangeKeyName() != nil {
		batch = repository.table(keys[0].TableName()).Batch(*keys[0].HashKeyName(), *keys[0].RangeKeyName())
	}

	dynamoKeys := make([]dynamo.Keyed, len(keys))
	for i := 0; i < len(keys); i++ {
		dynamoKeys[i] = dynamo.Keyed(keys[i])
	}

	count, err := batch.Write().Delete(dynamoKeys...).RunWithContext(ctx)
	if err != nil {
		repository.log.error(ctx, keys[0].TableName(), err.Error())
		return err
	}

	err = repository.metrics.Publish(ctx, keys[0].TableName(), MetricNameDeleteItemsCount, float64(count))
	if err != nil {
		repository.log.error(ctx, keys[0].TableName(), err.Error())
	}

	return nil
}

// GetItemsWithContext by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context, the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (repository *Repository) GetItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) (bool, error) {
	if err := isValidKey(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	err := repository.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).AllWithContext(ctx, items)
	if err != nil {
		if err == dynamo.ErrNotFound {
			repository.log.info(ctx, key.TableName(), ErrNoItemFound.Error())
			return false, nil
		}

		repository.log.error(ctx, key.TableName(), err.Error())
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

// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// context which used to enable log with context, the output will be given in items
// returns error in case of error
func (repository *Repository) QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) error {

	if !IsPointerOFSlice(item) {
		return ErrInvalidPointerSliceType
	}
	if err := isValidKey(query); err != nil {
		repository.log.error(ctx, query.TableName(), err.Error())
		return err
	}

	q := repository.table(query.TableName()).Get(*query.HashKeyName(), query.HashKey())

	// by range
	if query.RangeKeyName() != nil && query.RangeKey() != nil {
		q = q.Range(*query.RangeKeyName(), dynamo.Operator(query.RangeOp()), query.RangeKey())
	}

	err := q.AllWithContext(ctx, item)
	if err != nil {
		repository.log.error(ctx, query.TableName(), err.Error())
		return err
	}

	return nil
}

// GetItem get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (repository Repository) GetItem(key KeyInterface, item interface{}) (bool, error) {
	return repository.GetItemWithContext(context.TODO(), key, item)
}

// SaveItem item; it accepts a key interface, that is used to get the table name; item is the item to be saved
// returns error in case of error
func (repository Repository) SaveItem(key KeyInterface, item interface{}) error {
	return repository.SaveItemWithContext(context.TODO(), key, item)
}

// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update;
// returns error in case of error
func (repository Repository) Update(expression UpdateExpression, key KeyInterface, values map[string]interface{}) error {
	return repository.UpdateWithContext(context.TODO(), expression, key, values)
}

// DeleteItem item by key; returns error in case of error
func (repository Repository) DeleteItem(key KeyInterface) error {
	return repository.DeleteItemWithContext(context.TODO(), key)
}

// SaveItems batch save a slice of items by key
func (repository Repository) SaveItems(key KeyInterface, items interface{}) error {
	return repository.SaveItemsWithContext(context.TODO(), key, items)
}

// DeleteItems deletes items matching the keys
func (repository Repository) DeleteItems(keys []KeyInterface) error {
	return repository.DeleteItemsWithContext(context.TODO(), keys)
}

// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (repository Repository) GetItems(key KeyInterface, items interface{}) (bool, error) {
	return repository.GetItemsWithContext(context.TODO(), key, items)
}

// Query by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// returns error in case of error
func (repository Repository) Query(query QueryInterface, item interface{}) error {
	return repository.QueryWithContext(context.TODO(), query, item)
}

// GIndex creates an index repository by name
func (repository Repository) GIndex(name string) GlobalIndexInterface {
	return GlobalIndex{
		name:         name,
		log:          repository.log,
		dynamoClient: repository.dynamoClient,
	}
}

func (repository Repository) table(tableName string) dynamo.Table {
	return repository.dynamoClient.Table(tableName)
}
