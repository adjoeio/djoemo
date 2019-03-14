package djoemo

import (
	"context"
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

// GetWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (repository *Repository) GetWithContext(key KeyInterface, item interface{}, ctx context.Context) (bool, error) {
	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return false, err
	}

	// by hash
	query := repository.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		query = query.Range(*key.RangeKeyName(), dynamo.Equal, key.RangeKey())
	}

	err := query.One(item)
	if err != nil {
		if err == dynamo.ErrNotFound {
			repository.log.Infof(key.TableName(), ErrNoItemFound.Error(), ctx)
			return false, nil
		}

		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return false, err
	}

	return true, nil
}

// SaveWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
// returns error in case of error
func (repository *Repository) SaveWithContext(key KeyInterface, item interface{}, ctx context.Context) error {
	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	err := repository.table(key.TableName()).Put(item).Run()
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	err = repository.metrics.Publish(key.TableName(), MetricNameSavedItemsCount, float64(1))
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
	}

	return nil
}

// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update; context which used to enable log with context
// returns error in case of error
func (repository *Repository) UpdateWithContext(expression UpdateExpression, key KeyInterface, values map[string]interface{}, ctx context.Context) error {
	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
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
				repository.log.Errorf(key.TableName(), err.Error(), ctx)
				return err
			}
			update.SetExpr(expr, valueSlice...)
		}
	}

	err := update.Run()
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	err = repository.metrics.Publish(key.TableName(), MetricNameUpdatedItemsCount, float64(1))
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
	}

	return nil
}

// Delete item by its key; it accepts key of item to be deleted; context which used to enable log with context
// returns error in case of error
func (repository *Repository) DeleteWithContext(key KeyInterface, ctx context.Context) error {

	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}
	// by hash
	delete := repository.table(key.TableName()).Delete(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		delete = delete.Range(*key.RangeKeyName(), key.RangeKey())
	}

	err := delete.Run()
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	err = repository.metrics.Publish(key.TableName(), MetricNameDeleteItemsCount, float64(1))
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
	}

	return nil
}

// SaveItems batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
// returns error in case of error
func (repository *Repository) SaveItemsWithContext(key KeyInterface, items interface{}, ctx context.Context) error {

	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
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
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	count, err := batch.Write().Put(itemSlice...).Run()
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return err
	}

	err = repository.metrics.Publish(key.TableName(), MetricNameSavedItemsCount, float64(count))
	if err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
	}

	return nil
}

// DeleteItems deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
// returns error in case of error
func (repository *Repository) DeleteItemsWithContext(keys []KeyInterface, ctx context.Context) error {
	if len(keys) == 0 {
		return nil
	}
	for i := 0; i < len(keys); i++ {
		if err := isValidKey(keys[i]); err != nil {
			repository.log.Errorf(keys[i].TableName(), err.Error(), ctx)
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

	count, err := batch.Write().Delete(dynamoKeys...).Run()
	if err != nil {
		repository.log.Errorf(keys[0].TableName(), err.Error(), ctx)
		return err
	}

	err = repository.metrics.Publish(keys[0].TableName(), MetricNameDeleteItemsCount, float64(count))
	if err != nil {
		repository.log.Errorf(keys[0].TableName(), err.Error(), nil)
	}

	return nil
}

// GetItemsWithContext by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context, the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (repository *Repository) GetItemsWithContext(key KeyInterface, items interface{}, ctx context.Context) (bool, error) {
	if err := isValidKey(key); err != nil {
		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return false, err
	}

	err := repository.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).All(items)
	if err != nil {
		if err == dynamo.ErrNotFound {
			repository.log.Infof(key.TableName(), ErrNoItemFound.Error(), ctx)
			return false, nil
		}

		repository.log.Errorf(key.TableName(), err.Error(), ctx)
		return false, err
	}

	return true, nil
}

// Get get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (repository Repository) Get(key KeyInterface, item interface{}) (bool, error) {
	return repository.GetWithContext(key, item, nil)
}

// Save item; it accepts a key interface, that is used to get the table name; item is the item to be saved
// returns error in case of error
func (repository Repository) Save(key KeyInterface, item interface{}) error {
	return repository.SaveWithContext(key, item, nil)
}

// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update;
// returns error in case of error
func (repository Repository) Update(expression UpdateExpression, key KeyInterface, values map[string]interface{}) error {
	return repository.UpdateWithContext(expression, key, values, nil)
}

// Delete item by key; returns error in case of error
func (repository Repository) Delete(key KeyInterface) error {
	return repository.DeleteWithContext(key, nil)
}

// SaveItems batch save a slice of items by key
func (repository Repository) SaveItems(key KeyInterface, items interface{}) error {
	return repository.SaveItemsWithContext(key, items, nil)
}

// DeleteItems deletes items matching the keys
func (repository Repository) DeleteItems(keys []KeyInterface) error {
	return repository.DeleteItemsWithContext(keys, nil)
}

// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (repository Repository) GetItems(key KeyInterface, items interface{}) (bool, error) {
	return repository.GetItemsWithContext(key, items, nil)
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
