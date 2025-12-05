package djoemo

import (
	"context"
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"

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

	err := buildTableKeyCondition(repository.table(key.TableName()), key).OneWithContext(ctx, item)
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
		if expression == Add {
			update.Add(expr, value)
		}
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

func (repository *Repository) prepareUpdateWithUpdateExpressions(
	ctx context.Context,
	key KeyInterface,
	updateExpressions UpdateExpressions,
) (*dynamo.Update, error) {
	if err := isValidKey(key); err != nil {
		return nil, err
	}

	// by hash
	update := repository.table(key.TableName()).Update(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		update = update.Range(*key.RangeKeyName(), key.RangeKey())
	}

	for updateExpression, v := range updateExpressions {
		expression := UpdateExpression(updateExpression)

		for expr, value := range v {
			if expression == Add {
				update.Add(expr, value)
			}
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
					return nil, err
				}
				update.SetExpr(expr, valueSlice...)
			}
		}
	}

	return update, nil
}

// UpdateWithUpdateExpressions updates an item with update expressions defined at field level, enabling you to set
// different update expressions for each field. The first key of the updateMap specifies the Update expression to use
// for the expressions in the map
func (repository *Repository) UpdateWithUpdateExpressions(
	ctx context.Context,
	key KeyInterface,
	updateExpressions UpdateExpressions,
) error {
	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = update.RunWithContext(ctx)
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

// UpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions defined at field level and returns
// the item, as it appears after the update, enabling you to set different update expressions for each field. The first
// key of the updateMap specifies the Update expression to use for the expressions in the map
func (repository *Repository) UpdateWithUpdateExpressionsAndReturnValue(
	ctx context.Context,
	key KeyInterface,
	item interface{},
	updateExpressions UpdateExpressions,
) error {
	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return err
	}

	err = update.ValueWithContext(ctx, item)
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

// ConditionalUpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions and a condition.
// If the condition is met, the item will be updated and returned as it appears after the update.
// The first key of the updateMap specifies the Update expression to use for the expressions in the map
func (repository *Repository) ConditionalUpdateWithUpdateExpressionsAndReturnValue(
	ctx context.Context,
	key KeyInterface,
	item interface{},
	updateExpressions UpdateExpressions,
	conditionExpression string,
	conditionArgs ...interface{},
) (conditionMet bool, err error) {
	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	update = update.If(conditionExpression, conditionArgs...)

	err = update.ValueWithContext(ctx, item)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.info(ctx, key.TableName(), dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}

	err = repository.metrics.Publish(ctx, key.TableName(), MetricNameUpdatedItemsCount, float64(1))
	if err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
	}

	return true, nil
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

	if limit := valueFromPtr(query.Limit()); limit > 0 {
		q = q.Limit(limit)
	}

	if query.Descending() {
		q = q.Order(dynamo.Descending)
	}

	err := q.AllWithContext(ctx, item)
	if err != nil {
		repository.log.error(ctx, query.TableName(), err.Error())
		return err
	}

	return nil
}

// OptimisticLockSaveWithContext saves an item if the version attribute on the server matches the version of the object
func (repository Repository) OptimisticLockSaveWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error) {
	model, isDjoemoModel := item.(ModelInterface)
	if !isDjoemoModel {
		return false, errors.New("Items to use with OptimisticLock must implement the ModelInterface")
	}

	currentVersion := model.GetVersion()
	model.IncreaseVersion()
	model.InitCreatedAt()
	model.InitUpdatedAt()

	update := repository.table(key.TableName()).Put(item).If("attribute_not_exists(Version) OR Version = ?", currentVersion)

	err := update.Run()
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok && awserr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.info(ctx, key.TableName(), dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}
	return true, nil
}

// ConditionalUpdateWithContext updates an item when the condition is met, otherwise the update will be rejected
func (repository Repository) ConditionalUpdateWithContext(ctx context.Context, key KeyInterface, item interface{}, expression string, expressionArgs ...interface{}) (bool, error) {
	update := repository.table(key.TableName()).Put(item).If(expression, expressionArgs...)

	err := update.Run()
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok && awserr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.info(ctx, key.TableName(), dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}
		repository.log.error(ctx, key.TableName(), err.Error())
		return false, err
	}
	return true, nil
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

// OptimisticLockSave updates an item if the version attribute on the server matches the one of the object
func (repository Repository) OptimisticLockSave(key KeyInterface, item interface{}) (bool, error) {
	return repository.OptimisticLockSaveWithContext(context.TODO(), key, item)
}

// ConditionalUpdate updates an item when the condition is met, otherwise the update will be rejected
func (repository Repository) ConditionalUpdate(key KeyInterface, item interface{}, expression string, expressionArgs ...interface{}) (bool, error) {
	return repository.ConditionalUpdateWithContext(context.TODO(), key, item, expression, expressionArgs)
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

// ScanIteratorWithContext returns an instance of an Iterator that provides methods for scanning tables
func (repository Repository) ScanIteratorWithContext(ctx context.Context, key KeyInterface, searchLimit int64) (IteratorInterface, error) {
	if err := isValidTableName(key); err != nil {
		repository.log.error(ctx, key.TableName(), err.Error())
		return nil, err
	}

	scan := repository.table(key.TableName()).Scan()
	pagingIterator := scan.Iter()

	itr := &Iterator{
		scan:        scan,
		tableName:   key.TableName(),
		searchLimit: searchLimit,
		iterator:    pagingIterator,
		ctx:         ctx,
	}
	itr.scan.SearchLimit(searchLimit)

	return itr, nil
}

// BatchGetItemsWithContext gets multiple items by their keys; all keys must refer to the same table.
// out must be a pointer to a slice of your model type.
// Returns (true, nil) if at least one item is found, (false, nil) if none found, or (false, err) on error.
func (repository *Repository) BatchGetItemsWithContext(ctx context.Context, keys []KeyInterface, out interface{}) (bool, error) {
	if len(keys) == 0 {
		return false, nil
	}

	// Validate keys and ensure they all point to the same table
	tableName := keys[0].TableName()
	for i := 0; i < len(keys); i++ {
		if err := isValidKey(keys[i]); err != nil {
			repository.log.error(ctx, keys[i].TableName(), err.Error())
			return false, err
		}
		if keys[i].TableName() != tableName {
			err := errors.New("BatchGetItemsWithContext: all keys must belong to the same table")
			repository.log.error(ctx, tableName, err.Error())
			return false, err
		}
	}

	// by hash
	batch := repository.table(tableName).Batch(*keys[0].HashKeyName())
	// by hash & range
	if keys[0].RangeKeyName() != nil && keys[0].RangeKey() != nil {
		batch = repository.table(tableName).Batch(*keys[0].HashKeyName(), *keys[0].RangeKeyName())
	}

	// Build dynamo keys
	dKeys := make([]dynamo.Keyed, len(keys))
	for i := 0; i < len(keys); i++ {
		dKeys[i] = dynamo.Keyed(keys[i])
	}

	// Execute batch get
	err := batch.Get(dKeys...).AllWithContext(ctx, out)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			repository.log.info(ctx, tableName, ErrNoItemFound.Error())
			return false, nil
		}

		repository.log.error(ctx, tableName, err.Error())
		return false, err
	}

	// Check if slice is empty
	val := reflect.ValueOf(out)
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
