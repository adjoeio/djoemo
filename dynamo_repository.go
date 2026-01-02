package djoemo

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/guregu/dynamo"
)

// Repository facade for github.com/guregu/djoemo
type Repository struct {
	dynamoClient *dynamo.DB
	log          LogInterface
	metrics      *Metrics
}

// NewRepository factory method for djoemo repository
func NewRepository(dynamoClient dynamodbiface.DynamoDBAPI) RepositoryInterface {
	return &Repository{
		dynamoClient: dynamo.NewFromIface(dynamoClient),
		log:          NewNopLog(),
		metrics:      &Metrics{},
	}
}

// WithLog enables logging; it accepts LogInterface as logger
func (repository *Repository) WithLog(log LogInterface) {
	repository.log = log
}

// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
func (repository *Repository) WithMetrics(metricsInterface MetricsInterface) {
	repository.metrics.Add(metricsInterface)
}

// WithPrometheusMetrics enables prometheus metrics
func (repository *Repository) WithPrometheusMetrics(registry *prometheus.Registry) RepositoryInterface {
	prommetrics := NewPrometheusMetrics(registry)
	repository.metrics.Add(prommetrics)
	return repository
}

// GetItemWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
func (repository Repository) GetItemWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error) {
	var err error
	defer repository.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidKey(key); err != nil {
		return false, err
	}

	err = buildTableKeyCondition(repository.table(key.TableName()), key).OneWithContext(ctx, item)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			repository.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(ErrNoItemFound.Error())
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// SaveItemWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
// returns error in case of error
func (repository Repository) SaveItemWithContext(ctx context.Context, key KeyInterface, item interface{}) error {
	var err error
	defer repository.recordMetrics(ctx, OpCommit, key, &err)()

	if err = isValidKey(key); err != nil {
		return err
	}

	err = repository.table(key.TableName()).Put(item).RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

// UpdateWithContext updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update; context which used to enable log with context
// returns error in case of error
func (repository Repository) UpdateWithContext(ctx context.Context, expression UpdateExpression, key KeyInterface, values map[string]interface{}) error {
	var err error
	defer repository.recordMetrics(ctx, OpUpdate, key, &err)()

	if err = isValidKey(key); err != nil {
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
				return err
			}
			update.SetExpr(expr, valueSlice...)
		}
	}

	err = update.RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (repository Repository) prepareUpdateWithUpdateExpressions(
	_ context.Context,
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
func (repository Repository) UpdateWithUpdateExpressions(
	ctx context.Context,
	key KeyInterface,
	updateExpressions UpdateExpressions,
) error {
	var err error
	defer repository.recordMetrics(ctx, OpUpdate, key, &err)()

	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		return err
	}

	err = update.RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

// UpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions defined at field level and returns
// the item, as it appears after the update, enabling you to set different update expressions for each field. The first
// key of the updateMap specifies the Update expression to use for the expressions in the map
func (repository Repository) UpdateWithUpdateExpressionsAndReturnValue(
	ctx context.Context,
	key KeyInterface,
	item interface{},
	updateExpressions UpdateExpressions,
) error {
	var err error
	defer repository.recordMetrics(ctx, OpUpdate, key, &err)()

	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		return err
	}

	err = update.ValueWithContext(ctx, item)
	if err != nil {
		return err
	}

	return nil
}

// ConditionalUpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions and a condition.
// If the condition is met, the item will be updated and returned as it appears after the update.
// The first key of the updateMap specifies the Update expression to use for the expressions in the map
func (repository Repository) ConditionalUpdateWithUpdateExpressionsAndReturnValue(
	ctx context.Context,
	key KeyInterface,
	item interface{},
	updateExpressions UpdateExpressions,
	conditionExpression string,
	conditionArgs ...interface{},
) (bool, error) {
	var err error
	defer repository.recordMetrics(ctx, OpUpdate, key, &err)()

	update, err := repository.prepareUpdateWithUpdateExpressions(ctx, key, updateExpressions)
	if err != nil {
		return false, err
	}

	update = update.If(conditionExpression, conditionArgs...)

	err = update.ValueWithContext(ctx, item)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// DeleteItemWithContext item by its key; it accepts key of item to be deleted; context which used to enable log with context
// returns error in case of error
func (repository Repository) DeleteItemWithContext(ctx context.Context, key KeyInterface) error {
	var err error
	defer repository.recordMetrics(ctx, OpDelete, key, &err)()

	if err = isValidKey(key); err != nil {
		return err
	}
	// by hash
	delete := repository.table(key.TableName()).Delete(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		delete = delete.Range(*key.RangeKeyName(), key.RangeKey())
	}

	err = delete.RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

// SaveItemsWithContext batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
// returns error in case of error
func (repository Repository) SaveItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) error {
	var err error
	defer repository.recordMetrics(ctx, OpCommit, key, &err)()

	if err = isValidKey(key); err != nil {
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
		return err
	}

	_, err = batch.Write().Put(itemSlice...).RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

// DeleteItemsWithContext deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
// returns error in case of error
func (repository Repository) DeleteItemsWithContext(ctx context.Context, keys []KeyInterface) error {
	var err error
	defer repository.recordMultipleMetrics(ctx, OpDelete, keys, &err)()

	if len(keys) == 0 {
		return nil
	}
	for i := 0; i < len(keys); i++ {
		if err = isValidKey(keys[i]); err != nil {
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

	_, err = batch.Write().Delete(dynamoKeys...).RunWithContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

// GetItemsWithContext by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context, the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
func (repository Repository) GetItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) (bool, error) {
	var err error
	defer repository.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidKey(key); err != nil {
		return false, err
	}

	err = repository.table(key.TableName()).Get(*key.HashKeyName(), key.HashKey()).AllWithContext(ctx, items)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			repository.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(ErrNoItemFound.Error())
			return false, nil
		}

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
func (repository Repository) QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) (err error) {
	defer repository.recordMetrics(ctx, OpRead, query, &err)()

	if !IsPointerOFSlice(item) {
		return ErrInvalidPointerSliceType
	}
	if err = isValidKey(query); err != nil {
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

	err = q.AllWithContext(ctx, item)
	if err != nil {
		return err
	}

	return nil
}

// OptimisticLockSaveWithContext saves an item if the version attribute on the server matches the version of the object
func (repository Repository) OptimisticLockSaveWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error) {
	var err error
	defer repository.recordMetrics(ctx, OpCommit, key, &err)()

	model, isDjoemoModel := item.(ModelInterface)
	if !isDjoemoModel {
		return false, errors.New("Items to use with OptimisticLock must implement the ModelInterface")
	}

	currentVersion := model.GetVersion()
	model.IncreaseVersion()
	model.InitCreatedAt()
	model.InitUpdatedAt()

	update := repository.table(key.TableName()).Put(item).If("attribute_not_exists(Version) OR Version = ?", currentVersion)

	err = update.Run()
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok && awserr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}

		return false, err
	}
	return true, nil
}

// ConditionalUpdateWithContext updates an item when the condition is met, otherwise the update will be rejected
func (repository Repository) ConditionalUpdateWithContext(ctx context.Context, key KeyInterface, item interface{}, expression string, expressionArgs ...interface{}) (bool, error) {
	var err error
	defer repository.recordMetrics(ctx, OpUpdate, key, &err)()

	update := repository.table(key.TableName()).Put(item).If(expression, expressionArgs...)

	err = update.Run()
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok && awserr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			repository.log.WithContext(ctx).WithField(TableName, key.TableName()).Info(dynamodb.ErrCodeConditionalCheckFailedException)
			return false, nil
		}

		return false, err
	}
	return true, nil
}

// GIndex creates an index repository by name
func (repository *Repository) GIndex(name string) GlobalIndexInterface {
	return &GlobalIndex{
		name:         name,
		log:          repository.log,
		dynamoClient: repository.dynamoClient,
		metrics:      repository.metrics,
	}
}

func (repository *Repository) table(tableName string) dynamo.Table {
	return repository.dynamoClient.Table(tableName)
}

// ScanIteratorWithContext returns an instance of an Iterator that provides methods for scanning tables
func (repository *Repository) ScanIteratorWithContext(ctx context.Context, key KeyInterface, searchLimit int64) (IteratorInterface, error) {
	var err error
	defer repository.recordMetrics(ctx, OpRead, key, &err)()

	if err = isValidTableName(key); err != nil {
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
func (repository Repository) BatchGetItemsWithContext(ctx context.Context, keys []KeyInterface, out interface{}) (bool, error) {
	var err error
	defer repository.recordMultipleMetrics(ctx, OpRead, keys, &err)()

	if len(keys) == 0 {
		return false, nil
	}

	// Validate keys and ensure they all point to the same table
	tableName := keys[0].TableName()
	for i := 0; i < len(keys); i++ {
		if err = isValidKey(keys[i]); err != nil {
			return false, err
		}
		if keys[i].TableName() != tableName {
			return false, ErrInvalidBatchRequest
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
	err = batch.Get(dKeys...).AllWithContext(ctx, out)
	if err != nil {
		if errors.Is(err, dynamo.ErrNotFound) {
			repository.log.WithContext(ctx).WithField(TableName, tableName).Info(ErrNoItemFound.Error())
			return false, nil
		}
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

func (repository Repository) recordMetrics(ctx context.Context, op string, key KeyInterface, err *error) func() {
	start := time.Now()
	return func() {
		repository.metrics.Record(ctx, op, key, time.Since(start), isOpSuccess(err))
	}
}

func (repository Repository) recordMultipleMetrics(ctx context.Context, op string, keys []KeyInterface, err *error) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		for _, key := range keys {
			repository.metrics.Record(ctx, op, key, duration, isOpSuccess(err))
		}
	}
}

func isOpSuccess(err *error) bool {
	if err == nil || *err == nil {
		return true
	}
	return errors.Is(*err, dynamo.ErrNotFound)
}
