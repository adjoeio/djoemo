## Djoemo library
```
import "github.com/adjoeio/djoemo"
```
is a facade library for [guregu/dynamo](https://github.com/guregu/dynamo) and uses the [repository pattern](https://deviq.com/repository-pattern) to simplify dynamodb operations (save, update , delete and retrieve) on go structs

## Factories 

```go
// NewRepository factory method for dynamo repository
NewRepository(dynamoClient dynamodbiface.DynamoDBAPI) RepositoryInterface
```

```go
// Key factory method to create struct implement key interface
func Key() *key {
    return &key{}
}

// usage 
key := djoemo.Key().
    WithTableName("user").
    WithHashKeyName("UserUUID").
    WithHashKey("123").
    WithRangeKeyName("CreatedAt").
    WithRangeKey(time.Now().Day())
```

## Interfaces

**RepositoryInterface:**
```go
// WithLog enables logging; it accepts LogInterface as logger
WithLog(log LogInterface)

// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
WithMetrics(metricsInterface MetricsInterface)

// WithPrometheusMetrics enables prometheus metrics
WithPrometheusMetrics(registry *prometheus.Registry)

// GetItemWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
GetItemWithContext(ctx context.Context, key KeyInterface, item any) (bool, error)

// SaveItemWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
// returns error in case of error
SaveItemWithContext(ctx context.Context, key KeyInterface, item any) error

// UpdateWithContext updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
// values contains the values that should be used in the update; context which used to enable log with context
// returns error in case of error
UpdateWithContext(ctx context.Context, expression UpdateExpression, key KeyInterface, values map[string]any) error

// UpdateWithUpdateExpressions updates an item with update expressions defined at field level, enabling you to set
// different update expressions for each field. The first key of the updateMap specifies the Update expression to use
// for the expressions in the map
UpdateWithUpdateExpressions(ctx context.Context, key KeyInterface, updateExpressions UpdateExpressions) error

// UpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions defined at field level and returns
// the item, as it appears after the update, enabling you to set different update expressions for each field. The first
// key of the updateMap specifies the Update expression to use for the expressions in the map
UpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key KeyInterface, item any, updateExpressions UpdateExpressions) error

// ConditionalUpdateWithUpdateExpressionsAndReturnValue updates an item with update expressions and a condition.
// If the condition is met, the item will be updated and returned as it appears after the update.
// The first key of the updateMap specifies the Update expression to use for the expressions in the map
ConditionalUpdateWithUpdateExpressionsAndReturnValue(ctx context.Context, key KeyInterface, item any, updateExpressions UpdateExpressions, conditionExpression string, conditionArgs ...any) (conditionMet bool, err error)

// DeleteItemWithContext item by its key; it accepts key of item to be deleted; context which used to enable log with context
// returns error in case of error
DeleteItemWithContext(ctx context.Context, key KeyInterface) error

// SaveItemsWithContext batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
// returns error in case of error
SaveItemsWithContext(ctx context.Context, key KeyInterface, items any) error

// DeleteItemsWithContext deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
// returns error in case of error
DeleteItemsWithContext(ctx context.Context, key []KeyInterface) error

// GetItemsWithContext by key; it accepts key of item to get it; context which used to enable log with context
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
GetItemsWithContext(ctx context.Context, key KeyInterface, out any) (bool, error)

// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// context which used to enable log with context, the output will be given in items
// returns error in case of error
QueryWithContext(ctx context.Context, query QueryInterface, item any) error

// GIndex returns index repository
GIndex(name string) GlobalIndexInterface

// OptimisticLockSaveWithContext saves an item if the version attribute on the server matches the version of the object
OptimisticLockSaveWithContext(ctx context.Context, key KeyInterface, item any) (bool, error)

// ScanIteratorWithContext returns an instance of an iterator that provides methods to use for scanning tables
ScanIteratorWithContext(ctx context.Context, key KeyInterface, searchLimit int64) (IteratorInterface, error)

// ConditionalUpdateWithContext updates an item if the passed expression and condition evaluates to true
ConditionalUpdateWithContext(ctx context.Context, key KeyInterface, item any, expression string, expressionArgs ...any) (bool, error)

// BatchGetItemsWithContext gets multiple items by their keys; it accepts a slice of keys (all from the same table)
// and fills out (pointer to a slice) with any found items.
// returns true if at least one item is found, returns false and nil if no items found, returns false and error in case of error
BatchGetItemsWithContext(ctx context.Context, keys []KeyInterface, out any) (bool, error)
```

**GlobalIndexInterface:**
```go
// GetItemWithContext get item from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context; the output will be given in item
// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
GetItemWithContext(ctx context.Context, key KeyInterface, item interface{}) (bool, error)

// GetItemsWithContext by key from index; it accepts a key interface that is used to get the table name, hash key and range key if it exists;
// context which used to enable log with context, the output will be given in items
// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
GetItemsWithContext(ctx context.Context, key KeyInterface, items interface{}) (bool, error)

// GetItemsWithRangeWithContext same as GetItemsWithContext, but also respects range key
GetItemsWithRangeWithContext(ctx context.Context, key KeyInterface, items interface{}) (bool, error)

// QueryWithContext by query; it accepts a query interface that is used to get the table name, hash key and range key with its operator if it exists;
// context which used to enable log with context, the output will be given in items
// returns error in case of error
QueryWithContext(ctx context.Context, query QueryInterface, item interface{}) error
```

**KeyInterface:**
Acts as adapter between dynamo db table key and golang model.
```go
// TableName returns dynamo table name
TableName() string

// HashKeyName returns the name of hash key if it exists
HashKeyName() *string

// RangeKeyName returns the name of range key if it exists
RangeKeyName() *string

// HashKey returns the hash key value
HashKey() interface{}

// RangeKey returns the range key value
RangeKey() interface{}
```

**LogInterface:**
To support debug, it's necessary to provide a logger with this interface.
```go
// WithFields adds fields from map string interface to logger
WithFields(fields map[string]interface{}) LogInterface

// WithField adds a single field to logger
WithField(field string, value any) LogInterface

// WithContext adds context to logger
WithContext(ctx context.Context) LogInterface

// Info logs info
Info(message string )

// Warn logs warning
Warn(message string )

// Error logs error
Error(message string )
```

**MetricsInterface:**
To support metrics, it's necessary to provide a metrics publisher with this interface.
```go
// Record publishes metrics for an operation
Record(ctx context.Context, caller string, key KeyInterface, duration time.Duration, err *error)
```

## Usage

**Get example:**

```go
// enable log by passing logger interface
repository.WithLog(logInterface)

// enable metrics by passing metrics interface
repository.WithMetrics(metricsInterface)

user := &User{}
// use factory to create dynamo key interface
key := djoemo.Key().
    WithTableName("user").
    WithHashKeyName("UserUUID").
    WithHashKey("123")

// optional:
// create context with source label for metrics (increase observability)
ctx = WithSourceLabel(ctx, "FooBarAPI")

// get item
found, err := repository.GetItemWithContext(
    ctx,
	key,
	user)
if err != nil {
    fmt.Println(err.Error())
}

if !found {
    fmt.Println("user not found")
}
```

**notes**  
* The operation will not fail, if publish of metrics returns an error. If the logger is enabled, it will just log the error.

For more examples see examples/example.go .