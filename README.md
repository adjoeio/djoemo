## Djoemo library
```
import "github.com/adjoeio/djoemo"
```
is a facade library for [guregu/dynamo](https://github.com/guregu/dynamo) and uses the [repository pattern](https://deviq.com/repository-pattern) to simplify dynamodb operations (save, update , delete and retrieve) on go structs
## Factories 

```
// NewRepository factory method for dynamo repository
NewRepository(dynamoClient dynamodbiface.DynamoDBAPI) RepositoryInterface

```

```
// Key factory method to create struct implement key interface
func Key() *key {
    return &key{}
}

// usage 

    key := dynamo.Key().
        WithTableName("user").
        WithHashKeyName("UserUUID").
        WithHashKey("123").
        WithRangeKeyName("CreatedAt").
        WithRangeKey(time.Now().Day())
        
```


## Interfaces

**RepositoryInterface:**
```
	// WithLog enables logging; it accepts LogInterface as logger
	WithLog(log LogInterface)

	// WithMetrics enables metrics; it accepts MetricsInterface as metrics publisher
	WithMetrics(metricsInterface MetricsInterface)

	// Get get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	Get(key KeyInterface, item interface{}) (bool, error)

	// Save item; it accepts a key interface, that is used to get the table name; item is the item to be saved
	// returns error in case of error
	Save(key KeyInterface, item interface{}) error

	// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update;
	// returns error in case of error
	Update(expression UpdateExpression, key KeyInterface, values map[string]interface{}) error

	// Delete item by key; returns error in case of error
	Delete(key KeyInterface) error

	// SaveItems batch save a slice of items by key; returns error in case of error
	SaveItems(key KeyInterface, items interface{}) error

	// DeleteItems deletes items matching the keys; returns error in case of error
	DeleteItems(key []KeyInterface) error

	// GetItems by key; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in items
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItems(key KeyInterface, items interface{}) (bool, error)

	// GetWithContext get item; it accepts a key interface that is used to get the table name, hash key and range key if it exists; the output will be given in item
	// returns true if item is found, returns false and nil if no item found, returns false and an error in case of error
	GetWithContext(key KeyInterface, item interface{}, ctx context.Context) (bool, error)

	// SaveWithContext it accepts a key interface, that is used to get the table name; item is the item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveWithContext(key KeyInterface, item interface{}, ctx context.Context) error

	// Update updates item by key; it accepts an expression (Set, SetSet, SetIfNotExists, SetExpr); key is the key to be updated;
	// values contains the values that should be used in the update; context which used to enable log with context
	// returns error in case of error
	UpdateWithContext(expression UpdateExpression, key KeyInterface, values map[string]interface{}, ctx context.Context) error

	// Delete item by its key; it accepts key of item to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteWithContext(key KeyInterface, ctx context.Context) error

	// SaveItems batch save a slice of items by key; it accepts key of item to be saved; item to be saved; context which used to enable log with context
	// returns error in case of error
	SaveItemsWithContext(key KeyInterface, items interface{}, ctx context.Context) error

	// DeleteItems deletes items matching the keys; it accepts array of keys to be deleted; context which used to enable log with context
	// returns error in case of error
	DeleteItemsWithContext(key []KeyInterface, ctx context.Context) error

	// GetItems by key; it accepts key of item to get it; context which used to enable log with context
	// returns true if items are found, returns false and nil if no items found, returns false and error in case of error
	GetItemsWithContext(key KeyInterface, out interface{}, ctx context.Context) (bool, error)

	// GIndex returns index repository
	GIndex(name string) GlobalIndexInterface
```

**GlobalIndexInterface:**
```
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
 
```

**KeyInterface:**
act as adapter between dynamo db table key and golang model 
```
    // TableName returns dynamo table name
    TableName() string
    
    // HashKeyName returns the name of hash key if it exists
    HashKeyName() *string
    
    // RangeKeyName returns the name of range key if it exists
    RangeKeyName() *string
    
    // HashKey returns the hash key value
    HashKey() interface{}
    
    // HashKey returns the range key value
    RangeKey() interface{}
 
```

**LogInterface:**
to support debug, it's necessary to provide a logger with this interface
```
    // WithFields adds fields from map string interface to logger
    WithFields(fields map[string]interface{}) LogInterface
        
    // Warn logs info
    Info(message string )
    
    // Warn logs warning
    Warn(message string )
    
    // Error logs error
    Error(message string )
```


**MetricsInterface:**
to support metrics, it's necessary to provide a metrics publisher with this interface
```
    // Publish publishes metrics
    Publish(key string, metricName string, metricValue float64) error
```


##Usage

**Get example:**

```
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

    // get item
    found, err := repository.Get(key, user)
    if err != nil {
        fmt.Println(err.Error())
    }

    if !found {
        fmt.Println("user not found")
    }

    // context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

    // get item with extra to allow trace fields in logger
    found, err = repository.GetWithContext(key, user, ctx)
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