package examples

import (
	"context"
	"fmt"
	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	"time"
)

var (
	mockCtrl         = gomock.NewController(GinkgoT())
	dAPIMock         = mock.NewMockDynamoDBAPI(mockCtrl)
	logInterface     = mock.NewMockLogInterface(mockCtrl)
	metricsInterface = mock.NewMockMetricsInterface(mockCtrl)
	// init the repo with click
	repository = djoemo.NewRepository(dAPIMock)
)

type User struct {
	UserUUID  string
	Time      time.Time
	Msg       string              `dynamo:"Message"`
	Count     int                 `dynamo:",omitempty"`
	Friends   []string            `dynamo:",set"` // Sets
	Set       map[string]struct{} `dynamo:",set"` // Map sets, too!
	SecretKey string              `dynamo:"-"`    // Ignored
}

// GetItem shows an example how to get an item from dynamodb
func Get() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	user := &User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	found, err := repository.GetItem(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	type TraceInfo string
	ctx = context.WithValue(ctx, TraceInfo("TraceInfo"), map[string]interface{}{"TraceID": "trace-id"})

	// get item with context to allow trace fields in logger
	found, err = repository.GetItemWithContext(ctx, key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}
}

// GetItems shows an example, how to get multiple items from dynamodb
func GetItems() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	users := &[]User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	found, err := repository.GetItems(key, users)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("users not found")
	}

	// get item with context to allow trace fields in logger
	found, err = repository.GetItem(key, users)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("users not found")
	}
}

// Query shows an example, how to query multiple items from dynamodb
func Query() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	users := &[]User{}
	// use factory to create djoemo key interface
	q := djoemo.Query().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123").
		WithRangeKeyName("Email").
		WithRangeKey("user@").
		WithRangeOp(djoemo.BeginsWith)

	// query items
	err := repository.Query(q, users)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// SaveItem shows an example, how to save an item
func Save() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	user := &User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	err := repository.SaveItem(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	// SaveItem item with context to allow trace fields in logger
	err = repository.SaveItem(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// SaveItems shows an example, how to save multiple items
func SaveItems() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	user := &User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123").
		WithRangeKeyName("CreatedAt").
		WithRangeKey(time.Now().Day())

	// get item
	err := repository.SaveItems(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	// SaveItems item with context to allow trace fields in logger
	err = repository.SaveItems(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// Update shows an example, how to update certain fields of an item
func Update() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	updates := map[string]interface{}{
		"Message": "msg1",
	}

	err := repository.Update(djoemo.Set, key, updates)

	if err != nil {
		fmt.Println(err.Error())
	}

	// Update item with context to allow trace fields in logger
	err = repository.Update(djoemo.Set, key, updates)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// DeleteItem shows an example, how to delete item by key
func Delete() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	err := repository.DeleteItem(key)
	if err != nil {
		fmt.Println(err.Error())
	}

	// DeleteItem item with context to allow trace fields in logger
	err = repository.DeleteItem(key)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// DeleteItems shows an example, how to delete multiple items by keys
func DeleteItems() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("UserUUID").
		WithHashKey("123")

	// get item
	err := repository.DeleteItems([]djoemo.KeyInterface{key})
	if err != nil {
		fmt.Println(err.Error())
	}

	// DeleteItems with context to allow trace fields in logger
	err = repository.DeleteItems([]djoemo.KeyInterface{key})
	if err != nil {
		fmt.Println(err.Error())
	}
}

// GetFromGlobalIndex shows an example, how to get item from global index
func GetFromGlobalIndex() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	user := &User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("DeviceID").
		WithHashKey("123")

	// get item
	found, err := repository.GIndex("UserIndex").GetItem(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	type TraceInfo string
	ctx = context.WithValue(ctx, TraceInfo("TraceInfo"), map[string]interface{}{"TraceID": "trace-id"})

	// GIndex item with context to allow trace fields in logger
	found, err = repository.GIndex("UserIndex").GetItemWithContext(ctx, key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}
}

// GetItemsFromGlobalIndex shows an example, how to get multiple items from global index
func GetItemsFromGlobalIndex() {
	// enable log by passing logger interface
	repository.WithLog(logInterface)

	// enable metrics by passing metrics interface
	repository.WithMetrics(metricsInterface)

	user := &User{}
	// use factory to create djoemo key interface
	key := djoemo.Key().
		WithTableName("user").
		WithHashKeyName("DeviceID").
		WithHashKey("123")

	// get item
	found, err := repository.GIndex("UserIndex").GetItem(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	type TraceInfo string
	ctx = context.WithValue(ctx, TraceInfo("TraceInfo"), map[string]interface{}{"TraceID": "trace-id"})

	// GetItems with context to allow trace fields in logger
	found, err = repository.GIndex("UserIndex").GetItemsWithContext(ctx, key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}
}
