package examples

import (
	"adjoe.io/djoemo"
	"adjoe.io/djoemo/mock"
	"context"
	"fmt"
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
	Msg       string              `djoemo:"Message"`
	Count     int                 `djoemo:",omitempty"`
	Friends   []string            `djoemo:",set"` // Sets
	Set       map[string]struct{} `djoemo:",set"` // Map sets, too!
	SecretKey string              `djoemo:"-"`    // Ignored
}

// Get shows an example how to get an item from dynamodb
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

	// get item with context to allow trace fields in logger
	found, err = repository.Get(key, user)
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

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// get item with context to allow trace fields in logger
	found, err = repository.Get(key, users)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("users not found")
	}
}

// Save shows an example, how to save an item
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
	err := repository.Save(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}
	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// Save item with context to allow trace fields in logger
	err = repository.Save(key, user)
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
	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

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
	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// Update item with context to allow trace fields in logger
	err = repository.Update(djoemo.Set, key, updates)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// Delete shows an example, how to delete item by key
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
	err := repository.Delete(key)
	if err != nil {
		fmt.Println(err.Error())
	}
	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// Delete item with context to allow trace fields in logger
	err = repository.Delete(key)
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
	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

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
	found, err := repository.GIndex("UserIndex").Get(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// GIndex item with context to allow trace fields in logger
	found, err = repository.GIndex("UserIndex").GetWithContext(key, user, ctx)
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
	found, err := repository.GIndex("UserIndex").Get(key, user)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}

	// context is optional param, which used to enable log with context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "TraceInfo", map[string]interface{}{"TraceID": "trace-id"})

	// GetItems with context to allow trace fields in logger
	found, err = repository.GIndex("UserIndex").GetItemsWithContext(key, user, ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	if !found {
		fmt.Println("user not found")
	}
}
