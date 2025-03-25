package djoemo_test

import (
	"context"
	"errors"
	. "github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Repository", func() {

	const (
		UserTableName = "UserTable"
	)

	var (
		dMock       mock.DynamoMock
		repository  RepositoryInterface
		logMock     *mock.MockLogInterface
		metricsMock *mock.MockMetricsInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = NewRepository(dAPIMock)
	})

	Describe("DeleteItem", func() {
		Describe("DeleteItem Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")

				err := repository.DeleteItem(key)
				Expect(err).To(BeEqualTo(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")

				err := repository.DeleteItem(key)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")

				err := repository.DeleteItem(key)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
			})
		})
		It("should delete item by hash key", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			deleteDBInput := map[string]interface{}{
				"UUID": "uuid",
			}

			dMock.Should().
				Delete(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInput(deleteDBInput),
				).Exec()

			err := repository.DeleteItem(key)
			Expect(err).To(BeNil())
		})
		It("should delete item by hash key and range key", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName").
				WithRangeKey("user")

			deleteDBInput := map[string]interface{}{
				"UUID":     "uuid",
				"UserName": "user",
			}

			dMock.Should().
				Delete(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInput(deleteDBInput),
				).Exec()

			err := repository.DeleteItem(key)
			Expect(err).To(BeNil())
		})

		It("should return error in case of db error", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			deleteDBInput := map[string]interface{}{
				"UUID": "uuid",
			}
			err := errors.New("failed to delete")
			dMock.Should().
				Delete(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInput(deleteDBInput),
					dMock.WithError(err),
				).Exec()

			ret := repository.DeleteItem(key)
			Expect(ret).To(BeEqualTo(err))
		})
	})

	Describe("DeleteItems", func() {
		Describe("DeleteItem Invalid keys", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				key1 := Key().WithHashKeyName("UUID").WithHashKey("uuid1")
				keys := []KeyInterface{key, key1}

				err := repository.DeleteItems(keys)
				Expect(err).To(BeEqualTo(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				key1 := Key().WithTableName(UserTableName).WithHashKey("uuid1")
				keys := []KeyInterface{key, key1}

				err := repository.DeleteItems(keys)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				key1 := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				keys := []KeyInterface{key, key1}

				err := repository.DeleteItems(keys)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
			})
		})

		It("should delete items by hash key", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			err := repository.DeleteItems(keys)
			Expect(err).To(BeNil())
		})

		It("should delete items by hash and range key ", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName").
				WithRangeKey("user")
			key1 := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1").
				WithRangeKeyName("UserName").
				WithRangeKey("user1")

			keys := []KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid", "UserName": "user"}, {"UUID": "uuid1", "UserName": "user1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			err := repository.DeleteItems(keys)
			Expect(err).To(BeNil())
		})

		It("should return error in case of db error", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			err := errors.New("failed to delete")
			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
					dMock.WithError(err),
				).Exec()

			ret := repository.DeleteItems(keys)
			Expect(ret).To(BeEqualTo(err))
		})

		It("should return nil if keys empty", func() {
			var keys []KeyInterface

			err := repository.DeleteItems(keys)
			Expect(err).To(BeNil())
		})

		It("should publish metrics if metric is supported", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().WithContext(context.TODO()).Return(metricsMock)
			metricsMock.EXPECT().Publish(key.TableName(), MetricNameDeleteItemsCount, float64(2)).Return(nil)
			err := repository.DeleteItems(keys)
			Expect(err).To(BeNil())
		})

		It("should not affect save and log error if publish failed", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			repository.WithMetrics(metricsMock)
			repository.WithLog(logMock)
			metricsMock.EXPECT().WithContext(context.TODO()).Return(metricsMock)
			metricsMock.EXPECT().Publish(key.TableName(), MetricNameDeleteItemsCount, float64(2)).
				Return(errors.New("failed to publish"))

			logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
			logMock.EXPECT().WithContext(context.TODO()).Return(logMock)
			logMock.EXPECT().Error("failed to publish")
			err := repository.DeleteItems(keys)
			Expect(err).To(BeNil())
		})
	})
})
