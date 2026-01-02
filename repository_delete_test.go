package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"

	"github.com/adjoeio/djoemo/mock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Repository", func() {
	const (
		UserTableName = "UserTable"
	)

	var (
		dMock       mock.DynamoMock
		repository  djoemo.RepositoryInterface
		logMock     *mock.MockLogInterface
		metricsMock *mock.MockMetricsInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = djoemo.NewRepository(dAPIMock)
	})

	Describe("DeleteItem", func() {
		Describe("DeleteItem Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")

				err := repository.DeleteItemWithContext(context.Background(), key)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")

				err := repository.DeleteItemWithContext(context.Background(), key)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")

				err := repository.DeleteItemWithContext(context.Background(), key)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})
		It("should delete item by hash key", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			err := repository.DeleteItemWithContext(context.Background(), key)
			Expect(err).To(BeNil())
		})
		It("should delete item by hash key and range key", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			err := repository.DeleteItemWithContext(context.Background(), key)
			Expect(err).To(BeNil())
		})

		It("should return error in case of db error", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			ret := repository.DeleteItemWithContext(context.Background(), key)
			Expect(ret).To(Equal(err))
		})
	})

	Describe("DeleteItems", func() {
		Describe("DeleteItem Invalid keys", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				key1 := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid1")
				keys := []djoemo.KeyInterface{key, key1}

				err := repository.DeleteItemsWithContext(context.Background(), keys)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				key1 := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid1")
				keys := []djoemo.KeyInterface{key, key1}

				err := repository.DeleteItemsWithContext(context.Background(), keys)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				key1 := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				keys := []djoemo.KeyInterface{key, key1}

				err := repository.DeleteItemsWithContext(context.Background(), keys)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		It("should delete items by hash key", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []djoemo.KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			err := repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(err).To(BeNil())
		})

		It("should delete items by hash and range key ", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName").
				WithRangeKey("user")
			key1 := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1").
				WithRangeKeyName("UserName").
				WithRangeKey("user1")

			keys := []djoemo.KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid", "UserName": "user"}, {"UUID": "uuid1", "UserName": "user1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			err := repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(err).To(BeNil())
		})

		It("should return error in case of db error", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []djoemo.KeyInterface{key, key1}

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

			ret := repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(ret).To(Equal(err))
		})

		It("should return nil if keys empty", func() {
			var keys []djoemo.KeyInterface

			err := repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(err).To(BeNil())
		})

		It("should record metrics", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []djoemo.KeyInterface{key, key1}

			deleteDBInput := []map[string]interface{}{
				{"UUID": "uuid"}, {"UUID": "uuid1"},
			}

			dMock.Should().
				DeleteAll(
					dMock.WithTable(key.TableName()),
					dMock.WithDeleteInputs(deleteDBInput),
				).Exec()

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpDelete, key, gomock.Any(), true)
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpDelete, key1, gomock.Any(), true)
			err := repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(err).To(BeNil())
		})

		It("should log on error", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")
			key1 := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid1")

			keys := []djoemo.KeyInterface{key, key1}

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

			repository.WithLog(logMock)

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpDelete, key, gomock.Any(), false)
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpDelete, key1, gomock.Any(), false)

			err = repository.DeleteItemsWithContext(context.Background(), keys)
			Expect(err).To(Equal(err))
		})
	})

	Describe("Log", func() {
		It("should log with extra fields if log is supported for DeleteItemWithContext", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			repository.WithLog(logMock)

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().Record(gomock.Any(), "delete", key, gomock.Any(), false)

			ret := repository.DeleteItemWithContext(context.Background(), key)
			Expect(ret).To(BeEquivalentTo(err))
		})
	})

	Describe("Metrics", func() {
		It("should record metrics if metric is supported for DeleteItemWithContext", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().Record(gomock.Any(), "delete", key, gomock.Any(), true)

			err := repository.DeleteItemWithContext(context.Background(), key)
			Expect(err).To(BeNil())
		})
	})
})
