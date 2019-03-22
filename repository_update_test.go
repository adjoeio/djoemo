package djoemo_test

import (
	"context"
	"errors"
	. "github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = NewRepository(dAPIMock)
	})

	Describe("Update", func() {
		Describe("Update Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.Update(Set, key, updates)
				Expect(err).To(Equal(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.Update(Set, key, updates)
				Expect(err).To(Equal(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.Update(Set, key, updates)
				Expect(err).To(Equal(ErrInvalidHashKeyValue))
			})
		})

		It("should Update item with Set", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("email").
				WithRangeKey("mail@adjoe.io")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.Update(Set, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetSet", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.Update(SetSet, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetIfNotExists", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.Update(SetIfNotExists, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetExpr", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("Meta.#sMZXW6", "bar"),
				),
			).Exec()

			updates := map[string]interface{}{
				"Meta.$ = ?": []interface{}{"foo", "bar"},
			}

			err := repository.Update(SetExpr, key, updates)
			Expect(err).To(BeNil())
		})
		It("should return in err in case of db err", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("email").
				WithRangeKey("mail@adjoe.io")

			err := errors.New("failed to update item")
			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithError(err),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			ret := repository.Update(Set, key, updates)
			Expect(ret).To(Equal(err))
		})
	})

	Describe("Log", func() {
		It("should log with extra fields if log is supported", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("email").
				WithRangeKey("mail@adjoe.io")
			err := errors.New("failed to update item")
			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithError(err),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			repository.WithLog(logMock)
			logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
			logMock.EXPECT().WithContext(context.TODO()).Return(logMock)
			logMock.EXPECT().Error(err.Error())
			ret := repository.Update(Set, key, updates)
			Expect(ret).To(BeEquivalentTo(err))

		})
	})

	Describe("Metrics", func() {
		It("should publish metrics if metric is supported", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			repository.WithMetrics(metricsMock)
			metricsMock.EXPECT().WithContext(context.TODO()).Return(metricsMock)
			metricsMock.EXPECT().Publish(key.TableName(), MetricNameUpdatedItemsCount, float64(1)).Return(nil)
			err := repository.Update(SetSet, key, updates)
			Expect(err).To(BeNil())
		})

		It("should not affect update and log error if publish failed", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			repository.WithMetrics(metricsMock)
			repository.WithLog(logMock)
			metricsMock.EXPECT().WithContext(context.TODO()).Return(metricsMock)
			metricsMock.EXPECT().Publish(key.TableName(), MetricNameUpdatedItemsCount, float64(1)).
				Return(errors.New("failed to publish"))
			logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
			logMock.EXPECT().WithContext(context.TODO()).Return(logMock)
			logMock.EXPECT().Error("failed to publish")
			err := repository.Update(SetSet, key, updates)
			Expect(err).To(BeNil())
		})
	})
})
