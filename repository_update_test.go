package djoemo_test

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/mock/gomock"

	. "github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
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
				Expect(err).To(BeEqualTo(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.Update(Set, key, updates)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.Update(Set, key, updates)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
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
		It("should Update item with Add", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("ElemCount", 1),
				),
			).Exec()

			updates := map[string]interface{}{
				"ElemCount": 1,
			}

			err := repository.Update(Add, key, updates)
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
			Expect(ret).To(BeEqualTo(err))
		})
	})

	Describe("UpdateItem with condition", func() {
		It("should save an item if the condition is met", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			updates := map[string]interface{}{
				"UserName": "username2",
			}

			dMock.Should().Save(
				dMock.WithTable(UserTableName),
				dMock.WithConditionExpression("(UserName = ?)", "username"),
				dMock.WithInput(updates),
			).Exec()

			expression := "UserName = ?"
			expressionArgs := "username"
			updated, err := repository.ConditionalUpdateWithContext(context.Background(), key, updates, expression, expressionArgs)

			Expect(err).To(BeNil())
			Expect(updated).To(BeEqualTo(true))
		})

		It("should reject the update of an item if the condition is not met", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			updates := map[string]interface{}{
				"UserName": "username",
			}

			dMock.Should().Save(
				dMock.WithTable(UserTableName),
				dMock.WithConditionExpression("(UserName = ?)", "user"),
				dMock.WithInput(updates),
				dMock.WithError(errors.New((&types.ConditionalCheckFailedException{}).ErrorCode())),
			).Exec()

			expression := "UserName = ?"
			expressionArgs := "user"
			updated, err := repository.ConditionalUpdateWithContext(context.Background(), key, updates, expression, expressionArgs)

			Expect(err).To(HaveOccurred())
			Expect(updated).To(BeEqualTo(false))
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
