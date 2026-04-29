package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = djoemo.NewRepository(dAPIMock)
		repository.WithMetrics(metricsMock)
		repository.WithLog(logMock)
	})

	Describe("Update", func() {
		Describe("Update Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)
				updates := map[string]interface{}{
					"UserName": "name2",
					"TraceID":  "name4",
				}

				err := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		It("should Update item with Set", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetSet", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.SetSet, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetIfNotExists", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.SetIfNotExists, key, updates)
			Expect(err).To(BeNil())
		})

		It("should Update item with SetExpr", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("Meta.#sMZXW6", "bar"),
				),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"Meta.$ = ?": []interface{}{"foo", "bar"},
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.SetExpr, key, updates)
			Expect(err).To(BeNil())
		})
		It("should Update item with Add", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("ElemCount", 1),
				),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"ElemCount": 1,
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.Add, key, updates)
			Expect(err).To(BeNil())
		})
		It("should return in err in case of db err", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("email").
				WithRangeKey("mail@adjoe.io")

			err := errors.New("failed to update item")
			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithError(err),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			ret := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
			Expect(ret).To(Equal(err))
		})
	})

	Describe("UpdateItem with condition", func() {
		It("should save an item if the condition is met", func() {
			key := djoemo.Key().WithTableName(UserTableName).
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

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			expression := "UserName = ?"
			expressionArgs := "username"
			updated, err := repository.ConditionalUpdateWithContext(context.Background(), key, updates, expression, expressionArgs)

			Expect(err).To(BeNil())
			Expect(updated).To(Equal(true))
		})

		It("should reject the update of an item if the condition is not met", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			updates := map[string]interface{}{
				"UserName": "username",
			}

			dMock.Should().Save(
				dMock.WithTable(UserTableName),
				dMock.WithConditionExpression("(UserName = ?)", "user"),
				dMock.WithInput(updates),
				dMock.WithError(errors.New(dynamodb.ErrCodeConditionalCheckFailedException)),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			expression := "UserName = ?"
			expressionArgs := "user"
			updated, err := repository.ConditionalUpdateWithContext(context.Background(), key, updates, expression, expressionArgs)

			Expect(err).To(HaveOccurred())
			Expect(updated).To(Equal(false))
		})
	})

	Describe("Log", func() {
		It("should log with extra fields if log is supported", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("email").
				WithRangeKey("mail@adjoe.io")
			err := errors.New("failed to update item")
			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithError(err),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			ret := repository.UpdateWithContext(context.Background(), djoemo.Set, key, updates)
			Expect(ret).To(BeEquivalentTo(err))
		})
	})

	Describe("Metrics", func() {
		It("should record metrics if metric is supported", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().Update(
				dMock.WithTable(key.TableName()),
				dMock.WithMatch(
					mock.InputExpect().
						FieldEq("UserName", "name2").FieldEq("TraceID", "name4"),
				),
			).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := map[string]interface{}{
				"UserName": "name2",
				"TraceID":  "name4",
			}

			err := repository.UpdateWithContext(context.Background(), djoemo.SetSet, key, updates)
			Expect(err).To(BeNil())
		})
	})
})
