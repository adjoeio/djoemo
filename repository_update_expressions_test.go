package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	awserr "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Repository UpdateWithUpdateExpressions", func() {
	const UserTableName = "UserTable"

	var (
		dAPIMock    *mock.MockDynamoDBAPI
		repository  djoemo.RepositoryInterface
		logMock     *mock.MockLogInterface
		metricsMock *mock.MockMetricsInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		dAPIMock = mock.NewMockDynamoDBAPI(mockCtrl)
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		repository = djoemo.NewRepository(dAPIMock)
		repository.WithLog(logMock)
		repository.WithMetrics(metricsMock)
	})

	Describe("UpdateWithUpdateExpressions", func() {
		It("should fail with invalid key", func() {
			key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			err := repository.UpdateWithUpdateExpressions(context.Background(), key, updates)
			Expect(err).To(Equal(djoemo.ErrInvalidTableName))
		})

		It("should update with multiple expression types", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid").
				WithRangeKeyName("Email").WithRangeKey("mail@adjoe.io")

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(&dynamodb.UpdateItemOutput{}, nil).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := djoemo.UpdateExpressions{
				djoemo.Set:            {"UserName": "name"},
				djoemo.Add:            {"Counter": 1},
				djoemo.SetSet:         {"Tags": []string{"a"}},
				djoemo.SetIfNotExists: {"TraceID": "trace"},
			}

			err := repository.UpdateWithUpdateExpressions(context.Background(), key, updates)
			Expect(err).To(BeNil())
		})

		It("should update with SetExpr", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(&dynamodb.UpdateItemOutput{}, nil).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			updates := djoemo.UpdateExpressions{
				djoemo.SetExpr: {"Meta.$ = ?": []interface{}{"foo", "bar"}},
			}

			err := repository.UpdateWithUpdateExpressions(context.Background(), key, updates)
			Expect(err).To(BeNil())
		})

		It("should return error if SetExpr value is not a slice", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			updates := djoemo.UpdateExpressions{
				djoemo.SetExpr: {"Meta.$ = ?": "not a slice"},
			}

			err := repository.UpdateWithUpdateExpressions(context.Background(), key, updates)
			Expect(err).To(Equal(djoemo.ErrInvalidSliceType))
		})

		It("should return error when dynamodb fails", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			dbErr := errors.New("update failed")
			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(nil, dbErr).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			err := repository.UpdateWithUpdateExpressions(context.Background(), key, updates)
			Expect(err).To(Equal(dbErr))
		})
	})

	Describe("UpdateWithUpdateExpressionsAndReturnValue", func() {
		It("should fail with invalid key", func() {
			key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			err := repository.UpdateWithUpdateExpressionsAndReturnValue(context.Background(), key, user, updates)
			Expect(err).To(Equal(djoemo.ErrInvalidTableName))
		})

		It("should update and return value", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			updated := map[string]interface{}{
				"UUID":     "uuid",
				"UserName": "new-name",
			}
			av, _ := dynamodbattribute.MarshalMap(updated)

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(&dynamodb.UpdateItemOutput{Attributes: av}, nil).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "new-name"},
			}

			err := repository.UpdateWithUpdateExpressionsAndReturnValue(context.Background(), key, user, updates)
			Expect(err).To(BeNil())
			Expect(user.UserName).To(Equal("new-name"))
		})

		It("should return error from dynamodb", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			dbErr := errors.New("update failed")
			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(nil, dbErr).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			err := repository.UpdateWithUpdateExpressionsAndReturnValue(context.Background(), key, user, updates)
			Expect(err).To(Equal(dbErr))
		})
	})

	Describe("ConditionalUpdateWithUpdateExpressionsAndReturnValue", func() {
		It("should fail with invalid key", func() {
			key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			updated, err := repository.ConditionalUpdateWithUpdateExpressionsAndReturnValue(
				context.Background(), key, user, updates, "attribute_exists(UUID)")
			Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			Expect(updated).To(BeFalse())
		})

		It("should update when condition met and return value", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			updated := map[string]interface{}{
				"UUID":     "uuid",
				"UserName": "conditional-name",
			}
			av, _ := dynamodbattribute.MarshalMap(updated)

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(&dynamodb.UpdateItemOutput{Attributes: av}, nil).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), true)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "conditional-name"},
			}

			ok, err := repository.ConditionalUpdateWithUpdateExpressionsAndReturnValue(
				context.Background(), key, user, updates, "attribute_exists(UUID)")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())
			Expect(user.UserName).To(Equal("conditional-name"))
		})

		It("should return false and nil when ConditionalCheckFailed", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			condErr := awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "cond failed", nil)

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(nil, condErr).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)
			logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
			logMock.EXPECT().WithField(djoemo.TableName, UserTableName).Return(logMock)
			logMock.EXPECT().Info(dynamodb.ErrCodeConditionalCheckFailedException)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			ok, err := repository.ConditionalUpdateWithUpdateExpressionsAndReturnValue(
				context.Background(), key, user, updates, "attribute_exists(UUID)")
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("should return false and error for other dynamo errors", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			dbErr := errors.New("some dynamo error")

			dAPIMock.EXPECT().
				UpdateItemWithContext(gomock.Any(), gomock.Any()).
				Return(nil, dbErr).
				AnyTimes()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.Set: {"UserName": "name"},
			}

			ok, err := repository.ConditionalUpdateWithUpdateExpressionsAndReturnValue(
				context.Background(), key, user, updates, "attribute_exists(UUID)")
			Expect(err).To(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("should propagate error from prepareUpdate when SetExpr has invalid value", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").WithHashKey("uuid")

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpUpdate, key, gomock.Any(), false)

			user := &User{}
			updates := djoemo.UpdateExpressions{
				djoemo.SetExpr: {"Meta.$ = ?": "not a slice"},
			}

			ok, err := repository.ConditionalUpdateWithUpdateExpressionsAndReturnValue(
				context.Background(), key, user, updates, "attribute_exists(UUID)")
			Expect(err).To(Equal(djoemo.ErrInvalidSliceType))
			Expect(ok).To(BeFalse())
		})
	})
})
