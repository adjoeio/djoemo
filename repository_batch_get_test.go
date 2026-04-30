package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Repository BatchGetItemsWithContext", func() {
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

	It("should return false and nil when keys slice is empty", func() {
		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(context.Background(), []djoemo.KeyInterface{}, users)
		Expect(err).To(BeNil())
		Expect(found).To(BeFalse())
	})

	It("should return error when key has no table name", func() {
		invalidKey := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, invalidKey, gomock.Any(), false)

		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{invalidKey},
			users,
		)
		Expect(err).To(Equal(djoemo.ErrInvalidTableName))
		Expect(found).To(BeFalse())
	})

	It("should return error when keys refer to different tables", func() {
		key1 := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid1")
		key2 := djoemo.Key().WithTableName("OtherTable").
			WithHashKeyName("UUID").WithHashKey("uuid2")

		// Note: success flag captured from outer err var which isn't
		// updated for ErrInvalidBatchRequest return path.
		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, gomock.Any(), gomock.Any(), gomock.Any()).Times(2)

		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{key1, key2},
			users,
		)
		Expect(err).To(Equal(djoemo.ErrInvalidBatchRequest))
		Expect(found).To(BeFalse())
	})

	It("should return items when batch get succeeds by hash key", func() {
		key1 := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid1")
		key2 := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid2")

		items := []map[string]*dynamodb.AttributeValue{}
		for _, u := range []map[string]interface{}{
			{"UUID": "uuid1", "UserName": "name1"},
			{"UUID": "uuid2", "UserName": "name2"},
		} {
			av, _ := dynamodbattribute.MarshalMap(u)
			items = append(items, av)
		}
		output := &dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*dynamodb.AttributeValue{
				UserTableName: items,
			},
		}

		dAPIMock.EXPECT().
			BatchGetItemWithContext(gomock.Any(), gomock.Any()).
			Return(output, nil).
			AnyTimes()

		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key1, gomock.Any(), true)
		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key2, gomock.Any(), true)

		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{key1, key2},
			users,
		)
		Expect(err).To(BeNil())
		Expect(found).To(BeTrue())
		Expect(len(*users)).To(Equal(2))
	})

	It("should handle hash and range keys", func() {
		key1 := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid1").
			WithRangeKeyName("Email").WithRangeKey("a@adjoe.io")
		key2 := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid2").
			WithRangeKeyName("Email").WithRangeKey("b@adjoe.io")

		items := []map[string]*dynamodb.AttributeValue{}
		for _, u := range []map[string]interface{}{
			{"UUID": "uuid1", "Email": "a@adjoe.io"},
			{"UUID": "uuid2", "Email": "b@adjoe.io"},
		} {
			av, _ := dynamodbattribute.MarshalMap(u)
			items = append(items, av)
		}
		output := &dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*dynamodb.AttributeValue{
				UserTableName: items,
			},
		}

		dAPIMock.EXPECT().
			BatchGetItemWithContext(gomock.Any(), gomock.Any()).
			Return(output, nil).
			AnyTimes()

		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key1, gomock.Any(), true)
		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key2, gomock.Any(), true)

		profiles := &[]Profile{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{key1, key2},
			profiles,
		)
		Expect(err).To(BeNil())
		Expect(found).To(BeTrue())
		Expect(len(*profiles)).To(Equal(2))
	})

	It("should return false and nil when dynamo returns ErrNotFound", func() {
		key := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid")

		output := &dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*dynamodb.AttributeValue{
				UserTableName: {},
			},
		}

		dAPIMock.EXPECT().
			BatchGetItemWithContext(gomock.Any(), gomock.Any()).
			Return(output, nil).
			AnyTimes()

		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
		logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
		logMock.EXPECT().WithField(djoemo.TableName, UserTableName).Return(logMock)
		logMock.EXPECT().Info(djoemo.ErrNoItemFound.Error())

		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{key},
			users,
		)
		Expect(err).To(BeNil())
		Expect(found).To(BeFalse())
	})

	It("should return false and error when dynamo returns an error", func() {
		key := djoemo.Key().WithTableName(UserTableName).
			WithHashKeyName("UUID").WithHashKey("uuid")

		dbErr := errors.New("some dynamo error")

		dAPIMock.EXPECT().
			BatchGetItemWithContext(gomock.Any(), gomock.Any()).
			Return(nil, dbErr).
			AnyTimes()

		metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

		users := &[]User{}
		found, err := repository.BatchGetItemsWithContext(
			context.Background(),
			[]djoemo.KeyInterface{key},
			users,
		)
		Expect(err).To(HaveOccurred())
		Expect(found).To(BeFalse())
	})
})
