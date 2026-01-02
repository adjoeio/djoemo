package djoemo_test

import (
	"context"
	"errors"

	"go.uber.org/mock/gomock"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Global Index", func() {
	const (
		UserTableName    = "UserTable"
		ProfileTableName = "ProfileTable"
		IndexName        = "gindex"
	)

	var (
		dMock       mock.DynamoMock
		repository  djoemo.RepositoryInterface
		metricsMock *mock.MockMetricsInterface
		logMock     *mock.MockLogInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		repository = djoemo.NewRepository(dAPIMock)
		repository.WithMetrics(metricsMock)
		repository.WithLog(logMock)
	})

	Describe("GetItem", func() {
		Describe("GetItem Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})

		Describe("GetItems Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})
		Describe("GetItem", func() {
			It("should get item with Hash", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(user.UUID).To(Equal(userDBOutput["UUID"]))
			})

			It("should get item with Hash and range", func() {
				key := djoemo.Key().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user@adjeo.io")

				profileDBOutput := map[string]interface{}{
					"UUID":  "uuid",
					"Email": "user@adjeo.io",
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithCondition(*key.RangeKeyName(), key.RangeKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profile := &Profile{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, profile)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(profile.UUID).To(Equal(profileDBOutput["UUID"]))
				Expect(profile.Email).To(Equal(profileDBOutput["Email"]))
			})

			It("should return false and nil if item was not found", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(nil),
					).Exec()

				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
				logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
				logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
				logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})
		Describe("GetItems", func() {
			It("should get items with Hash", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := []map[string]interface{}{
					{"UUID": "uuid", "UserName": "name1"},
					{"UUID": "uuid", "UserName": "name2"},
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GIndex(IndexName).GetItemsWithContext(context.Background(), key, users)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *users
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(userDBOutput[0]["UUID"]))
			})

			It("should get items with Hash and ignore range", func() {
				key := djoemo.Key().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				profileDBOutput := []map[string]interface{}{
					{"UUID": "uuid", "Email": "email1"},
					{"UUID": "uuid", "Email": "email2"},
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profiles := &[]Profile{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GIndex(IndexName).GetItemsWithContext(context.Background(), key, profiles)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *profiles
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(profileDBOutput[0]["UUID"]))
			})

			It("should return false and nil if item was not found", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("AppID").
					WithRangeKey("appid")

				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
				logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
				logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
				logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

				found, err := repository.GIndex(IndexName).GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")
				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GIndex(IndexName).GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})
		Describe("GetItemsWithRangeWithContext", func() {
			It("should get items with Hash and Range", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("AppID").
					WithRangeKey("appid")

				userDBOutput := []map[string]interface{}{
					{"UUID": "uuid", "UserName": "name1"},
					{"UUID": "uuid", "UserName": "name2"},
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithCondition(*key.RangeKeyName(), key.RangeKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				users := &[]User{}
				found, err := repository.GIndex(IndexName).GetItemsWithRangeWithContext(context.Background(), key, users)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *users
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(userDBOutput[0]["UUID"]))
			})
		})
	})

	Describe("Query Items", func() {
		Describe("Query Items Invalid key ", func() {
			It("should fail with table name is nil", func() {
				query := djoemo.Query().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &[]User{}
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				err := repository.QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				query := djoemo.Query().WithTableName(UserTableName).WithHashKey("uuid")
				user := &[]User{}
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				query := djoemo.Query().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &[]User{}
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		Describe("Query Items", func() {
			It("should query items with Hash", func() {
				q := djoemo.Query().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var users []User
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), q, &users)
				Expect(err).To(BeNil())
				Expect(users[0].UUID).To(Equal(userDBOutput["UUID"]))
			})

			It("should query items with hash and range", func() {
				q := djoemo.Query().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user").
					WithRangeOp(djoemo.BeginsWith)

				profileDBOutput := []map[string]interface{}{
					{
						"UUID":  "uuid1",
						"Email": "user1@adjeo.io",
					}, {
						"UUID":  "uuid2",
						"Email": "user2@adjeo.io",
					},
				}

				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithCondition(*q.RangeKeyName(), q.RangeKey(), string(djoemo.BeginsWith)),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var profiles []Profile
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), q, &profiles)

				Expect(err).To(BeNil())
				Expect(len(profiles)).To(Equal(2))
				Expect(profiles[0].UUID).To(Equal("uuid1"))
				Expect(profiles[1].UUID).To(Equal("uuid2"))
			})

			It("should query items with limit and order", func() {
				q := djoemo.Query().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithLimit(2).
					WithDescending()

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithQueryOutput(userDBOutput),
						dMock.WithLimit(2),
						dMock.WithDesc(true),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var users []User
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), q, &users)
				Expect(err).To(BeNil())
				Expect(users[0].UUID).To(Equal(userDBOutput["UUID"]))
			})

			It("should return error if output is not pointer to slice ", func() {
				q := djoemo.Query().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user").
					WithRangeOp(djoemo.BeginsWith)
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), false)

				var profile Profile
				err := repository.GIndex(IndexName).QueryWithContext(context.Background(), q, &profile)

				Expect(err).To(BeEquivalentTo(djoemo.ErrInvalidPointerSliceType))
			})
		})
	})
})
