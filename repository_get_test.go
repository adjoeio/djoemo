package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/guregu/dynamo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Repository", func() {
	const (
		UserTableName    = "UserTable"
		ProfileTableName = "ProfileTable"
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
		dMock = mock.NewDynamoMock(dAPIMock)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		logMock = mock.NewMockLogInterface(mockCtrl)
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

				found, err := repository.GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GetItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})

		Describe("GetItems Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, err := repository.GetItemsWithContext(context.Background(), key, users)

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

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithGetOutput(userDBOutput),
					).Exec()

				user := &User{}
				found, err := repository.GetItemWithContext(context.Background(), key, user)

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

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithRange(*key.RangeKeyName(), key.RangeKey()),
						dMock.WithGetOutput(profileDBOutput),
					).Exec()

				profile := &Profile{}
				found, err := repository.GetItemWithContext(context.Background(), key, profile)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(profile.UUID).To(Equal(profileDBOutput["UUID"]))
				Expect(profile.Email).To(Equal(profileDBOutput["Email"]))
			})

			It("should return false and nil if item was not found", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
				logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
				logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
				logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithGetOutput(nil),
					).Exec()

				user := &User{}
				found, err := repository.GetItemWithContext(context.Background(), key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithError(err),
					).Exec()

				user := &User{}
				found, err := repository.GetItemWithContext(context.Background(), key, user)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})

		It("should return false and nil if dynamos ErrNotFound occurred", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
			logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
			logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
			logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

			dMock.Should().
				Get(
					dMock.WithTable(key.TableName()),
					dMock.WithHash(*key.HashKeyName(), key.HashKey()),
					dMock.WithError(dynamo.ErrNotFound),
					dMock.WithGetOutput(nil),
				).Exec()

			user := &User{}
			found, err := repository.GetItemWithContext(context.Background(), key, user)

			Expect(err).To(BeNil())
			Expect(found).To(BeFalse())
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

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItemsWithContext(context.Background(), key, users)
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

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profiles := &[]Profile{}
				found, err := repository.GetItemsWithContext(context.Background(), key, profiles)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *profiles
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(profileDBOutput[0]["UUID"]))
			})

			It("should return false and nil if item was not found", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
				logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
				logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
				logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and nil if dynamos ErrNotFound occurred", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)
				logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
				logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
				logMock.EXPECT().Info((djoemo.ErrNoItemFound).Error())

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(dynamo.ErrNotFound),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItemsWithContext(context.Background(), key, users)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})

		Describe("GetItems with Iterator", func() {
			It("should return items one-by-one when iterating via NextItem", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				scanLimit := int64(1)
				scanOutput := []map[string]interface{}{
					{
						"UUID":     "uuid",
						"Email":    "email",
						"UserName": "user",
					},
					{
						"UUID":     "uuidTwo",
						"Email":    "emailTwo",
						"UserName": "userTwo",
					},
				}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				dMock.Should().ScanAll(
					dMock.WithTable(UserTableName),
					dMock.WithScanAllOutput(scanOutput),
					dMock.WithLimit(scanLimit),
				).Exec()

				itr, _ := repository.ScanIteratorWithContext(context.Background(), key, scanLimit)

				user := User{}
				var users []User
				for itr.NextItem(&user) {
					users = append(users, user)
				}

				Expect(len(users)).To(Equal(2))
				Expect(users[0].UserName).To(Equal("user"))
				Expect(users[1].UserName).To(Equal("userTwo"))
			})
		})
		Describe("Log", func() {
			It("should log with extra fields if log is supported for GetItemWithContext", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("failed to get item")
				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithError(err),
					).Exec()

				user := &User{}
				repository.WithLog(logMock)
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, ret := repository.GetItemWithContext(context.Background(), key, user)
				Expect(ret).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})

			It("should log with extra fields if log is supported for GetItemsWithContext", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("failed to get items")
				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				users := &[]User{}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), false)

				found, ret := repository.GetItemsWithContext(context.Background(), key, users)
				Expect(ret).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})

		Describe("Metrics", func() {
			It("should record metrics if metric is supported for GetItemWithContext", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithGetOutput(userDBOutput),
					).Exec()

				user := &User{}
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GetItemWithContext(context.Background(), key, user)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
			})

			It("should record metrics if metric is supported for GetItemsWithContext", func() {
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
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				users := &[]User{}
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, key, gomock.Any(), true)

				found, err := repository.GetItemsWithContext(context.Background(), key, users)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
			})
		})
	})
})
