package djoemo_test

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/mock/gomock"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

	Describe("SaveItem", func() {
		Describe("SaveItem invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				user := &User{
					UUID: "uuid",
				}
				err := repository.SaveItemWithContext(context.Background(), key, user)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				user := &User{
					UUID: "uuid",
				}
				err := repository.SaveItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				user := &User{
					UUID: "uuid",
				}
				err := repository.SaveItemWithContext(context.Background(), key, user)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		It("should save item", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			userDBInput := map[string]interface{}{
				"UUID":      "uuid",
				"UserName":  "name1",
				"UpdatedAt": "0001-01-01T00:00:00Z",
				"CreatedAt": "0001-01-01T00:00:00Z",
			}

			dMock.Should().
				Save(
					dMock.WithTable(key.TableName()),
					dMock.WithInput(userDBInput),
				).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), true)

			user := &User{
				UUID:     "uuid",
				UserName: "name1",
			}
			err := repository.SaveItemWithContext(context.Background(), key, user)

			Expect(err).To(BeNil())
		})
	})
	Describe("SaveItems", func() {
		Describe("SaveItem invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := djoemo.Key().WithHashKeyName("UUID").WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItemsWithContext(context.Background(), key, users)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItemsWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := djoemo.Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItemsWithContext(context.Background(), key, users)

				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		It("should save items", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName")

			userDBInput := []map[string]interface{}{
				{
					"UUID":      "uuid1",
					"UserName":  "name1",
					"UpdatedAt": "0001-01-01T00:00:00Z",
					"CreatedAt": "0001-01-01T00:00:00Z",
				},
				{
					"UUID":      "uuid2",
					"UserName":  "name2",
					"UpdatedAt": "0001-01-01T00:00:00Z",
					"CreatedAt": "0001-01-01T00:00:00Z",
				},
			}

			dMock.Should().
				SaveAll(
					dMock.WithTable(key.TableName()),
					dMock.WithInputs(userDBInput),
				).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), true)

			users := []User{
				{
					UUID:     "uuid1",
					UserName: "name1",
				},
				{
					UUID:     "uuid2",
					UserName: "name2",
				},
			}
			err := repository.SaveItemsWithContext(context.Background(), key, users)
			Expect(err).To(BeNil())
		})

		It("should fail when not pass slice", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName")
			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)

			users := User{
				UUID:     "uuid1",
				UserName: "name1",
			}
			err := repository.SaveItemsWithContext(context.Background(), key, users)
			Expect(err).To(Equal(djoemo.ErrInvalidSliceType))
		})

		It("should return in err in case of db err", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName")

			userDBInput := []map[string]interface{}{
				{
					"UUID":      "uuid1",
					"UserName":  "name1",
					"UpdatedAt": "0001-01-01T00:00:00Z",
					"CreatedAt": "0001-01-01T00:00:00Z",
				},
				{
					"UUID":      "uuid2",
					"UserName":  "name2",
					"UpdatedAt": "0001-01-01T00:00:00Z",
					"CreatedAt": "0001-01-01T00:00:00Z",
				},
			}
			err := errors.New("failed to save items")
			dMock.Should().
				SaveAll(
					dMock.WithTable(key.TableName()),
					dMock.WithInputs(userDBInput),
					dMock.WithError(err),
				).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)

			users := []User{
				{
					UUID:     "uuid1",
					UserName: "name1",
				},
				{
					UUID:     "uuid2",
					UserName: "name2",
				},
			}
			ret := repository.SaveItemsWithContext(context.Background(), key, users)
			Expect(ret).To(Equal(err))
		})
	})

	Describe("Optimistic Lock Save", func() {
		djoemoTimeNow := djoemo.Now
		BeforeEach(func() {
			now := time.Date(2019, 1, 1, 12, 15, 0, 0, time.UTC)
			djoemo.Now = func() djoemo.DjoemoTime {
				return djoemo.DjoemoTime{Time: now}
			}
		})
		AfterEach(func() {
			djoemo.Now = djoemoTimeNow
		})
		It("should save an item with optimistic Locking", func() {
			type DjoemoUser struct {
				djoemo.Model
				User
			}
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			userDBInput := map[string]interface{}{
				"UUID":      "uuid",
				"UserName":  "name1",
				"UpdatedAt": 1546344900000000000,
				"CreatedAt": 1546344900000000000,
				"Version":   1,
			}

			dMock.Should().
				Save(
					dMock.WithTable(key.TableName()),
					dMock.WithConditionExpression("(attribute_not_exists(Version) OR Version = ?)", 0),
					dMock.WithInput(userDBInput),
				).Exec()

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), true)

			user := &DjoemoUser{
				User: User{
					UUID:     "uuid",
					UserName: "name1",
				},
			}

			saved, err := repository.OptimisticLockSaveWithContext(context.Background(), key, user)

			Expect(err).To(BeNil())
			Expect(saved).To(BeTrue())
		})
	})

	Describe("Log", func() {
		It("should log with extra fields if log is supported", func() {
			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			userDBInput := map[string]interface{}{
				"UUID":      "uuid",
				"UserName":  "name1",
				"UpdatedAt": "0001-01-01T00:00:00Z",
				"CreatedAt": "0001-01-01T00:00:00Z",
			}
			err := errors.New("cannot save user")
			dMock.Should().
				Save(
					dMock.WithTable(key.TableName()),
					dMock.WithInput(userDBInput),
					dMock.WithError(err),
				).Exec()

			user := &User{
				UUID:     "uuid",
				UserName: "name1",
			}

			metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), false)

			ret := repository.SaveItemWithContext(context.Background(), key, user)
			Expect(ret).To(BeEquivalentTo(err))
		})
	})

	Describe("Metrics", func() {
		Describe("SaveItem", func() {
			It("should record metrics if metric is supported", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBInput := map[string]interface{}{
					"UUID":      "uuid",
					"UserName":  "name1",
					"UpdatedAt": "0001-01-01T00:00:00Z",
					"CreatedAt": "0001-01-01T00:00:00Z",
				}

				dMock.Should().
					Save(
						dMock.WithTable(key.TableName()),
						dMock.WithInput(userDBInput),
					).Exec()

				user := &User{
					UUID:     "uuid",
					UserName: "name1",
				}

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpCommit, key, gomock.Any(), true)

				err := repository.SaveItemWithContext(context.Background(), key, user)
				Expect(err).To(BeNil())
			})
		})

		Describe("SaveItems", func() {
			It("should record metrics with source label", func() {
				key := djoemo.Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("UserName")

				userDBInput := []map[string]interface{}{
					{
						"UUID":      "uuid1",
						"UserName":  "name1",
						"UpdatedAt": "0001-01-01T00:00:00Z",
						"CreatedAt": "0001-01-01T00:00:00Z",
					},
					{
						"UUID":      "uuid2",
						"UserName":  "name2",
						"UpdatedAt": "0001-01-01T00:00:00Z",
						"CreatedAt": "0001-01-01T00:00:00Z",
					},
				}

				dMock.Should().
					SaveAll(
						dMock.WithTable(key.TableName()),
						dMock.WithInputs(userDBInput),
					).Exec()

				users := []User{
					{
						UUID:     "uuid1",
						UserName: "name1",
					},
					{
						UUID:     "uuid2",
						UserName: "name2",
					},
				}

				ctx := djoemo.WithSourceLabel(context.Background(), "FooBarAPI")
				metricsMock.EXPECT().Record(ctx, djoemo.OpCommit, key, gomock.Any(), true).
					Do(func(ctx context.Context, caller string, key djoemo.KeyInterface, duration time.Duration, success bool) {
						Expect(djoemo.GetLabelsFromContext(ctx)["source"]).To(Equal("FooBarAPI"))
					})

				err := repository.SaveItemsWithContext(ctx, key, users)

				Expect(err).To(BeNil())
			})
		})
	})
})
