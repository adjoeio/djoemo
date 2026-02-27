package djoemo_test

import (
	"context"

	"go.uber.org/mock/gomock"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		repository = djoemo.NewRepository(dAPIMock)
		repository.WithMetrics(metricsMock)
		repository.WithLog(logMock)
	})

	Describe("djoemo.Query Items", func() {
		Describe("djoemo.Query Items Invalid key ", func() {
			It("should fail with table name is nil", func() {
				query := djoemo.Query().WithHashKeyName("UUID").WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				user := &[]User{}
				err := repository.QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				query := djoemo.Query().WithTableName(UserTableName).WithHashKey("uuid")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				user := &[]User{}
				err := repository.QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				query := djoemo.Query().WithTableName(UserTableName).WithHashKeyName("UUID")
				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, query, gomock.Any(), false)
				user := &[]User{}
				err := repository.QueryWithContext(context.Background(), query, user)
				Expect(err).To(Equal(djoemo.ErrInvalidHashKeyValue))
			})
		})

		Describe("djoemo.Query Items", func() {
			It("should query items with Hash", func() {
				q := djoemo.Query().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Query(
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var users []User
				err := repository.QueryWithContext(context.Background(), q, &users)
				Expect(err).To(BeNil())
				Expect(users[0].UUID).To(Equal(userDBOutput["UUID"]))
			})

			It("should query items with Hash", func() {
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
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithQueryOutput(userDBOutput),
						dMock.WithLimit(2),
						dMock.WithDesc(true),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var users []User
				err := repository.QueryWithContext(context.Background(), q, &users)
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
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
						dMock.WithCondition(*q.RangeKeyName(), q.RangeKey(), string(djoemo.BeginsWith)),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), true)

				var profiles []Profile
				err := repository.QueryWithContext(context.Background(), q, &profiles)

				Expect(err).To(BeNil())
				Expect(len(profiles)).To(Equal(2))
				Expect(profiles[0].UUID).To(Equal("uuid1"))
				Expect(profiles[1].UUID).To(Equal("uuid2"))
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
				err := repository.QueryWithContext(context.Background(), q, &profile)

				Expect(err).To(BeEquivalentTo(djoemo.ErrInvalidPointerSliceType))
			})

			Describe("Log", func() {
				It("should log with extra fields if log is supported", func() {
					q := djoemo.Query().WithTableName(UserTableName).
						WithHashKeyName("UUID").
						WithHashKey("uuid")
					err := context.DeadlineExceeded
					dMock.Should().
						Query(
							dMock.WithTable(q.TableName()),
							dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
							dMock.WithError(err),
						).Exec()

					var users []User

					metricsMock.EXPECT().Record(gomock.Any(), djoemo.OpRead, q, gomock.Any(), false)

					ret := repository.QueryWithContext(context.Background(), q, &users)
					Expect(ret).To(BeEquivalentTo(err))
				})
			})

			Describe("Metrics", func() {
				It("should record metrics if metric is supported", func() {
					q := djoemo.Query().WithTableName(UserTableName).
						WithHashKeyName("UUID").
						WithHashKey("uuid")

					userDBOutput := map[string]interface{}{
						"UUID": "uuid",
					}

					dMock.Should().
						Query(
							dMock.WithTable(q.TableName()),
							dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(djoemo.Equal)),
							dMock.WithQueryOutput(userDBOutput),
						).Exec()

					var users []User

					metricsMock.EXPECT().Record(gomock.Any(), "read", q, gomock.Any(), true)

					err := repository.QueryWithContext(context.Background(), q, &users)
					Expect(err).To(BeNil())
				})
			})
		})
	})
})
