package djoemo_test

import (
	"go.uber.org/mock/gomock"

	. "github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
)

var _ = Describe("Repository", func() {
	const (
		UserTableName    = "UserTable"
		ProfileTableName = "ProfileTable"
	)

	var (
		dMock      mock.DynamoMock
		repository RepositoryInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = NewRepository(dAPIMock)
	})

	Describe("Query Items", func() {
		Describe("Query Items Invalid key ", func() {
			It("should fail with table name is nil", func() {
				query := Query().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &[]User{}
				err := repository.Query(query, user)
				Expect(err).To(BeEqualTo(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				query := Query().WithTableName(UserTableName).WithHashKey("uuid")
				user := &[]User{}
				err := repository.Query(query, user)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				query := Query().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &[]User{}
				err := repository.Query(query, user)
				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
			})
		})

		Describe("Query Items", func() {
			It("should query items with Hash", func() {
				q := Query().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				userDBOutput := map[string]interface{}{
					"UUID": "uuid",
				}

				dMock.Should().
					Query(
						dMock.WithTable(q.TableName()),
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(Equal)),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				var users []User
				err := repository.Query(q, &users)
				Expect(err).To(BeNil())
				Expect(users[0].UUID).To(BeEqualTo(userDBOutput["UUID"]))
			})

			It("should query items with Hash", func() {
				q := Query().WithTableName(UserTableName).
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
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(Equal)),
						dMock.WithQueryOutput(userDBOutput),
						dMock.WithLimit(2),
						dMock.WithDesc(true),
					).Exec()

				var users []User
				err := repository.Query(q, &users)
				Expect(err).To(BeNil())
				Expect(users[0].UUID).To(BeEqualTo(userDBOutput["UUID"]))
			})

			It("should query items with hash and range", func() {
				q := Query().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user").
					WithRangeOp(BeginsWith)

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
						dMock.WithCondition(*q.HashKeyName(), q.HashKey(), string(Equal)),
						dMock.WithCondition(*q.RangeKeyName(), q.RangeKey(), string(BeginsWith)),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				var profiles []Profile
				err := repository.Query(q, &profiles)

				Expect(err).To(BeNil())
				Expect(len(profiles)).To(BeEqualTo(2))
				Expect(profiles[0].UUID).To(BeEqualTo("uuid1"))
				Expect(profiles[1].UUID).To(BeEqualTo("uuid2"))
			})

			It("should return error if output is not pointer to slice ", func() {
				q := Query().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user").
					WithRangeOp(BeginsWith)

				var profile Profile
				err := repository.Query(q, &profile)

				Expect(err).To(BeEquivalentTo(ErrInvalidPointerSliceType))
			})
		})
	})
})
