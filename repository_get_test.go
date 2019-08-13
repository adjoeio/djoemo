package djoemo_test

import (
	"context"
	"errors"

	. "github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	"github.com/golang/mock/gomock"
	"github.com/guregu/dynamo"
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

	Describe("GetItem", func() {
		Describe("GetItem Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &User{}
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeEqualTo(ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				user := &User{}
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &User{}
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})

		Describe("GetItems Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeEqualTo(ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeEqualTo(ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeEqualTo(ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})
		Describe("GetItem", func() {
			It("should get item with Hash", func() {
				key := Key().WithTableName(UserTableName).
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
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(user.UUID).To(BeEqualTo(userDBOutput["UUID"]))
			})

			It("should get item with Hash and range", func() {
				key := Key().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid").
					WithRangeKeyName("Email").
					WithRangeKey("user@adjeo.io")

				profileDBOutput := map[string]interface{}{
					"UUID":  "uuid",
					"Email": "user@adjeo.io",
				}

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithRange(*key.RangeKeyName(), key.RangeKey()),
						dMock.WithGetOutput(profileDBOutput),
					).Exec()

				profile := &Profile{}
				found, err := repository.GetItem(key, profile)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(profile.UUID).To(BeEqualTo(profileDBOutput["UUID"]))
				Expect(profile.Email).To(BeEqualTo(profileDBOutput["Email"]))
			})

			It("should return false and nil if item was not found", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithGetOutput(nil),
					).Exec()

				user := &User{}
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")
				dMock.Should().
					Get(
						dMock.WithTable(key.TableName()),
						dMock.WithHash(*key.HashKeyName(), key.HashKey()),
						dMock.WithError(err),
					).Exec()

				user := &User{}
				found, err := repository.GetItem(key, user)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
		})

		It("should return false and nil if dynamos ErrNotFound occured", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().
				Get(
					dMock.WithTable(key.TableName()),
					dMock.WithHash(*key.HashKeyName(), key.HashKey()),
					dMock.WithError(dynamo.ErrNotFound),
					dMock.WithGetOutput(nil),
				).Exec()

			user := &User{}
			found, err := repository.GetItem(key, user)

			Expect(err).To(BeNil())
			Expect(found).To(BeFalse())
		})

		Describe("GetItems", func() {
			It("should get items with Hash", func() {
				key := Key().WithTableName(UserTableName).
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
				found, err := repository.GetItems(key, users)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *users
				Expect(len(result)).To(BeEqualTo(2))
				Expect(result[0].UUID).To(BeEqualTo(userDBOutput[0]["UUID"]))
			})

			It("should get items with Hash and ignore range", func() {
				key := Key().WithTableName(ProfileTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				profileDBOutput := []map[string]interface{}{
					{"UUID": "uuid", "Email": "email1"},
					{"UUID": "uuid", "Email": "email2"},
				}

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profiles := &[]Profile{}
				found, err := repository.GetItems(key, profiles)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *profiles
				Expect(len(result)).To(BeEqualTo(2))
				Expect(result[0].UUID).To(BeEqualTo(profileDBOutput[0]["UUID"]))

			})

			It("should return false and nil if item was not found", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and nil if dynamos ErrNotFound occured", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(dynamo.ErrNotFound),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeNil())
				Expect(found).To(BeFalse())
			})

			It("should return false and error in case of error", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")
				err := errors.New("invalid query")
				dMock.Should().
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				users := &[]User{}
				found, err := repository.GetItems(key, users)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})

		})

		Describe("GetItems with Iterator", func() {
			It("should return items one-by-one when iterating via NextItem", func() {
				key := Key().WithTableName(UserTableName).
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

	})
})
