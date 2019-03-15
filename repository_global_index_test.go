package djoemo_test

import (
	. "djoemo"
	"djoemo/mock"
	"errors"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Global Index", func() {
	const (
		UserTableName    = "UserTable"
		ProfileTableName = "ProfileTable"
		IndexName        = "gindex"
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

	Describe("Get", func() {
		Describe("Get Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &User{}
				found, err := repository.GIndex(IndexName).Get(key, user)

				Expect(err).To(Equal(ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				user := &User{}
				found, err := repository.GIndex(IndexName).Get(key, user)

				Expect(err).To(Equal(ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &User{}
				found, err := repository.GIndex(IndexName).Get(key, user)

				Expect(err).To(Equal(ErrInvalidHashKeyValue))
				Expect(found).To(BeFalse())
			})
		})

		Describe("GetItems Invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				users := &[]User{}
				found, err := repository.GIndex(IndexName).Get(key, users)

				Expect(err).To(Equal(ErrInvalidTableName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				users := &[]User{}
				found, err := repository.GIndex(IndexName).Get(key, users)

				Expect(err).To(Equal(ErrInvalidHashKeyName))
				Expect(found).To(BeFalse())
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				users := &[]User{}
				found, err := repository.GIndex(IndexName).Get(key, users)

				Expect(err).To(Equal(ErrInvalidHashKeyValue))
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
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				user := &User{}
				found, err := repository.GIndex(IndexName).Get(key, user)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(user.UUID).To(Equal(userDBOutput["UUID"]))
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
					Query(
						dMock.WithTable(key.TableName()),
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithCondition(*key.RangeKeyName(), key.RangeKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profile := &Profile{}
				found, err := repository.GIndex(IndexName).Get(key, profile)

				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				Expect(profile.UUID).To(Equal(profileDBOutput["UUID"]))
				Expect(profile.Email).To(Equal(profileDBOutput["Email"]))
			})

			It("should return false and nil if item was not found", func() {
				key := Key().WithTableName(UserTableName).
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
				found, err := repository.GIndex(IndexName).Get(key, user)

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
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				user := &User{}
				found, err := repository.GIndex(IndexName).Get(key, user)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})
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
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(userDBOutput),
					).Exec()

				users := &[]User{}
				found, err := repository.GIndex(IndexName).GetItems(key, users)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *users
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(userDBOutput[0]["UUID"]))
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
						dMock.WithIndex(IndexName),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(profileDBOutput),
					).Exec()

				profiles := &[]Profile{}
				found, err := repository.GIndex(IndexName).GetItems(key, profiles)
				Expect(err).To(BeNil())
				Expect(found).To(BeTrue())
				result := *profiles
				Expect(len(result)).To(Equal(2))
				Expect(result[0].UUID).To(Equal(profileDBOutput[0]["UUID"]))

			})

			It("should return false and nil if item was not found", func() {
				key := Key().WithTableName(UserTableName).
					WithHashKeyName("UUID").
					WithHashKey("uuid")

				dMock.Should().
					Query(
						dMock.WithIndex(IndexName),
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithQueryOutput(nil),
					).Exec()

				users := &[]User{}
				found, err := repository.GIndex(IndexName).GetItems(key, users)

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
						dMock.WithIndex(IndexName),
						dMock.WithTable(key.TableName()),
						dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
						dMock.WithError(err),
					).Exec()

				users := &[]User{}
				found, err := repository.GIndex(IndexName).GetItems(key, users)

				Expect(err).To(BeEquivalentTo(err))
				Expect(found).To(BeFalse())
			})

		})
	})
})
