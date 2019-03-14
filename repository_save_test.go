package djoemo_test

import (
	"adjoe.io/djoemo/mock"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	"github.com/pkg/errors"

	. "github.com/onsi/gomega"

	. "adjoe.io/djoemo"
)

var _ = Describe("Repository", func() {
	const (
		UserTableName = "UserTable"
	)

	var (
		dMock       mock.DynamoMock
		repository  RepositoryInterface
		logMock     *mock.MockLogInterface
		metricsMock *mock.MockMetricsInterface
	)

	BeforeEach(func() {
		mockCtrl := gomock.NewController(GinkgoT())
		dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
		logMock = mock.NewMockLogInterface(mockCtrl)
		metricsMock = mock.NewMockMetricsInterface(mockCtrl)
		dMock = mock.NewDynamoMock(dAPIMock)
		repository = NewRepository(dAPIMock)
	})

	Describe("Save", func() {
		Describe("Save invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				user := &User{
					UUID: "uuid",
				}
				err := repository.Save(key, user)
				Expect(err).To(Equal(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				user := &User{
					UUID: "uuid",
				}
				err := repository.Save(key, user)

				Expect(err).To(Equal(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				user := &User{
					UUID: "uuid",
				}
				err := repository.Save(key, user)

				Expect(err).To(Equal(ErrInvalidHashKeyValue))
			})
		})

		It("should save item", func() {
			key := Key().WithTableName(UserTableName).
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
			err := repository.Save(key, user)

			Expect(err).To(BeNil())
		})
	})
	Describe("SaveItems", func() {
		Describe("Save invalid key ", func() {
			It("should fail with table name is nil", func() {
				key := Key().WithHashKeyName("UUID").WithHashKey("uuid")
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItems(key, users)
				Expect(err).To(Equal(ErrInvalidTableName))
			})
			It("should fail with hash key name is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKey("uuid")
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItems(key, users)

				Expect(err).To(Equal(ErrInvalidHashKeyName))
			})
			It("should fail with hash key value is nil", func() {
				key := Key().WithTableName(UserTableName).WithHashKeyName("UUID")
				users := []User{
					{
						UUID: "uuid1",
					},
					{
						UUID: "uuid2",
					},
				}
				err := repository.SaveItems(key, users)

				Expect(err).To(Equal(ErrInvalidHashKeyValue))
			})
		})

		It("should save items", func() {
			key := Key().WithTableName(UserTableName).
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
			err := repository.SaveItems(key, users)
			Expect(err).To(BeNil())
		})

		It("should fail when not pass slice", func() {
			key := Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid").
				WithRangeKeyName("UserName")

			users := User{
				UUID:     "uuid1",
				UserName: "name1",
			}
			err := repository.SaveItems(key, users)
			Expect(err).To(Equal(ErrInvalidSliceType))
		})

		It("should return in err in case of db err", func() {
			key := Key().WithTableName(UserTableName).
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
			ret := repository.SaveItems(key, users)
			Expect(ret).To(Equal(err))
		})
	})

	Describe("Log", func() {
		It("should log with extra fields if log is supported", func() {
			key := Key().WithTableName(UserTableName).
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

			ctx := WithFields(map[string]interface{}{"TraceID": "trace-id", "UUID": "uuid"})
			logMock.EXPECT().WithContext(ctx).Return(logMock)
			repository.WithLog(logMock)
			logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
			logMock.EXPECT().Errorf(err.Error(), nil)
			ret := repository.Save(key, user)
			Expect(ret).To(BeEquivalentTo(err))
		})
	})

	Describe("Metrics", func() {
		Describe("Save", func() {
			It("should publish metrics if metric is supported", func() {
				key := Key().WithTableName(UserTableName).
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

				repository.WithMetrics(metricsMock)
				metricsMock.EXPECT().Publish(key.TableName(), MetricNameSavedItemsCount, float64(1)).Return(nil)
				ctx := WithFields(map[string]interface{}{"TraceID": "trace-id", "UUID": "uuid"})
				logMock.EXPECT().WithContext(ctx).Return(logMock)
				err := repository.Save(key, user)
				Expect(err).To(BeNil())
			})

			It("should not affect save and log error if publish failed", func() {
				key := Key().WithTableName(UserTableName).
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

				repository.WithMetrics(metricsMock)
				repository.WithLog(logMock)
				metricsMock.EXPECT().Publish(key.TableName(), MetricNameSavedItemsCount, float64(1)).
					Return(errors.New("failed to publish"))
				logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
				ctx := WithFields(map[string]interface{}{"TraceID": "trace-id", "UUID": "uuid"})
				logMock.EXPECT().WithContext(ctx).Return(logMock)
				logMock.EXPECT().Errorf("failed to publish", nil)
				err := repository.Save(key, user)
				Expect(err).To(BeNil())
			})
		})

		Describe("SaveItems", func() {
			It("should publish metrics if metric is supported", func() {
				key := Key().WithTableName(UserTableName).
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

				traceInfo := map[string]interface{}{"TraceID": "trace-id", "UUID": "uuid"}
				repository.WithMetrics(metricsMock)
				metricsMock.EXPECT().Publish(key.TableName(), MetricNameSavedItemsCount, float64(2)).Return(nil)
				err := repository.SaveItemsWithContext(key, users, WithFields(traceInfo))

				Expect(err).To(BeNil())
			})

			It("should not affect save and log error if publish failed", func() {
				key := Key().WithTableName(UserTableName).
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

				repository.WithMetrics(metricsMock)
				repository.WithLog(logMock)
				metricsMock.EXPECT().Publish(key.TableName(), MetricNameSavedItemsCount, float64(2)).
					Return(errors.New("failed to publish"))
				logMock.EXPECT().WithFields(map[string]interface{}{"TableName": key.TableName()}).Return(logMock)
				ctx := WithFields(map[string]interface{}{"TraceID": "trace-id", "UUID": "uuid"})
				logMock.EXPECT().WithContext(ctx).Return(logMock)
				logMock.EXPECT().Errorf("failed to publish", nil)
				err := repository.SaveItems(key, users)
				Expect(err).To(BeNil())
			})
		})
	})
})
