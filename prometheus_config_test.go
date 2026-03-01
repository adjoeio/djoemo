package djoemo_test

import (
	"context"
	"errors"

	"github.com/adjoeio/djoemo"
	"github.com/adjoeio/djoemo/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Prometheus Config", func() {
	const (
		UserTableName = "UserTable"
		IndexName     = "gindex"
	)

	Describe("Repository WithPrometheusMetrics", func() {
		It("uses custom namespace and subsystem from config", func() {
			mockCtrl := gomock.NewController(GinkgoT())
			dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
			dMock := mock.NewDynamoMock(dAPIMock)
			logMock := mock.NewMockLogInterface(mockCtrl)

			registry := prometheus.NewRegistry()
			cfg := djoemo.DefaultPrometheusConfig()
			cfg.Namespace = "myapp"
			cfg.Subsystem = "dynamodb"
			cfg.Log = logMock

			repository := djoemo.NewRepository(dAPIMock)
			repository.WithLog(logMock)
			repository.WithPrometheusMetrics(registry, cfg)

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
			found, err := repository.GetItemWithContext(context.Background(), key, user)

			Expect(err).To(BeNil())
			Expect(found).To(BeTrue())

			mfs, gatherErr := registry.Gather()
			Expect(gatherErr).NotTo(HaveOccurred())
			Expect(mfs).NotTo(BeEmpty())

			// Custom config should produce myapp_dynamodb_* metrics
			var readCounter *dto.MetricFamily
			var readDuration *dto.MetricFamily
			for _, mf := range mfs {
				switch mf.GetName() {
				case "myapp_dynamodb_read":
					readCounter = mf
				case "myapp_dynamodb_read_duration_seconds":
					readDuration = mf
				}
			}
			Expect(readCounter).NotTo(BeNil(), "expected myapp_dynamodb_read metric, got: %v", metricFamilyNames(mfs))
			Expect(readDuration).NotTo(BeNil(), "expected myapp_dynamodb_read_duration_seconds metric, got: %v", metricFamilyNames(mfs))
			Expect(readCounter.GetMetric()[0].GetCounter().GetValue()).To(Equal(1.0))
		})

		It("uses custom ConstLabels from config", func() {
			mockCtrl := gomock.NewController(GinkgoT())
			dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
			dMock := mock.NewDynamoMock(dAPIMock)
			logMock := mock.NewMockLogInterface(mockCtrl)

			registry := prometheus.NewRegistry()
			cfg := djoemo.DefaultPrometheusConfig()
			cfg.ConstLabels = prometheus.Labels{"env": "test", "service": "api"}
			cfg.Log = logMock

			repository := djoemo.NewRepository(dAPIMock)
			repository.WithLog(logMock)
			repository.WithPrometheusMetrics(registry, cfg)

			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dMock.Should().
				Get(
					dMock.WithTable(key.TableName()),
					dMock.WithHash(*key.HashKeyName(), key.HashKey()),
					dMock.WithGetOutput(nil),
				).Exec()

			logMock.EXPECT().WithContext(gomock.Any()).Return(logMock)
			logMock.EXPECT().WithField(djoemo.TableName, key.TableName()).Return(logMock)
			logMock.EXPECT().Info(djoemo.ErrNoItemFound.Error())

			user := &User{}
			_, _ = repository.GetItemWithContext(context.Background(), key, user)

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var readCounter *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "adjoe_djoemo_read" {
					readCounter = mf
					break
				}
			}
			Expect(readCounter).NotTo(BeNil())
			Expect(getLabelValue(readCounter.GetMetric()[0].GetLabel(), "env")).To(Equal("test"))
			Expect(getLabelValue(readCounter.GetMetric()[0].GetLabel(), "service")).To(Equal("api"))
		})
	})

	Describe("GlobalIndex WithPrometheusMetrics", func() {
		It("uses custom namespace and subsystem from config", func() {
			mockCtrl := gomock.NewController(GinkgoT())
			dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
			dMock := mock.NewDynamoMock(dAPIMock)
			logMock := mock.NewMockLogInterface(mockCtrl)

			registry := prometheus.NewRegistry()
			cfg := djoemo.DefaultPrometheusConfig()
			cfg.Namespace = "myapp"
			cfg.Subsystem = "gindex"
			cfg.Log = logMock

			repository := djoemo.NewRepository(dAPIMock)
			repository.WithLog(logMock)
			repository.GIndex(IndexName).WithPrometheusMetrics(registry, cfg)

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
			found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

			Expect(err).To(BeNil())
			Expect(found).To(BeTrue())

			mfs, gatherErr := registry.Gather()
			Expect(gatherErr).NotTo(HaveOccurred())
			Expect(mfs).NotTo(BeEmpty())

			var readCounter *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "myapp_gindex_read" {
					readCounter = mf
					break
				}
			}
			Expect(readCounter).NotTo(BeNil(), "expected myapp_gindex_read metric, got: %v", metricFamilyNames(mfs))
			Expect(readCounter.GetMetric()[0].GetCounter().GetValue()).To(Equal(1.0))
		})

		It("records failure when GetItem returns error", func() {
			mockCtrl := gomock.NewController(GinkgoT())
			dAPIMock := mock.NewMockDynamoDBAPI(mockCtrl)
			dMock := mock.NewDynamoMock(dAPIMock)
			logMock := mock.NewMockLogInterface(mockCtrl)

			registry := prometheus.NewRegistry()
			cfg := djoemo.DefaultPrometheusConfig()
			cfg.Namespace = "test"
			cfg.Subsystem = "index"
			cfg.Log = logMock

			repository := djoemo.NewRepository(dAPIMock)
			repository.WithLog(logMock)
			repository.GIndex(IndexName).WithPrometheusMetrics(registry, cfg)

			key := djoemo.Key().WithTableName(UserTableName).
				WithHashKeyName("UUID").
				WithHashKey("uuid")

			dynamoErr := errors.New("dynamodb error")
			dMock.Should().
				Query(
					dMock.WithTable(key.TableName()),
					dMock.WithIndex(IndexName),
					dMock.WithCondition(*key.HashKeyName(), key.HashKey(), "EQ"),
					dMock.WithError(dynamoErr),
				).Exec()

			user := &User{}
			found, err := repository.GIndex(IndexName).GetItemWithContext(context.Background(), key, user)

			Expect(err).To(Equal(dynamoErr))
			Expect(found).To(BeFalse())

			mfs, gatherErr := registry.Gather()
			Expect(gatherErr).NotTo(HaveOccurred())
			var readCounter *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "test_index_read" {
					readCounter = mf
					break
				}
			}
			Expect(readCounter).NotTo(BeNil())
			Expect(getLabelValue(readCounter.GetMetric()[0].GetLabel(), "status")).To(Equal("failure"))
		})
	})
})
