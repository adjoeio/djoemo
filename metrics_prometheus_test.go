package djoemo_test

import (
	"context"
	"sync"
	"time"

	"github.com/adjoeio/djoemo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var _ = Describe("Prometheus Metrics", func() {
	var (
		registry *prometheus.Registry
		metrics  *djoemo.Metrics
	)

	BeforeEach(func() {
		registry = prometheus.NewRegistry()
		promMetrics := djoemo.NewPrometheusMetrics(registry)
		metrics = djoemo.New()
		metrics.Add(promMetrics)
	})

	Describe("Record", func() {
		It("records counter and histogram with success status", func() {
			key := djoemo.Key().WithTableName("UserTable").WithHashKeyName("UUID").WithHashKey("id-1")
			metrics.Record(context.Background(), djoemo.OpRead, key, 50*time.Millisecond, true)

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			Expect(mfs).NotTo(BeEmpty(), "expected metrics from Gather, got names: %v", metricFamilyNames(mfs))

			// Find read counter and read_duration_seconds histogram (Prometheus adds _total to counters)
			var readTotal *dto.MetricFamily
			var readDuration *dto.MetricFamily
			for _, mf := range mfs {
				switch mf.GetName() {
				case "read":
					readTotal = mf
				case "read_duration_seconds":
					readDuration = mf
				}
			}
			Expect(readTotal).NotTo(BeNil(), "read_total counter should be registered, got: %v", metricFamilyNames(mfs))
			Expect(readDuration).NotTo(BeNil(), "read_duration_seconds histogram should be registered, got: %v", metricFamilyNames(mfs))

			// Counter: expect status=success, table=usertable
			Expect(readTotal.GetMetric()).To(HaveLen(1))
			Expect(readTotal.GetMetric()[0].GetCounter().GetValue()).To(Equal(1.0))
			labels := readTotal.GetMetric()[0].GetLabel()
			Expect(getLabelValue(labels, "status")).To(Equal("success"))
			Expect(getLabelValue(labels, "table")).To(Equal("usertable"))
			Expect(getLabelValue(labels, "source")).To(Equal("metrics_prometheus_test.go"))

			// Histogram: expect duration in seconds (50ms = 0.05)
			Expect(readDuration.GetMetric()).To(HaveLen(1))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.05, 0.001))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(1)))
		})

		It("records with failure status", func() {
			key := djoemo.Key().WithTableName("OrderTable").WithHashKeyName("ID").WithHashKey("order-1")
			metrics.Record(context.Background(), djoemo.OpCommit, key, 100*time.Millisecond, false)

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var commitTotal *dto.MetricFamily
			var commitDuration *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "commit" {
					commitTotal = mf
				}
				if mf.GetName() == "commit_duration_seconds" {
					commitDuration = mf
				}
			}
			Expect(commitTotal).NotTo(BeNil())
			Expect(commitTotal.GetMetric()[0].GetLabel()).To(ContainElement(&dto.LabelPair{Name: stringPtr("status"), Value: stringPtr("failure")}))
			Expect(commitDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.1, 0.001))
			Expect(commitDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(1)))
		})

		It("handles nil key with empty table label", func() {
			metrics.Record(context.Background(), djoemo.OpRead, nil, 10*time.Millisecond, true)

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var readTotal *dto.MetricFamily
			var readDuration *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "read" {
					readTotal = mf
				}
				if mf.GetName() == "read_duration_seconds" {
					readDuration = mf
				}
			}
			Expect(readTotal).NotTo(BeNil())
			Expect(getLabelValue(readTotal.GetMetric()[0].GetLabel(), "table")).To(Equal(""))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.01, 0.001))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(1)))
		})

		It("picks up source label from context", func() {
			ctx := djoemo.WithSourceLabel(context.Background(), "checkout-service")
			key := djoemo.Key().WithTableName("CartTable").WithHashKeyName("ID").WithHashKey("cart-1")
			metrics.Record(ctx, djoemo.OpUpdate, key, 25*time.Millisecond, true)

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var updateTotal *dto.MetricFamily
			var updateDuration *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "update" {
					updateTotal = mf
				}
				if mf.GetName() == "update_duration_seconds" {
					updateDuration = mf
				}
			}
			Expect(updateTotal).NotTo(BeNil())
			Expect(getLabelValue(updateTotal.GetMetric()[0].GetLabel(), "source")).To(Equal("checkout-service"))
			Expect(updateDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.025, 0.001))
			Expect(updateDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(1)))
			Expect(getLabelValue(updateDuration.GetMetric()[0].GetLabel(), "source")).To(Equal("checkout-service"))
		})
	})

	Describe("Duplicate registration safety", func() {
		It("does not panic when multiple instances share the same registry", func() {
			sharedRegistry := prometheus.NewRegistry()

			// Create two prometheus metrics instances with same registry
			pm1 := djoemo.NewPrometheusMetrics(sharedRegistry)
			pm2 := djoemo.NewPrometheusMetrics(sharedRegistry)

			m := djoemo.New()
			m.Add(pm1)
			m.Add(pm2)

			key := djoemo.Key().WithTableName("TestTable").WithHashKeyName("ID").WithHashKey("1")
			Expect(func() {
				m.Record(context.Background(), "read", key, 10*time.Millisecond, true)
				m.Record(context.Background(), "read", key, 20*time.Millisecond, true)
			}).NotTo(Panic())

			mfs, err := sharedRegistry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var readTotal *dto.MetricFamily
			var readDuration *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "read" {
					readTotal = mf
				}
				if mf.GetName() == "read_duration_seconds" {
					readDuration = mf
				}
			}
			Expect(readTotal).NotTo(BeNil())
			// Both Record calls should have incremented the same counter
			Expect(readTotal.GetMetric()[0].GetCounter().GetValue()).To(Equal(4.0)) // Count of 2 calls * 2 metrics instances
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.06, 0.001))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(4)))
		})
	})

	Describe("Concurrent Record", func() {
		It("safely handles concurrent Record calls for the same caller", func() {
			key := djoemo.Key().WithTableName("UserTable").WithHashKeyName("UUID").WithHashKey("id")

			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					metrics.Record(context.Background(), djoemo.OpRead, key, 5*time.Millisecond, true)
				}()
			}
			wg.Wait()

			mfs, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			var readTotal *dto.MetricFamily
			var readDuration *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "read" {
					readTotal = mf
				}
				if mf.GetName() == "read_duration_seconds" {
					readDuration = mf
				}
			}
			Expect(readTotal).NotTo(BeNil())
			Expect(readTotal.GetMetric()[0].GetCounter().GetValue()).To(Equal(100.0))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleSum()).To(BeNumerically("~", 0.5, 0.001))
			Expect(readDuration.GetMetric()[0].GetHistogram().GetSampleCount()).To(Equal(uint64(100)))
		})
	})
})

func getLabelValue(labels []*dto.LabelPair, name string) string {
	for _, l := range labels {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func metricFamilyNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, len(mfs))
	for i, mf := range mfs {
		names[i] = mf.GetName()
	}
	return names
}

func stringPtr(s string) *string {
	return &s
}
