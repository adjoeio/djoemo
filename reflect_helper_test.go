package djoemo_test

import (
	"github.com/adjoeio/djoemo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReflectHelper", func() {
	Describe("transform interface of slice to slice of interfaces", func() {
		It("should transform interface of slice to slice of interfaces", func() {
			items := []string{"a", "b"}
			ret, err := djoemo.InterfaceToArrayOfInterface(items)
			Expect(err).To(BeNil())
			Expect(len(ret)).To(BeEquivalentTo(2))

			Expect(ret[0].(string)).To(BeEquivalentTo("a"))
			Expect(ret[1].(string)).To(BeEquivalentTo("b"))
		})

		It("should return error when not pass interface of slice", func() {
			item := "a"
			ret, err := djoemo.InterfaceToArrayOfInterface(item)
			Expect(err).To(BeEquivalentTo(djoemo.ErrInvalidSliceType))
			Expect(ret).To(BeNil())
		})
	})
})
