package djoemo_test

import (
	. "djoemo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReflectHelper", func() {
	Describe("transform interface of slice to slice of interfaces", func() {
		It("should transform interface of slice to slice of interfaces", func() {
			items := []string{"a", "b"}
			ret, err := InterfaceToArrayOfInterface(items)
			Expect(err).To(BeNil())
			Expect(len(ret)).To(Equal(2))
			Expect(ret[0].(string)).To(Equal("a"))
			Expect(ret[1].(string)).To(Equal("b"))
		})

		It("should return error when not pass interface of slice", func() {
			item := "a"
			ret, err := InterfaceToArrayOfInterface(item)
			Expect(err).To(Equal(ErrInvalidSliceType))
			Expect(ret).To(BeNil())
		})
	})
})
