package djoemo_test

import (
	. "github.com/adjoeio/djoemo/v2"
)

var _ = Describe("ReflectHelper", func() {
	Describe("transform interface of slice to slice of interfaces", func() {
		It("should transform interface of slice to slice of interfaces", func() {
			items := []string{"a", "b"}
			ret, err := InterfaceToArrayOfInterface(items)
			Expect(err).To(BeNil())
			Expect(len(ret)).To(BeEqualTo(2))

			Expect(ret[0].(string)).To(BeEqualTo("a"))
			Expect(ret[1].(string)).To(BeEqualTo("b"))
		})

		It("should return error when not pass interface of slice", func() {
			item := "a"
			ret, err := InterfaceToArrayOfInterface(item)
			Expect(err).To(BeEqualTo(ErrInvalidSliceType))
			Expect(ret).To(BeNil())
		})
	})
})
