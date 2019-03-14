package djoemo

import (
	"reflect"
)

//InterfaceToArrayOfInterface transforms interface of slice to slice of interfaces
func InterfaceToArrayOfInterface(sliceOfItems interface{}) ([]interface{}, error) {
	s := reflect.ValueOf(sliceOfItems)
	if s.Kind() != reflect.Slice {
		return nil, ErrInvalidSliceType
	}

	items := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		items[i] = s.Index(i).Interface()
	}
	if len(items) == 0 {
		return nil, nil
	}

	return items, nil
}
