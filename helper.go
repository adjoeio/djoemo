package djoemo

import (
	"github.com/guregu/dynamo"
)

func valueFromPtr[T any](ptr *T) T {
	if ptr == nil {
		var v T
		return v
	}

	return *ptr
}

func buildTableKeyCondition(table dynamo.Table, key KeyInterface) *dynamo.Query {
	q := table.Get(*key.HashKeyName(), key.HashKey())

	// by range
	if key.RangeKeyName() != nil && key.RangeKey() != nil {
		strVal, ok := key.RangeKey().(string)
		if ok && strVal == "" {
			return q
		}

		q = q.Range(*key.RangeKeyName(), dynamo.Equal, key.RangeKey())
	}

	return q
}
