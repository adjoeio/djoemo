package djoemo

import "github.com/adjoeio/djoemo/model"

// QueryInterface provides an interface for djoemo query used to query item in djoemo table
type QueryInterface interface {
	model.Key
	RangeOp() Operator
	Limit() *int64
	Descending() bool
}
