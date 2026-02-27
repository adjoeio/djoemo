package djoemo

// QueryInterface provides an interface for djoemo query used to query item in djoemo table
type QueryInterface interface {
	KeyInterface
	RangeOp() Operator
	Limit() *int64
	Descending() bool
}
