package djoemo

// Key provides an interface for djoemo key used to identify item in djoemo table
type KeyInterface interface {
	// TableName returns the djoemo table name
	TableName() string
	// HashKeyName returns the name of hash key if exists
	HashKeyName() *string
	// RangeKeyName returns the name of range key if exists
	RangeKeyName() *string
	// HashKey returns the hash key value
	HashKey() any
	// HashKey returns the range key value
	RangeKey() any
}
