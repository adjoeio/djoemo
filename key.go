package djoemo

type key struct {
	tableName    string
	index        *string
	hashKeyName  *string
	rangeKeyName *string
	hashKey      interface{}
	rangeKey     interface{}
}

// Key factory method to create struct that implements key interface
func Key() *key {
	return &key{}
}

// WithTableName set djoemo key table name
func (k *key) WithTableName(tableName string) *key {
	k.tableName = tableName
	return k
}

// WithIndex set djoemo index
func (k *key) WithIndex(index *string) *key {
	k.index = index
	return k
}

// WithHashKeyName set djoemo key hash key name
func (k *key) WithHashKeyName(hashKeyName string) *key {
	k.hashKeyName = &hashKeyName
	return k
}

// WithRangeKeyName set djoemo key range key name
func (k *key) WithRangeKeyName(rangeKeyName string) *key {
	k.rangeKeyName = &rangeKeyName
	return k
}

// WithHashKey set djoemo key hash key value
func (k *key) WithHashKey(hashKey interface{}) *key {
	k.hashKey = hashKey
	return k
}

// WithRangeKey set djoemo key range key value
func (k *key) WithRangeKey(rangeKey interface{}) *key {
	k.rangeKey = rangeKey
	return k
}

// TableName returns the djoemo table name
func (k *key) TableName() string {
	return k.tableName
}

// Index returns the djoemo index
func (k *key) Index() *string {
	return k.index
}

// HashKeyName returns the name of hash key if exists
func (k *key) HashKeyName() *string {
	return k.hashKeyName
}

// RangeKeyName returns the name of range key if exists
func (k *key) RangeKeyName() *string {
	return k.rangeKeyName
}

// HashKey returns the hash key value
func (k *key) HashKey() interface{} {
	return k.hashKey
}

// HashKey returns the range key value
func (k *key) RangeKey() interface{} {
	return k.rangeKey
}

func isValidKey(key KeyInterface) error {
	if key.TableName() == "" {
		return ErrInvalidTableName
	}
	if key.HashKeyName() == nil {
		return ErrInvalidHashKeyName
	}
	if key.HashKey() == nil {
		return ErrInvalidHashKeyValue
	}

	return nil
}
