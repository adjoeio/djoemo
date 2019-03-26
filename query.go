package djoemo

// Operator is an operation to apply comparisons.
type Operator string

// Operators used for comparing against the range key.
const (
	Equal          Operator = "EQ"
	NotEqual                = "NE"
	Less                    = "LT"
	LessOrEqual             = "LE"
	Greater                 = "GT"
	GreaterOrEqual          = "GE"
	BeginsWith              = "BEGINS_WITH"
	Between                 = "BETWEEN"
)

type query struct {
	key
	rangeOp Operator
}

// Key factory method to create struct that implements key interface
func Query() *query {
	return &query{}
}

// WithTableName set djoemo key table name
func (q *query) WithTableName(tableName string) *query {
	q.tableName = tableName
	return q
}

// WithHashKeyName set djoemo key hash key name
func (q *query) WithHashKeyName(hashKeyName string) *query {
	q.hashKeyName = &hashKeyName
	return q
}

// WithRangeKeyName set djoemo key range key name
func (q *query) WithRangeKeyName(rangeKeyName string) *query {
	q.rangeKeyName = &rangeKeyName
	return q
}

// WithHashKey set djoemo key hash key value
func (q *query) WithHashKey(hashKey interface{}) *query {
	q.hashKey = hashKey
	return q
}

// WithRangeKey set djoemo key range key value
func (q *query) WithRangeKey(rangeKey interface{}) *query {
	q.rangeKey = rangeKey
	return q
}

// WithRangeKey set djoemo key range key value
func (q *query) WithRangeOp(rangeOp Operator) *query {
	q.rangeOp = rangeOp
	return q
}

// RangeRangeOperator returns the operator used for comparing against the range key
func (q *query) RangeOp() Operator {
	if q.rangeOp == "" {
		return Equal
	}
	return q.rangeOp
}
