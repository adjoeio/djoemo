package djoemo

type UpdateExpression string

// Set changes path to the given value.
const Set UpdateExpression = "Set"

// SetSet changes a set at the given path to the given value.
const SetSet UpdateExpression = "SetSet"

// SetIfNotExists changes path to the given value, if it does not already exist.
const SetIfNotExists UpdateExpression = "SetIfNotExists"

// SetExpr performs a custom set expression, substituting the args into expr as in filter expressions.
const SetExpr UpdateExpression = "SetExpr"

// Add increments the path value in case of a number, or in case of a set it appends to that set.
// If a prior value doesn't exist it will set the path to that value.
const Add UpdateExpression = "ADD"

// UpdateExpressions is a type alias used for specifiyng multiple
// update expressions at once
type UpdateExpressions map[UpdateExpression]map[string]interface{}
