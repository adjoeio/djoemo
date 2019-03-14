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
