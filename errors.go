package djoemo

import "errors"

//ErrInvalidTableName table name is invalid error
var ErrInvalidTableName = errors.New("invalid table name")

//ErrInvalidHashKeyName hash key name is invalid error
var ErrInvalidHashKeyName = errors.New("invalid hash key name")

//ErrInvalidHashKeyValue hash key value is invalid error
var ErrInvalidHashKeyValue = errors.New("invalid hash key value")

//ErrNoItemFound item not found error
var ErrNoItemFound = errors.New("no item found")

//ErrInvalidSliceType interface should be slice error
var ErrInvalidSliceType = errors.New("invalid type expected slice")
