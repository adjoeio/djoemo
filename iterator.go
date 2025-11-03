package djoemo

import (
	"context"
	"fmt"

	"github.com/guregu/dynamo/v2"
)

// IteratorInterface ...
type IteratorInterface interface {
	NextItem(out interface{}) (bool, error)
}

// Iterator ...
type Iterator struct {
	scan        *dynamo.Scan
	tableName   string
	searchLimit int64
	iterator    dynamo.PagingIter
	ctx         context.Context
}

// NextItem unmarshals the next item into out and returns if there are more items following
func (itr *Iterator) NextItem(out interface{}) (bool, error) {
	more := itr.iterator.Next(itr.ctx, out)
	err := itr.iterator.Err()
	if !more && err == nil {
		pagingKey, pagingKeyErr := itr.iterator.LastEvaluatedKey(itr.ctx)
		if pagingKeyErr != nil {
			return false, fmt.Errorf("failed to get last evaluated key: %w", pagingKeyErr)
		}

		if pagingKey == nil {
			return false, nil
		}

		itr.scan = itr.scan.StartFrom(pagingKey)
		itr.iterator = itr.scan.Iter()

		more = itr.iterator.Next(itr.ctx, out)
		err = itr.iterator.Err()
	}

	return more, err
}
