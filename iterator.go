package djoemo

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
)

// IteratorInterface ...
type IteratorInterface interface {
	// NextItem unmarshals the next item into out and returns if there are more items following
	NextItem(out interface{}) bool
}

// Iterator ...
type Iterator struct {
	scan             *dynamo.Scan
	tableName        string
	searchLimit      int64
	lastEvaluatedKey map[string]*dynamodb.AttributeValue
	iterator         dynamo.PagingIter
	ctx              context.Context
}

func (itr *Iterator) NextItem(out interface{}) bool {
	more := itr.iterator.NextWithContext(itr.ctx, out)
	if !more && itr.iterator.LastEvaluatedKey() != nil {
		itr.scan = itr.scan.StartFrom(itr.iterator.LastEvaluatedKey())
		itr.iterator = itr.scan.Iter()
		return itr.iterator.NextWithContext(itr.ctx, out)
	}
	return more
}

// QueryIterator ...
type QueryIterator struct {
	query            *dynamo.Query
	tableName        string
	searchLimit      int64
	lastEvaluatedKey map[string]*dynamodb.AttributeValue
	iterator         dynamo.PagingIter
	ctx              context.Context
}

func (itr *QueryIterator) NextItem(out interface{}) bool {
	more := itr.iterator.NextWithContext(itr.ctx, out)
	if !more && itr.iterator.LastEvaluatedKey() != nil {
		itr.query = itr.query.StartFrom(itr.iterator.LastEvaluatedKey())
		itr.iterator = itr.query.Iter()
		return itr.iterator.NextWithContext(itr.ctx, out)
	}
	return more
}
