package djoemo

import "github.com/guregu/dynamo"

type UpdateOption func(update *dynamo.Update)

func WithCondition(conditionExpression string, conditionArgs ...any) func(update *dynamo.Update) {
	return func(update *dynamo.Update) {
		if update == nil {
			update = &dynamo.Update{}
		}

		update.If(conditionExpression, conditionArgs...)
	}
}
