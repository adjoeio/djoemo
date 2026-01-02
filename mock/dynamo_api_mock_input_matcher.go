package mock

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onsi/gomega"
)

/*
InputMatcher matcher to match only specified fields and not pass all fields to mock
## Usage
matcher := mocks.InputExpect().

	FieldEq("FIELD_NAME", "FIELD_VALUE").
	FieldEq("FIELD_NAME_1", "FIELD_VALUE_1"),

dynamoDBMock := mocks.NewMockDynamoDBAPI(mockCtrl)
dynamoDBMock.EXPECT().PutItem()

## usage with dynamomock helper
dynamoMock := mocks.NewMockDynamoDBAPI(mockCtrl)
dmock := mocks.NewDynamoMock(dynamoMock)

	dmock.Should().
			SaveItem(
				dmock.WithTable("TABLENAME"),
				dmock.WithMatch(
					mocks.InputExpect().
						FieldEq("FIELD_NAME", "FIELD_VALUE").
						FieldEq("FIELD_NAME_1", "FIELD_VALUE_1"),
				),
			).Exec()
*/
type InputMatcher struct {
	Fields    map[string]interface{}
	TableName string
}

// Matches match registered field with actual value mock received
func (i InputMatcher) Matches(x interface{}) bool {
	switch x := x.(type) {
	case *dynamodb.PutItemInput:
		return i.matchPutItemInput(x)
	case *dynamodb.UpdateItemInput:
		return i.matchUpdateItemInput(x)
	}
	return false
}

func (i InputMatcher) String() string {
	return ""
}

// InputExpect init matcher with empty fields
func InputExpect() *InputMatcher {
	i := &InputMatcher{}
	i.Fields = make(map[string]interface{})
	return i
}

// FieldEq add field to be matched by dynamodb mock
func (i *InputMatcher) FieldEq(name string, value interface{}) *InputMatcher {
	i.Fields[name] = value
	return i
}

func (i *InputMatcher) matchPutItemInput(x interface{}) bool {
	inputItem := x.(*dynamodb.PutItemInput)
	inputFields := make(map[string]interface{})
	dynamodbattribute.UnmarshalMap(inputItem.Item, &inputFields)

	// hack to make sure numric values has the same casting
	marshalfields, _ := dynamodbattribute.MarshalMap(i.Fields)
	fields := make(map[string]interface{})
	dynamodbattribute.UnmarshalMap(marshalfields, &fields)
	for ik, iv := range fields {
		// if both values are nil pointers, we check different
		if (inputFields[ik] == nil || (reflect.ValueOf(inputFields[ik]).Kind() == reflect.Ptr && reflect.ValueOf(inputFields[ik]).IsNil())) &&
			(iv == nil || (reflect.ValueOf(iv).Kind() == reflect.Ptr && reflect.ValueOf(iv).IsNil())) {
			gomega.Expect(inputFields[ik]).To(gomega.BeNil())
		} else {
			gomega.Expect(inputFields[ik]).To(gomega.BeEquivalentTo(iv))
		}
	}
	return true
}

func (i *InputMatcher) matchUpdateItemInput(x interface{}) bool {
	inputItem := x.(*dynamodb.UpdateItemInput)
	// remove spaces & Set & if_not_exists(Field
	reg := regexp.MustCompile(`if_not_exists|\(([^ ]+)|\)|SET|ADD| `)
	updateExpression := reg.ReplaceAllString(*inputItem.UpdateExpression, "")
	updateExpressions := strings.Split(updateExpression, ",")
	fieldsValues := make(map[string]interface{})
	expressionAttributeValues := make(map[string]interface{})
	dynamodbattribute.UnmarshalMap(inputItem.ExpressionAttributeValues, &expressionAttributeValues)

	for _, expression := range updateExpressions {
		var keyValue []string
		if strings.ContainsRune(expression, '=') {
			keyValue = strings.Split(expression, "=")
		} else {
			keyValue = strings.Split(expression, ":")
			if len(keyValue) > 0 {
				keyValue[1] = fmt.Sprintf(":%s", keyValue[1])
			}
		}
		fieldsValues[keyValue[0]] = expressionAttributeValues[keyValue[1]]
	}

	// hack to make sure numric values has the same casting
	marshalFields, _ := dynamodbattribute.MarshalMap(i.Fields)
	fields := make(map[string]interface{})
	dynamodbattribute.UnmarshalMap(marshalFields, &fields)
	for ik, iv := range fields {
		gomega.Expect(fieldsValues[ik]).Should(gomega.BeEquivalentTo(iv))
	}
	return true
}
