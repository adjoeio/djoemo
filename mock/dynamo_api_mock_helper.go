package mock

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/mock/gomock"
	"github.com/guregu/dynamo/v2"
)

// DynamoMock wrapper for dynamodb mock support configuration function
type DynamoMock struct {
	DynamoDBAPIMock           *MockDynamoDBAPI
	TableName                 string
	Hash                      map[string]types.AttributeValue
	Range                     map[string]types.AttributeValue
	Index                     string
	GetOutput                 *dynamodb.GetItemOutput
	QueryOutput               *dynamodb.QueryOutput
	ScanAllOutput             *dynamodb.ScanOutput
	Input                     *dynamodb.PutItemInput
	DeleteItemInput           *dynamodb.DeleteItemInput
	Inputs                    *dynamodb.BatchWriteItemInput
	DeleteInputs              *dynamodb.BatchWriteItemInput
	BatchWriteOutput          *dynamodb.BatchWriteItemOutput
	BatchGetKeys              []map[string]interface{}
	BatchGetOutput            *dynamodb.BatchGetItemOutput
	Err                       error
	Times                     int
	Calls                     []call
	Conditions                map[string]types.Condition
	InputMatcher              gomock.Matcher
	Limit                     int64
	Desc                      bool
	ConditionExpression       *string
	ExpressionAttributeValues map[string]types.AttributeValue
}

// NewDynamoMock Factory for DynamoMock wrapper
func NewDynamoMock(DynamoDBAPIMock *MockDynamoDBAPI) DynamoMock {
	return DynamoMock{
		DynamoDBAPIMock: DynamoDBAPIMock,
	}
}

// DynamoDBOption type of configeration function
type DynamoDBOption func(*DynamoMock)

// Should return pointer to DynamoMock
func (d *DynamoMock) Should() *DynamoMock {
	d.Conditions = nil
	d.Desc = false
	d.Limit = 0
	d.InputMatcher = &InputMatcher{}
	d.Range = make(map[string]types.AttributeValue)
	return d
}

// Get register call for DynamoMock GetItemWithContext with its option
func (d *DynamoMock) Get(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	// todo use with error func in func scope
	var err error
	if d.GetOutput == nil {
		err = dynamo.ErrNotFound
	}
	if d.Err != nil {
		err = d.Err
	}
	return d.addCall("GetItem", d.getItemInput(), d.GetOutput, err)
}

// Query register call for DynamoMock Query with its option
func (d *DynamoMock) Query(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	// todo use with error func in func scope
	var err error
	if d.QueryOutput == nil {
		err = dynamo.ErrNotFound
	}
	if d.Err != nil {
		err = d.Err
	}
	return d.addCall("Query", d.queryInput(), d.QueryOutput, err)
}

// ScanAll ...
func (d *DynamoMock) ScanAll(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	// todo use with error func in func scope
	var err error
	if d.ScanAllOutput == nil {
		err = dynamo.ErrNotFound
	}
	return d.addCall("Scan", d.scanInput(), d.ScanAllOutput, err)
}

// Save register call for DynamoMock PutItemWithContext with its option
func (d *DynamoMock) Save(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}

	return d.addCall("PutItem", d.InputMatcher, nil, d.Err)
}

// Update register call for DynamoMock PutItemWithContext with its option
func (d *DynamoMock) Update(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}

	return d.addCall("UpdateItem", d.InputMatcher, nil, d.Err)
}

func (d *DynamoMock) SaveAll(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	return d.addCall("BatchWriteItem", d.InputMatcher, d.BatchWriteOutput, d.Err)
}

func (d *DynamoMock) GetAll(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	return d.addCall("BatchGetItemWithContext", d.batchGetInput(), d.BatchGetOutput, nil)
}

func (d *DynamoMock) Delete(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	return d.addCall("DeleteItem", d.DeleteItemInput, &dynamodb.DeleteItemOutput{}, d.Err)
}

func (d *DynamoMock) DeleteAll(opts ...DynamoDBOption) *DynamoMock {
	for _, opt := range opts {
		opt(d)
	}
	return d.addCall("BatchWriteItem", d.DeleteInputs, d.BatchWriteOutput, d.Err)
}

// WithHash register option hash key and value
func (d *DynamoMock) WithHash(key string, value interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Hash = map[string]types.AttributeValue{
			key: getAttributeValue(value),
		}
	}
}

// WithRange register option range key and value
func (d *DynamoMock) WithRange(key string, value interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Range = map[string]types.AttributeValue{
			key: getAttributeValue(value),
		}
	}
}

// WithIndex register option range key and value
func (d *DynamoMock) WithIndex(name string) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Index = name
	}
}

// WithLimit register option limit
func (d *DynamoMock) WithLimit(limit int64) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Limit = limit
	}
}

func (d *DynamoMock) WithGetKeys(keys []map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		args.BatchGetKeys = keys
	}
}

// WithDesc register option limit
func (d *DynamoMock) WithDesc(desc bool) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Desc = desc
	}
}

// WithInput register option dynamodb PutItemInput
func (d *DynamoMock) WithInput(value map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		av, _ := attributevalue.MarshalMap(value)
		args.Input = &dynamodb.PutItemInput{
			Item:         av,
			TableName:    aws.String(d.TableName),
			ReturnValues: types.ReturnValueNone,
		}
		if d.ConditionExpression != nil {
			args.Input.ConditionExpression = d.ConditionExpression
			args.Input.ExpressionAttributeValues = d.ExpressionAttributeValues
		}
		args.InputMatcher = gomock.Eq(args.Input)
	}
}

// WithInput register option dynamodb PutItemInput
func (d *DynamoMock) WithDeleteInput(value map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		av, _ := attributevalue.MarshalMap(value)
		args.DeleteItemInput = &dynamodb.DeleteItemInput{
			Key:          av,
			TableName:    aws.String(d.TableName),
			ReturnValues: types.ReturnValueNone,
		}
	}
}

func (d *DynamoMock) WithInputs(values []map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		var writeRequestArray []types.WriteRequest
		for _, v := range values {
			av, _ := attributevalue.MarshalMap(v)
			writeRequest := types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: av,
				},
			}
			writeRequestArray = append(writeRequestArray, writeRequest)
		}
		// size := strconv.Itoa(len(values))
		requestItems := make(map[string][]types.WriteRequest)
		requestItems[d.TableName] = writeRequestArray
		args.Inputs = &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}

		if len(values) == 0 {
			args.Inputs = &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{},
			}
		}

		args.InputMatcher = gomock.Eq(args.Inputs)
		// return all is processed
		args.BatchWriteOutput = &dynamodb.BatchWriteItemOutput{}
	}
}

func (d *DynamoMock) WithDeleteInputs(values []map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		var writeRequestArray []types.WriteRequest
		for _, v := range values {
			av, _ := attributevalue.MarshalMap(v)
			writeRequest := types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: av,
				},
			}
			writeRequestArray = append(writeRequestArray, writeRequest)
		}
		// size := strconv.Itoa(len(values))
		requestItems := make(map[string][]types.WriteRequest)
		requestItems[d.TableName] = writeRequestArray
		args.DeleteInputs = &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}

		args.InputMatcher = gomock.Eq(args.DeleteInputs)
		// return all is processed
		args.BatchWriteOutput = &dynamodb.BatchWriteItemOutput{}
	}
}

// WithMatch register option dynamodb PutItemInput
func (d *DynamoMock) WithMatch(m gomock.Matcher) DynamoDBOption {
	return func(args *DynamoMock) {
		args.InputMatcher = m
	}
}

// WithCondition register option dynamodb GetItemOutput
func (d *DynamoMock) WithCondition(field string, value interface{}, operator string) DynamoDBOption {
	return func(args *DynamoMock) {
		if d.Conditions == nil {
			d.Conditions = make(map[string]types.Condition)
		}
		list := make([]interface{}, 1)
		list[0] = value
		l, _ := attributevalue.MarshalList(list)
		args.Conditions[field] = types.Condition{
			AttributeValueList: l,
			ComparisonOperator: types.ComparisonOperator(operator),
		}
	}
}

// WithConditionExpression register option dynamodb GetItemOutput
func (d *DynamoMock) WithConditionExpression(expression string, value interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		d.ExpressionAttributeValues = make(map[string]types.AttributeValue)
		expressionAttributeValueField := ":v0"
		expression = strings.Replace(expression, "?", expressionAttributeValueField, 1)
		av, _ := attributevalue.Marshal(value)
		d.ExpressionAttributeValues[expressionAttributeValueField] = av
		d.ConditionExpression = &expression
	}
}

// WithQueryOutput register option dynamodb GetItemOutput
func (d *DynamoMock) WithQueryOutput(value interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		if value == nil {
			args.QueryOutput = nil
			return
		}

		items := []map[string]types.AttributeValue{}
		// if input value is map
		kind := reflect.ValueOf(value).Kind()
		if kind == reflect.Map {
			av, _ := attributevalue.MarshalMap(value)
			items = append(items, av)
		}
		// if input value list of maps
		if kind == reflect.Array || kind == reflect.Slice {
			value := value.([]map[string]interface{})
			for _, v := range value {
				av, _ := attributevalue.MarshalMap(v)
				items = append(items, av)

			}
		}

		args.QueryOutput = &dynamodb.QueryOutput{
			Items: items,
			Count: int32(len(items)),
		}
	}
}

// WithGetAllOutput register option dynamodb
func (d *DynamoMock) WithGetAllOutput(value []map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		if value == nil {
			args.BatchGetOutput = nil
			return
		}

		items := []map[string]types.AttributeValue{}

		for _, v := range value {
			av, _ := attributevalue.MarshalMap(v)
			items = append(items, av)

		}

		args.BatchGetOutput = &dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]types.AttributeValue{
				d.TableName: items,
			},
		}
	}
}

// WithGetOutput register option dynamodb GetItemOutput
func (d *DynamoMock) WithGetOutput(value map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		if value == nil {
			args.GetOutput = nil
			return
		}
		av, _ := attributevalue.MarshalMap(value)
		args.GetOutput = &dynamodb.GetItemOutput{
			Item: av,
		}
	}
}

// WithScanAllOutput ...
func (d *DynamoMock) WithScanAllOutput(value []map[string]interface{}) DynamoDBOption {
	return func(args *DynamoMock) {
		if value == nil {
			args.ScanAllOutput = nil
			return
		}

		var av []map[string]types.AttributeValue
		for _, v := range value {
			dv, _ := attributevalue.MarshalMap(v)
			av = append(av, dv)
		}

		args.ScanAllOutput = &dynamodb.ScanOutput{
			Items: av,
		}
	}
}

// WithTable register option dynamodb table name
func (d *DynamoMock) WithTable(name string) DynamoDBOption {
	return func(args *DynamoMock) {
		args.TableName = name
	}

}

// WithError register error to call note its on mock scope
func (d *DynamoMock) WithError(err error) DynamoDBOption {
	return func(args *DynamoMock) {
		args.Err = err
	}
}

// Exec execute all registered calls with its options
func (d *DynamoMock) Exec() *DynamoMock {
	m := d.DynamoDBAPIMock.EXPECT()
	for _, v := range d.Calls {
		if v.input != nil {
			Invoke(m, v.method, gomock.Any(), v.input).Return(v.output, v.err).AnyTimes()
		} else {
			Invoke(m, v.method, gomock.Any()).Return(v.output, v.err).AnyTimes()
		}
	}
	return d
}

// getItemInput return query input from registered hash , range and table name
func (d *DynamoMock) getItemInput() *dynamodb.GetItemInput {
	for k, v := range d.Range {
		d.Hash[k] = v
	}
	req := &dynamodb.GetItemInput{
		TableName: aws.String(d.TableName),
		Key:       d.Hash,
	}
	return req
}

// getItemInput return query input from registered hash , range and table name
func (d *DynamoMock) queryInput() *dynamodb.QueryInput {
	req := &dynamodb.QueryInput{
		TableName:     aws.String(d.TableName),
		KeyConditions: d.Conditions,
	}
	if d.Index != "" {
		req.IndexName = aws.String(d.Index)
	}
	if d.Limit != 0 {
		req.Limit = aws.Int32(int32(d.Limit))
	}
	if d.Desc {
		req.ScanIndexForward = aws.Bool(false)
	}

	return req
}

// getItemInput return query input from registered hash , range and table name
func (d *DynamoMock) batchGetInput() *dynamodb.BatchGetItemInput {
	var keys []map[string]types.AttributeValue
	for _, value := range d.BatchGetKeys {
		for k, v := range value {
			keyvalue, _ := attributevalue.Marshal(v)
			key := map[string]types.AttributeValue{
				k: keyvalue,
			}
			keys = append(keys, key)
		}

	}
	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			d.TableName: {
				ConsistentRead: aws.Bool(false),
				Keys:           keys,
			},
		},
	}

	return req
}

func (d *DynamoMock) scanInput() *dynamodb.ScanInput {
	b := false
	req := &dynamodb.ScanInput{
		TableName:      aws.String(d.TableName),
		ConsistentRead: &b,
	}
	if d.Limit != 0 {
		req.Limit = aws.Int32(int32(d.Limit))
	}
	return req
}

// addCall register mock method call with its input and output
func (d *DynamoMock) addCall(method string, input interface{}, output interface{}, err interface{}) *DynamoMock {
	c := call{
		method: method,
		input:  input,
		output: output,
		err:    err,
	}
	d.Calls = append(d.Calls, c)
	return d
}

// getAttributeValue return dynamodb.AttributeValue from interface type
func getAttributeValue(value interface{}) types.AttributeValue {
	var attributeValue types.AttributeValue
	switch value.(type) {
	case string:
		attributeValue = &types.AttributeValueMemberS{
			Value: value.(string),
		}
	case int, int8, int16, int32, int64:
		attributeValue = &types.AttributeValueMemberS{
			Value: strconv.Itoa(value.(int)),
		}
	}

	return attributeValue
}

// Invoke reflection function to call registered methods
func Invoke(any interface{}, name string, args ...interface{}) *gomock.Call {
	inputs := make([]reflect.Value, len(args))
	for i := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}
	method := reflect.ValueOf(any).MethodByName(name)

	result := method.Call(inputs)
	return result[0].Interface().(*gomock.Call)
}

type call struct {
	method string
	input  interface{}
	output interface{}
	err    interface{}
}
