package djoemo

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TimeFormatStandard is a mysql time.Time type with some helper functions
const TimeFormatStandard = "2006-01-02T15:04:05.000Z07:00"

// TenYears ...
const TenYears = time.Duration(time.Hour * 24 * 365 * 10)

// RFC3339Milli with millisecond precision
const RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"

// MonthFormat is the format for a month
const MonthFormat = "2006-01"

// DayFormat is the format for a day
const DayFormat = "2006-01-02"

// DayHourFormat is the format for a day
const DayHourFormat = "2006-01-02 15"

// DjoemoTime ...
type DjoemoTime struct {
	time.Time
}

// Date returns the Time corresponding to
//
//	yyyy-mm-dd hh:mm:ss + nsec nanoseconds
func Date(year int, month time.Month, day, hour, min, sec, nsec int, loc *time.Location) DjoemoTime {
	return DjoemoTime{Time: time.Date(year, month, day, hour, min, sec, nsec, loc)}
}

// UnmarshalJSON checks before unmarshal it if a json timestamp is typical adjoe one format
func (dt *DjoemoTime) UnmarshalJSON(p []byte) error {
	t, err := time.Parse(TimeFormatStandard, strings.Replace(
		string(p),
		"\"",
		"",
		-1,
	))
	if err != nil {
		return err
	}

	dt.Time = t

	return nil
}

// MarshalJSON ...
func (dt DjoemoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(dt.Time.Format(TimeFormatStandard))
}

// MarshalDynamoDBAttributeValue ...
func (dt *DjoemoTime) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	unix := int64(0)
	if dt != nil {
		unix = dt.UnixNano()
	}
	// workarround:
	// https://github.com/golang/go/issues/19486
	time.Local = nil

	// safe only >= 0 values to dynamodb, because its unixtime
	if unix <= 0 {
		unix = 0
	}

	s := strconv.FormatInt(unix, 10)

	av.N = &s
	return nil
}

// UnmarshalDynamoDBAttributeValue ...
func (dt *DjoemoTime) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	// todo: workarround:
	// https://github.com/golang/go/issues/19486
	// unix() does set local not to nil "!t(*time.Location=<nil>)}}", but parse does...
	// causes error while doing equal on object
	time.Local = nil

	if av.N == nil {
		return nil
	}

	n, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}

	// init zero time, if unixtime is not valid or random
	if n <= 0 {
		dt.Time = time.Time{}
		return nil
	}

	dt.Time = time.Unix(0, n)
	dt.Time = dt.Time.Round(0)

	return nil
}

// Now returns the current local time.
var Now = func() DjoemoTime {
	t := time.Now()
	// solves docker issue:
	// https://forum.golangbridge.org/t/nanosecond-timestamp-precision-not-playing-well-in-containers/6663/4
	t = t.Round(0)
	return DjoemoTime{Time: t}
}
