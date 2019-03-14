package djoemo_test

import (
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

const ContextFields string = "ContextFields"

func TestDynamo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dynamo Suite")
}

// User model with hash key only
type User struct {
	UUID      string
	UserName  string
	Meta      map[string]string
	UpdatedAt time.Time
	CreatedAt time.Time
	TraceID   string
}

// Profile model with hash and range
type Profile struct {
	UUID      string
	Email     string
	UserName  string
	UpdatedAt time.Time
	CreatedAt time.Time
	TraceID   string
}

// WithFields returns context with fields
func WithFields(fields map[string]interface{}) context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, ContextFields, fields)
}
