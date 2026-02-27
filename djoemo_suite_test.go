package djoemo_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDjoemo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Djoemo Suite")
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
