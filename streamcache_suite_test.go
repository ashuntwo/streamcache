package streamcache

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestStreamCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "StreamCache Suite")
}
