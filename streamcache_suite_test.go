package streamcache

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTTS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TTS Suite")
}
