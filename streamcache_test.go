package streamcache

import (
	"bytes"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/golang/glog"
	"time"
	"math/rand"
	"context"
	"errors"
	"io"
)

var _ = Describe("StreamCache", func() {
	Describe(".Put/.Get", func() {
		It("should should consistently get a stream with concurrency", func() {

			cache := New(1000, 3600*time.Second)
			done := make(chan error)
			expectedBytes := 100000
			readers := 10
			writers := 2
			readerLoops := 1000
			writerLoops := 100

			for i := 0 ; i < writers ; i++ {
				glog.Errorf("starting writer")
				go func(w int) {
					result := error(nil)
					for i := 0 ; i < writerLoops ; i++ {
						glog.Errorf("starting writer %d loop %d", w, i)
						stream := &randomWaitingReader{
							bytesRemaining: expectedBytes,
							maxDelay: 10*time.Millisecond,
							metadata: map[interface{}]interface{}{
								"Content-Type": "application/xml",
								"Status": 200,
							},
						}
						sce := cache.Put("asdf", stream, context.TODO())
						var buf bytes.Buffer
						bytes, err := sce.Copy(&buf)

						if err != nil {
							result = err
							break
						}

						if int(bytes) != expectedBytes {
							result = errors.New(fmt.Sprintf("expected %d but got %v bytes", expectedBytes, bytes))
							break
						}
					}
					glog.Errorf("writer error %v", result)
					done <- result
				}(i)
			}

			time.Sleep(1*time.Second)

			for i := 0 ; i < readers ; i++ {
				glog.Errorf("starting reader")
				go func(r int) {
					result := error(nil)
					for j := 0 ; j < readerLoops ; j++ {
						glog.Errorf("starting reader %d loop %d", r, i)
						sce := cache.Get("asdf")
						if sce == nil {
							result = errors.New("couldn't get cache entry")
							break
						}

						var buf bytes.Buffer
						bytes, err := sce.Copy(&buf)

						if err != nil {
							result = err
							break
						}

						if int(bytes) != expectedBytes {
							result = errors.New(fmt.Sprintf("expected %d but got %v bytes", expectedBytes, bytes))
							break
						}
					}
					glog.Errorf("reader error %v", result)
					done <- result
				}(i)
			}

			for i := 0 ; i < readers + writers ; i++ {
				Expect(<-done).To(BeNil())
			}
		})
	})
})

type randomWaitingReader struct {
	bytesRemaining int
	maxDelay time.Duration
	metadata map[interface{}]interface{}
}

func (rwr *randomWaitingReader) Read(p []byte) (int, error) {
	rwr.delay()

	if rwr.bytesRemaining <= 0 {
		return 0, io.EOF
	}

	count := min(len(p), rwr.bytesRemaining)
	rwr.bytesRemaining -= count
	glog.Errorf("rwr read: returning %d nil from read with %d remaining", count, rwr.bytesRemaining)

	return count, nil 
}

func (rwr *randomWaitingReader) GetMetadata() (map[interface{}]interface{}, error) {
	rwr.delay()

	return rwr.metadata, nil
}

func (rwr *randomWaitingReader) delay() {
	r := rand.Float32()
	delay := time.Duration(r * float32(rwr.maxDelay.Nanoseconds()))
	time.Sleep(delay)
}