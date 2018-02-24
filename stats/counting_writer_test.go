package stats

import (
	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
	"net/http"
)

var _ = Describe("CountingWriter", func() {
	Describe("write", func() {
		It("should count bytes and remember first byte time", func() {
			bf := &bufferedFlusher{}
			crw := NewCountingWriter(bf)
			crw.Write([]byte("hi!"))
			Expect(bf.flushed).To(Equal(true))
			Expect(int(crw.BytesWritten())).To(Equal(3))
			time.Sleep(1*time.Second)
			crw.Write([]byte("bye!"))
			Expect(int(crw.BytesWritten())).To(Equal(7))
			end := time.Now()
			Expect(end.Sub(crw.FirstByteTime()).Seconds()).To(BeNumerically(">", .5))
		})
	})
})

type bufferedFlusher struct {
	flushed bool
	buf bytes.Buffer
	header http.Header
}

func (b *bufferedFlusher) Write(p []byte) (nn int, err error) {
	return b.buf.Write(p)
}

func (b *bufferedFlusher) Flush() {
	b.flushed = true
}

func (b *bufferedFlusher) Header() http.Header {
	return b.header
}

func (b *bufferedFlusher) WriteHeader(r int) {

}