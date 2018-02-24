package stats

import (
	"net/http"
	"time"
	"github.com/golang/glog"
)

type CountingResponseWriter struct {
	writer        http.ResponseWriter
	bytesWritten  int64
	firstByteTime time.Time
}

func NewCountingWriter(writer http.ResponseWriter) *CountingResponseWriter {
	return &CountingResponseWriter{
		writer: writer,
	}
}

func (crw *CountingResponseWriter) Header() http.Header {
	return crw.writer.Header()
}

func (crw *CountingResponseWriter) WriteHeader(status int) {
	crw.writer.WriteHeader(status)
}

func (crw *CountingResponseWriter) Write(b []byte) (int, error) {
	bytesWritten, error := crw.writer.Write(b)
	glog.Errorf("writing %v %v", crw.bytesWritten, len(b))
	if crw.bytesWritten == 0 && len(b) != 0 {
		crw.firstByteTime = time.Now()
		glog.Errorf("first byte written")
		if flusher, ok := crw.writer.(http.Flusher) ; ok {
			glog.Errorf("flushing")
			flusher.Flush()
		}
	}
	crw.bytesWritten += int64(bytesWritten)
	return bytesWritten, error
}

func (crw *CountingResponseWriter) BytesWritten() int64 {
	return crw.bytesWritten
}

func (crw *CountingResponseWriter) FirstByteTime() time.Time {
	return crw.firstByteTime
}
