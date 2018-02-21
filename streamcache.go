package streamcache

import (
	"io"
	"time"
	"sync"
	"context"
	lru "github.com/hashicorp/golang-lru"
	"github.com/golang/glog"
)

type ReaderWithMetadata interface {
	GetMetadata() (map[interface{}]interface{}, error)
	io.Reader
}

type status int

const (
	status_inprogress status = iota
	status_metadata_read = iota
	status_complete = iota
	status_error = iota
)

const INITIAL_SIZE = 50000

type StreamCacheEntry struct {
	metadata map[interface{}]interface{}
	status status
	error error
	lock *sync.RWMutex
	cond *sync.Cond
	buffer []byte
	expiresAt time.Time
}

type StreamCache struct {
	cache *lru.Cache
	expiry time.Duration
}

type SCEReader struct {
	sce *StreamCacheEntry
	status status
	nextOffset int
}

func New(size int, expiry time.Duration) *StreamCache {
	cache, _ := lru.New(size)
	return &StreamCache{
		cache: cache,
		expiry: expiry,
	}
}

func (sce *StreamCacheEntry) Copy(dst io.Writer) (int64, error) {
	reader := &SCEReader{
		sce: sce,
	}

	cnt, err := io.Copy(dst, reader)
	glog.Errorf("copying returned %v %v", cnt, err)
	return cnt, err
}

func (sc *StreamCache) Get(key interface{}) *StreamCacheEntry {
	sceRaw, ok := sc.cache.Get(key)

	if !ok {
		return nil
	}

	sce := sceRaw.(*StreamCacheEntry)

	if sce.expiresAt.Before(time.Now()) {
		sc.cache.Remove(key)
		return nil
	}

	return sce
}

func (sc *StreamCache) Put(key interface{}, reader ReaderWithMetadata, ctx context.Context) *StreamCacheEntry {
	lock := &sync.RWMutex{}
	sce := &StreamCacheEntry{
		lock: lock,
		cond: sync.NewCond(lock.RLocker()),
		buffer: make([]byte, 0, INITIAL_SIZE),
		expiresAt: time.Now().Add(sc.expiry),
	}

	sc.cache.Add(key, sce)

	go sce.cache(reader)

	return sce
}

func (sc *StreamCache) Remove(key interface{}) {
	sc.cache.Remove(key)
}

func (scr *SCEReader) Read(p []byte) (int, error) {

	scr.sce.lock.RLock()
	defer scr.sce.lock.RUnlock()

	for {
		switch {
		case scr.status == status_complete:
			glog.Errorf("reached EOF with nextOffset %d", scr.nextOffset)
			return 0, io.EOF
		case scr.status == status_error:
			return 0, scr.sce.error
		case scr.nextOffset < len(scr.sce.buffer):
			count := min(len(p), len(scr.sce.buffer) - scr.nextOffset)
			copied := copy(p, scr.sce.buffer[scr.nextOffset:scr.nextOffset+count])
			scr.nextOffset += copied
			glog.Errorf("cache read: copied %d bytes to p with len %d. nextOffset now %d", copied, len(p), scr.nextOffset)
			return copied, nil
		case scr.sce.status == status_complete:
			glog.Errorf("reached EOF with nextOffset %d", scr.nextOffset)
			scr.status = status_complete
			return 0, io.EOF
		case scr.sce.status == status_error:
			scr.status = status_error
			return 0, scr.sce.error
		case scr.nextOffset >= len(scr.sce.buffer):
			scr.sce.cond.Wait()
		default:
			panic("unknown condition")
		}
	}
}

func (sce *StreamCacheEntry) cache(reader ReaderWithMetadata) {
	sce.lock.Lock()
	metadata, err := reader.GetMetadata()
	if err != nil {
		sce.status = status_error
		sce.error = err
		return
	}

	sce.metadata = metadata
	sce.status = status_metadata_read

	sce.cond.Broadcast()
	sce.lock.Unlock()

	buf := make([]byte, 4096)
	for !isTerminalState(sce.status) {
		read, err := reader.Read(buf)

		sce.lock.Lock()

		if read > 0 {
			nextBuf := append(sce.buffer, buf[0:read]...)
			glog.Errorf("cache: appended %d bytes to buffer len %d resulting in len %d", read, len(sce.buffer), len(nextBuf))
			sce.buffer = nextBuf
		}

		switch {
		case err == io.EOF:
			sce.status = status_complete
		case err != nil:
			sce.status = status_error
			sce.error = err
		}

		sce.cond.Broadcast()
		sce.lock.Unlock()
	}
}

func isTerminalState(st status) bool {
	return st >= status_complete
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}