package streamcache

import (
	"io"
	"time"
	"sync"
	"context"
	lru "github.com/hashicorp/golang-lru"
	"github.com/golang/glog"
	"sync/atomic"
)

type ReaderWithMetadata interface {
	GetMetadata() (map[interface{}]interface{}, error)
	io.Reader
}

type status int32

const (
	status_inprogress status = iota
	status_metadata_read = iota
	status_complete = iota
	status_error = iota
)

const INITIAL_SIZE = 50000

var nextEntryId int32

type StreamCacheEntry struct {
	id int32
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
	glog.Infof("%v: copying returned %v %v", sce.id, cnt, err)
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
		id: atomic.AddInt32(&nextEntryId, int32(1)),
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
			glog.Infof("%v: reached EOF with nextOffset %d", scr.sce.id, scr.nextOffset)
			return 0, io.EOF
		case scr.status == status_error:
			return 0, scr.sce.error
		case scr.nextOffset < len(scr.sce.buffer):
			count := min(len(p), len(scr.sce.buffer) - scr.nextOffset)
			copied := copy(p, scr.sce.buffer[scr.nextOffset:scr.nextOffset+count])
			scr.nextOffset += copied
			glog.Infof("%v: cache read: copied %d bytes to p with len %d. nextOffset now %d", scr.sce.id, copied, len(p), scr.nextOffset)
			return copied, nil
		case scr.sce.status == status_complete:
			glog.Infof("%v: reached EOF with nextOffset %d %d", scr.sce.id, scr.nextOffset, len(scr.sce.buffer))
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

func (sce *StreamCacheEntry) GetMetadata() (map[interface{}]interface{}, error) {
	sce.lock.RLock()
	defer sce.lock.RUnlock()

	for {
		switch {
		case sce.status < status_metadata_read:
			sce.cond.Wait()
		case sce.status == status_error:
			return sce.metadata, sce.error
		default:
			return sce.metadata, nil
		}
	}
}

func (sce *StreamCacheEntry) cache(reader ReaderWithMetadata) {
	metadata, err := reader.GetMetadata()

	sce.lock.Lock()

	if err != nil {
		sce.status = status_error
		sce.error = err
	} else {
		sce.metadata = metadata
		sce.status = status_metadata_read
	}

	sce.cond.Broadcast()
	sce.lock.Unlock()

	if err != nil {
		return
	}

	byteCount := 0
	buf := make([]byte, 4096)
	for !isTerminalState(sce.status) {
		read, err := reader.Read(buf)

		sce.lock.Lock()

		if read > 0 {
			byteCount += read
			nextBuf := append(sce.buffer, buf[0:read]...)
			glog.Infof("%v: cache: appended %d bytes to buffer len %d resulting in len %d", sce.id, read, len(sce.buffer), len(nextBuf))
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

	if closer, ok := reader.(io.Closer) ; ok {
		glog.Infof("%v: closing", sce.id)
		closer.Close()
	}

	glog.Infof("%v: cache: cached %v bytes. buf is now %v", sce.id, byteCount, len(sce.buffer))
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