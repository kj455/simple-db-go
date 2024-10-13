package buffermgr

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kj455/db/pkg/buffer"
	"github.com/kj455/db/pkg/file"
	ttime "github.com/kj455/db/pkg/time"
)

const defaultMaxWaitTime = 10 * time.Second

type BufferMgrImpl struct {
	pool         []buffer.Buffer
	availableNum int
	mu           sync.Mutex
	time         ttime.Time
	maxWaitTime  time.Duration
}

type Option func(*BufferMgrImpl)

func WithMaxWaitTime(t time.Duration) Option {
	return func(b *BufferMgrImpl) {
		b.maxWaitTime = t
	}
}

func WithTime(t ttime.Time) Option {
	return func(b *BufferMgrImpl) {
		b.time = t
	}
}

func NewBufferMgr(buffs []buffer.Buffer, opts ...Option) *BufferMgrImpl {
	bm := &BufferMgrImpl{
		pool:         buffs,
		availableNum: len(buffs),
		time:         ttime.NewTime(),
		maxWaitTime:  defaultMaxWaitTime,
	}
	for _, opt := range opts {
		opt(bm)
	}
	return bm
}

func (bm *BufferMgrImpl) Pin(block file.BlockId) (buffer.Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	startTime := bm.time.Now()
	var buff buffer.Buffer
	var ok bool
	for {
		buff, ok = bm.tryPin(block)
		if ok || bm.hasWaitedTooLong(startTime) {
			break
		}
		bm.mu.Unlock()
		bm.wait()
		bm.mu.Lock()
	}
	if !ok {
		return nil, errors.New("buffer: no available buffer")
	}
	return buff, nil
}

func (bm *BufferMgrImpl) Unpin(buff buffer.Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	buff.Unpin()
	if !buff.IsPinned() {
		bm.availableNum++
		return
	}
}

func (bm *BufferMgrImpl) AvailableNum() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.availableNum
}

func (bm *BufferMgrImpl) FlushAll(txNum int) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, b := range bm.pool {
		if b.ModifyingTx() != txNum {
			continue
		}
		if err := b.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (bm *BufferMgrImpl) wait() {
	bm.time.Sleep(bm.maxWaitTime)
}

func (bm *BufferMgrImpl) hasWaitedTooLong(startTime time.Time) bool {
	return bm.time.Since(startTime) > bm.maxWaitTime
}

func (bm *BufferMgrImpl) tryPin(block file.BlockId) (buffer.Buffer, bool) {
	buff, ok := bm.findBufferByBlock(block)
	fmt.Println("ok", ok)
	if !ok {
		buff, ok = bm.findUnpinnedBuffer()
		if !ok {
			fmt.Println("buffer: no unpinned buffer")
			return nil, false
		}
		err := buff.AssignToBlock(block)
		if err != nil {
			fmt.Println("buffer: failed to assign block to buff", err)
			return nil, false
		}
	}
	if !buff.IsPinned() {
		bm.availableNum--
	}
	buff.Pin()
	return buff, true
}

func (bm *BufferMgrImpl) findBufferByBlock(block file.BlockId) (buffer.Buffer, bool) {
	for _, buff := range bm.pool {
		b := buff.Block()
		if b != nil && b.Equals(block) {
			return buff, true
		}
	}
	return nil, false
}

func (bm *BufferMgrImpl) findUnpinnedBuffer() (buffer.Buffer, bool) {
	for _, buff := range bm.pool {
		if !buff.IsPinned() {
			return buff, true
		}
	}
	return nil, false
}
