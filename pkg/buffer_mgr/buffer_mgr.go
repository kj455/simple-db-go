package buffermgr

import (
	"errors"
	"sync"
	"time"

	"github.com/kj455/db/pkg/buffer"
	"github.com/kj455/db/pkg/file"
	dtime "github.com/kj455/db/pkg/time"
)

const defaultMaxWaitTime = 10 * time.Second

type BufferMgrImpl struct {
	pool         []buffer.Buffer
	availableNum int
	mu           sync.Mutex
	time         dtime.Time
	maxWaitTime  time.Duration
}

type NewBufferMgrParams struct {
	Buffers     []buffer.Buffer
	MaxWaitTime time.Duration
	Time        dtime.Time
}

func NewBufferMgr(p *NewBufferMgrParams) *BufferMgrImpl {
	maxWaitTime := p.MaxWaitTime
	if p.MaxWaitTime == 0 {
		maxWaitTime = defaultMaxWaitTime
	}
	return &BufferMgrImpl{
		pool:         p.Buffers,
		availableNum: len(p.Buffers),
		time:         p.Time,
		maxWaitTime:  maxWaitTime,
	}
}

func (bm *BufferMgrImpl) Pin(block file.BlockId) (buffer.Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	startTime := bm.time.Now()
	var buff buffer.Buffer
	for {
		buff = bm.tryPin(block)
		if buff != nil || bm.hasWaitedTooLong(startTime) {
			break
		}
		bm.mu.Unlock()
		bm.wait()
		bm.mu.Lock()
	}
	if buff == nil {
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
		if b.ModifyingTx() == txNum {
			err := b.Flush()
			if err != nil {
				return err
			}
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

func (bm *BufferMgrImpl) tryPin(block file.BlockId) buffer.Buffer {
	buff := bm.findBufferByBlock(block)
	if buff == nil {
		buff = bm.findUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		err := buff.AssignToBlock(block)
		if err != nil {
			return nil
		}
	}
	if !buff.IsPinned() {
		bm.availableNum--
	}
	buff.Pin()
	return buff
}

func (bm *BufferMgrImpl) findBufferByBlock(block file.BlockId) buffer.Buffer {
	for _, buff := range bm.pool {
		b := buff.Block()
		if b != nil && b.Equals(block) {
			return buff
		}
	}
	return nil
}

func (bm *BufferMgrImpl) findUnpinnedBuffer() buffer.Buffer {
	for _, buff := range bm.pool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
