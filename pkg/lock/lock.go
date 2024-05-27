package lock

import (
	"fmt"
	"sync"
	"time"

	"github.com/kj455/db/pkg/file"
	ttime "github.com/kj455/db/pkg/time"
)

const (
	DEFAULT_MAX_WAIT_TIME = 10 * time.Second
)

type Lock interface {
	SLock(block file.BlockId) error
	XLock(block file.BlockId) error
	Unlock(block file.BlockId)
}

type LockImpl struct {
	locks       map[file.BlockId]lockState
	mu          sync.Mutex
	cond        *sync.Cond
	maxWaitTime time.Duration
	time        ttime.Time
}

type NewLockParams struct {
	WaitTime time.Duration
	Time     ttime.Time
}

func NewLock(p NewLockParams) *LockImpl {
	waitTime := DEFAULT_MAX_WAIT_TIME
	if p.WaitTime != 0 {
		waitTime = p.WaitTime
	}
	l := &LockImpl{
		locks:       make(map[file.BlockId]lockState),
		maxWaitTime: waitTime,
		time:        p.Time,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *LockImpl) SLock(block file.BlockId) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for l.hasXlock(block) && !l.hasWaitedTooLong(now) {
		l.cond.Wait()
	}
	if l.hasXlock(block) {
		return fmt.Errorf("lock: SLock: block %v has X lock", block)
	}
	state := l.getLockState(block)
	next, err := state.Next()
	if err != nil {
		return err
	}
	l.locks[block] = next
	return nil
}

func (l *LockImpl) XLock(block file.BlockId) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	// Concurrency manager will always obtain an SLock on the block before requesting the XLock.
	// So, a value higher than 1 indicates that some other transaction also has a lock on this block.
	for l.hasOtherSLocks(block) && !l.hasWaitedTooLong(now) {
		l.cond.Wait()
	}
	if l.hasOtherSLocks(block) {
		return fmt.Errorf("lock: XLock: block %v has other S locks", block)
	}
	l.locks[block] = X_LOCKED
	return nil
}

func (l *LockImpl) Unlock(block file.BlockId) {
	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.getLockState(block)
	if state.IsMultipleSLocked() {
		l.locks[block] = state - 1
		return
	}
	delete(l.locks, block)
	l.cond.Broadcast()
}

func (lt *LockImpl) hasXlock(block file.BlockId) bool {
	return lt.getLockState(block).IsXLocked()
}

func (l *LockImpl) hasOtherSLocks(block file.BlockId) bool {
	return l.getLockState(block).IsMultipleSLocked()
}

func (lt *LockImpl) getLockState(block file.BlockId) lockState {
	state, ok := lt.locks[block]
	if !ok {
		return UNLOCKED
	}
	return state
}

func (l *LockImpl) hasWaitedTooLong(startTime time.Time) bool {
	return time.Since(startTime) > l.maxWaitTime
}
