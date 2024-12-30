package tx

import (
	"fmt"
	"sync"
	"time"

	"github.com/kj455/simple-db/pkg/file"
	ttime "github.com/kj455/simple-db/pkg/time"
)

const (
	DEFAULT_MAX_WAIT_TIME = 10 * time.Second
)

type LockImpl struct {
	locks       map[file.BlockId]lockState
	mu          *sync.Mutex
	cond        *sync.Cond
	maxWaitTime time.Duration
	time        ttime.Time
}

type LockOption func(*LockImpl)

func WithTime(t ttime.Time) LockOption {
	return func(o *LockImpl) {
		o.time = t
	}
}

func WithWaitTime(d time.Duration) LockOption {
	return func(o *LockImpl) {
		o.maxWaitTime = d
	}
}

func NewLock(options ...LockOption) *LockImpl {
	l := &LockImpl{
		locks:       make(map[file.BlockId]lockState),
		maxWaitTime: DEFAULT_MAX_WAIT_TIME,
		time:        ttime.NewTime(),
		mu:          &sync.Mutex{},
	}
	l.cond = sync.NewCond(l.mu)
	for _, option := range options {
		option(l)
	}
	return l
}

func (l *LockImpl) SLock(block file.BlockId) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.time.Now()
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

	now := l.time.Now()
	// Concurrency manager will always obtain an SLock on the block before requesting the XLock.
	// So, a value higher than 1 indicates that some other transaction also has a lock on this block.
	for l.hasOtherSLocks(block) && !l.hasWaitedTooLong(now) {
		l.cond.Wait()
	}
	if l.hasOtherSLocks(block) {
		return fmt.Errorf("lock: XLock: block %v has other S locks", block)
	}
	l.locks[block] = LOCK_STATE_X_LOCKED
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
		return LOCK_STATE_UNLOCKED
	}
	return state
}

func (l *LockImpl) hasWaitedTooLong(startTime time.Time) bool {
	return l.time.Since(startTime) > l.maxWaitTime
}
