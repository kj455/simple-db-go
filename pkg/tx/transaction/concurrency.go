package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/tx"
)

type LockType string

const (
	LOCK_TYPE_SLOCK LockType = "S"
	LOCK_TYPE_XLOCK LockType = "X"
)

type ConcurrencyMgrImpl struct {
	l     tx.Lock
	Locks map[file.BlockId]LockType
}

func NewConcurrencyMgr() *ConcurrencyMgrImpl {
	l := NewLock(NewLockParams{})
	return &ConcurrencyMgrImpl{
		l:     l,
		Locks: make(map[file.BlockId]LockType),
	}
}

func (cm *ConcurrencyMgrImpl) SLock(blk file.BlockId) error {
	if _, exists := cm.Locks[blk]; exists {
		return nil
	}
	if err := cm.l.SLock(blk); err != nil {
		return fmt.Errorf("concurrency: SLock: %v", err)
	}
	cm.Locks[blk] = LOCK_TYPE_SLOCK
	return nil
}

func (cm *ConcurrencyMgrImpl) XLock(blk file.BlockId) error {
	if cm.HasXLock(blk) {
		return nil
	}
	if err := cm.SLock(blk); err != nil {
		return fmt.Errorf("concurrency: SLock before XLock: %v", err)
	}
	if err := cm.l.XLock(blk); err != nil {
		return fmt.Errorf("concurrency: XLock: %v", err)
	}
	cm.Locks[blk] = LOCK_TYPE_XLOCK
	return nil
}

func (cm *ConcurrencyMgrImpl) Release() {
	for blk := range cm.Locks {
		cm.l.Unlock(blk)
		delete(cm.Locks, blk)
	}
}

func (cm *ConcurrencyMgrImpl) HasXLock(blk file.BlockId) bool {
	lockType, exists := cm.Locks[blk]
	if !exists {
		return false
	}
	return lockType == LOCK_TYPE_XLOCK
}
