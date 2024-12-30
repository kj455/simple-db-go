package tx

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
)

type LockType string

const (
	LOCK_TYPE_S LockType = "S"
	LOCK_TYPE_X LockType = "X"
)

/*
1. Before reading a block, acquire a shared lock on it.
2. Before modifying a block, acquire an exclusive lock on it.
3. Release all locks after a commit or rollback.
*/
type ConcurrencyMgrImpl struct {
	l     Lock
	Locks map[file.BlockId]LockType
}

func NewConcurrencyMgr() *ConcurrencyMgrImpl {
	return &ConcurrencyMgrImpl{
		l:     NewLock(),
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
	cm.Locks[blk] = LOCK_TYPE_S
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
	cm.Locks[blk] = LOCK_TYPE_X
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
	return lockType == LOCK_TYPE_X
}
