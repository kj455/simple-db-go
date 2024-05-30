package tx

import (
	"github.com/kj455/db/pkg/buffer"
	"github.com/kj455/db/pkg/file"
)

type Transaction interface {
	Commit()
	Rollback()
	Recover()

	Pin(block file.BlockId)
	Unpin(block file.BlockId)
	GetInt(block file.BlockId, offset int) int
	GetString(block file.BlockId, offset int) string
	SetInt(block file.BlockId, offset int, val int, okToLog bool)
	SetString(block file.BlockId, offset int, val string, okToLog bool)
	AvailableBuffs() int

	Size(filename string) int
	Append(filename string) file.BlockId
	BlockSize() int
}

// RecoveryMgr is an interface for recovery manager - undo only
type RecoveryMgr interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt(buff buffer.Buffer, offset int, val int) (int, error)
	SetString(buff buffer.Buffer, offset int, val string) (int, error)
}

type ConcurrencyMgr interface {
	SLock(blk file.BlockId) error
	XLock(blk file.BlockId) error
	Release()
}

type Lock interface {
	SLock(block file.BlockId) error
	XLock(block file.BlockId) error
	Unlock(block file.BlockId)
}

type BufferList interface {
	GetBuffer(block file.BlockId) (buffer.Buffer, bool)
	Pin(block file.BlockId) error
	Unpin(block file.BlockId)
	UnpinAll()
}
