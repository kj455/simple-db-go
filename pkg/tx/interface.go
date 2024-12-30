//go:generate mkdir -p mock
//go:generate mockgen -source=./interface.go -package=mock -destination=./mock/interface.go
package tx

import (
	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
)

type Transaction interface {
	Commit() error
	Rollback() error
	Recover() error

	Pin(block file.BlockId) error
	Unpin(block file.BlockId)
	GetInt(block file.BlockId, offset int) (int, error)
	GetString(block file.BlockId, offset int) (string, error)
	// SetInt sets the value of the specified block at the specified offset.
	// If okToLog is true, then the method logs the change.
	SetInt(block file.BlockId, offset int, val int, okToLog bool) error
	SetString(block file.BlockId, offset int, val string, okToLog bool) error
	AvailableBuffs() int

	Size(filename string) (int, error)
	Append(filename string) (file.BlockId, error)
	BlockSize() int
}

// RecoveryMgr is an interface for recovery manager - undo only
type RecoveryMgr interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt(buff buffer.Buffer, offset int, oldVal int) (int, error)
	SetString(buff buffer.Buffer, offset int, oldVal string) (int, error)
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

/*
BufferList manages the list of currently pinned buffers for a transaction.
A BufferList object needs to know two things:
  - which buffer  is assigned to a specified block
  - how many times each block is pinned

The code uses a map to determine buffers and a list to determine pin counts.
The list contains a  BlockId object as many times as it is pinned; each time the block is unpinned, one  instance is removed from the list.
*/
type BufferList interface {
	GetBuffer(block file.BlockId) (buffer.Buffer, bool)
	Pin(block file.BlockId) error
	Unpin(block file.BlockId)
	UnpinAll()
}

type TxNumberGenerator interface {
	Next() int
}
