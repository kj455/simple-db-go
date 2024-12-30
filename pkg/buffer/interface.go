//go:generate mkdir -p mock
//go:generate mockgen -source=./interface.go -package=mock -destination=./mock/interface.go
package buffer

import (
	"github.com/kj455/simple-db/pkg/file"
)

type Buffer interface {
	Block() file.BlockId
	IsPinned() bool
	Contents() ReadPage
	WriteContents(txNum, lsn int, write func(p ReadWritePage))
	ModifyingTx() int
	AssignToBlock(block file.BlockId) error
	Flush() error
	Pin()
	Unpin()
}

/*
BufferMgr has methods to pin and unpin a page.
The method pin returns a Buffer object pinned to a page containing the specified block, and the unpin method unpins the page.
The available method returns the number of unpinned buffer pages.
And the method flushAll ensures that all pages modified by the specified transaction have been written to disk.
*/
type BufferMgr interface {
	Pin(block file.BlockId) (Buffer, error)
	Unpin(buff Buffer)
	AvailableNum() int
	FlushAll(txNum int) error
}
