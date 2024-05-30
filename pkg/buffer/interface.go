package buffer

import "github.com/kj455/db/pkg/file"

type Buffer interface {
	Block() file.BlockId
	IsPinned() bool
	Contents() file.ReadPage
	WriteContents(txNum, lsn int, write func(p file.ReadWritePage))
	ModifyingTx() int
	AssignToBlock(block file.BlockId) error
	Flush() error
	Pin()
	Unpin()
}
