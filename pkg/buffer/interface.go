package buffer

import "github.com/kj455/simple-db/pkg/file"

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
