package buffer

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
)

type ReadPage interface {
	GetInt(offset int) uint32
	GetBytes(offset int) []byte
	GetString(offset int) string
}

type WritePage interface {
	SetInt(offset int, value uint32)
	SetBytes(offset int, value []byte)
	SetString(offset int, value string)
}

type ReadWritePage interface {
	ReadPage
	WritePage
}

type BufferImpl struct {
	fileMgr  file.FileMgr
	logMgr   log.LogMgr
	contents file.Page
	block    file.BlockId
	pins     int
	txNum    int
	lsn      int
}

func NewBuffer(fm file.FileMgr, lm log.LogMgr, blockSize int) *BufferImpl {
	return &BufferImpl{
		fileMgr:  fm,
		logMgr:   lm,
		contents: file.NewPage(blockSize),
		block:    nil,
		pins:     0,
		txNum:    -1,
		lsn:      -1,
	}
}

func (b *BufferImpl) Contents() ReadPage {
	return b.contents
}

func (b *BufferImpl) WriteContents(txNum, lsn int, write func(p ReadWritePage)) {
	b.setModified(txNum, lsn)
	write(b.contents)
}

func (b *BufferImpl) Block() file.BlockId {
	return b.block
}

func (b *BufferImpl) IsPinned() bool {
	return b.pins > 0
}

func (b *BufferImpl) ModifyingTx() int {
	return b.txNum
}

func (b *BufferImpl) AssignToBlock(block file.BlockId) error {
	if err := b.Flush(); err != nil {
		return fmt.Errorf("buffer: failed to flush: %w", err)
	}
	if err := b.fileMgr.Read(block, b.contents); err != nil {
		return fmt.Errorf("buffer: failed to read block: %w", err)
	}
	b.block = block
	b.pins = 0
	return nil
}

func (b *BufferImpl) Flush() error {
	if b.txNum < 0 {
		return nil
	}
	if err := b.logMgr.Flush(b.lsn); err != nil {
		return fmt.Errorf("buffer: failed to flush log: %w", err)
	}
	if err := b.fileMgr.Write(b.block, b.contents); err != nil {
		return fmt.Errorf("buffer: failed to write block: %w", err)
	}
	b.txNum = -1
	return nil
}

func (b *BufferImpl) Pin() {
	b.pins++
}

func (b *BufferImpl) Unpin() {
	b.pins--
}

func (b *BufferImpl) setModified(txNum, lsn int) {
	b.txNum = txNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}
