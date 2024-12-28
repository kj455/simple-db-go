package log

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
)

type LogIteratorImpl struct {
	fm        file.FileMgr
	block     file.BlockId
	page      file.Page
	curOffset int
}

func NewLogIterator(fm file.FileMgr, block file.BlockId) (*LogIteratorImpl, error) {
	page := file.NewPage(fm.BlockSize())
	iter := &LogIteratorImpl{
		fm:    fm,
		block: block,
		page:  page,
	}
	err := iter.moveToBlock(block)
	if err != nil {
		return nil, fmt.Errorf("log: cannot move to block %s: %w", block, err)
	}
	return iter, nil
}

// HasNext returns true if there are more records to read.
func (li *LogIteratorImpl) HasNext() bool {
	return li.curOffset < li.fm.BlockSize() || li.block.Number() > 0
}

// Next returns the next record from left to right(latest to oldest).
func (li *LogIteratorImpl) Next() ([]byte, error) {
	finished := li.curOffset == li.fm.BlockSize()
	if finished {
		blockId := file.NewBlockId(li.block.Filename(), li.block.Number()-1)
		li.block = blockId
		err := li.moveToBlock(blockId)
		if err != nil {
			return nil, fmt.Errorf("log: cannot move to block %s: %w", blockId, err)
		}
	}
	record := li.page.GetBytes(li.curOffset)
	const bytesLen = 4
	li.curOffset += bytesLen + len(record)
	return record, nil
}

func (li *LogIteratorImpl) moveToBlock(block file.BlockId) error {
	err := li.fm.Read(block, li.page)
	if err != nil {
		return err
	}
	li.curOffset = int(li.page.GetInt(0))
	return nil
}
