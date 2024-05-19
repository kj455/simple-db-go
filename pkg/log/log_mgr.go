package log

import (
	"fmt"
	"sync"

	"github.com/kj455/db/pkg/file"
)

type LogMgr interface {
	Append(record []byte) (int, error)
	Flush(lsn int) error
	Iterator() (*LogIteratorImpl, error)
}

type LogMgrImpl struct {
	filename     string
	fileMgr      file.FileMgr
	page         file.Page
	currentBlock file.BlockId
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

func NewLogMgr(fm file.FileMgr, filename string) (*LogMgrImpl, error) {
	page := file.NewPage(fm.BlockSize())
	lm := &LogMgrImpl{
		fileMgr:  fm,
		filename: filename,
		page:     page,
	}
	blockLength, err := fm.Length(filename)
	if err != nil {
		return nil, fmt.Errorf("log: cannot get length of file %s: %w", filename, err)
	}
	if blockLength == 0 {
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("log: cannot append new block: %w", err)
		}
		return lm, nil
	}
	lm.currentBlock = file.NewBlockId(filename, blockLength-1)
	err = fm.Read(lm.currentBlock, lm.page)
	if err != nil {
		return nil, fmt.Errorf("log: cannot read block %s: %w", lm.currentBlock, err)
	}
	return lm, nil
}

// Append appends a record to the log backwardly and returns the LSN of the record.
func (lm *LogMgrImpl) Append(record []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if !lm.hasWritableSpace(len(record)) {
		err := lm.flush()
		if err != nil {
			return -1, fmt.Errorf("log: cannot flush log: %w", err)
		}
		block, err := lm.appendNewBlock()
		if err != nil {
			return -1, fmt.Errorf("log: cannot append new block: %w", err)
		}
		lm.currentBlock = block
	}
	offset := lm.getLastOffset() - len(record)
	lm.setBytes(offset, record)
	lm.latestLSN++
	return lm.latestLSN, nil
}

// Flush flushes the log to disk.
func (lm *LogMgrImpl) Flush(lsn int) error {
	if lsn < lm.lastSavedLSN {
		return nil
	}
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.flush()
}

func (lm *LogMgrImpl) Iterator() (*LogIteratorImpl, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	err := lm.flush()
	if err != nil {
		return nil, fmt.Errorf("log: cannot flush log: %w", err)
	}
	return NewLogIterator(lm.fileMgr, lm.currentBlock)
}

func (lm *LogMgrImpl) appendNewBlock() (*file.BlockIdImpl, error) {
	block, err := lm.fileMgr.Append(lm.filename)
	if err != nil {
		return nil, err
	}
	err = lm.fileMgr.Write(block, lm.page)
	if err != nil {
		return nil, err
	}
	lm.setLastOffset(lm.fileMgr.BlockSize())
	return block, nil
}

func (lm *LogMgrImpl) flush() error {
	err := lm.fileMgr.Write(lm.currentBlock, lm.page)
	if err != nil {
		return err
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogMgrImpl) hasWritableSpace(size int) bool {
	const intSize = 4
	return lm.getLastOffset()-size >= intSize
}

func (lm *LogMgrImpl) getLastOffset() int {
	return int(lm.page.GetInt(0))
}

func (lm *LogMgrImpl) setLastOffset(pos int) {
	lm.page.SetInt(0, uint32(pos))
}

func (lm *LogMgrImpl) setBytes(offset int, value []byte) {
	lm.page.SetBytes(offset, value)
	lm.setLastOffset(offset)
}
