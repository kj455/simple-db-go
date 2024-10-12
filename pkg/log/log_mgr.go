package log

import (
	"fmt"
	"sync"

	"github.com/kj455/db/pkg/file"
)

/*
	LogMgrImpl is a log manager that manages the log records in a file.
	Log records are stored in a file in a backward manner(right to left).

```

	-------------------
	| 4 bytes: offset |
	-------------------
	|  empty space    |
	-------------------
	|  record 2       |
	-------------------
	|  record 1       |
	-------------------

````
*/
type LogMgrImpl struct {
	filename     string
	fileMgr      file.FileMgr
	page         file.Page
	currentBlock file.BlockId
	latestLSN    int // LSN: log sequence number
	lastSavedLSN int
	mu           sync.Mutex
}

// First 4 bytes of a block is the offset where the last record starts.
const OFFSET_SIZE = 4

func NewLogMgr(fm file.FileMgr, filename string) (*LogMgrImpl, error) {
	page := file.NewPage(fm.BlockSize())
	lm := &LogMgrImpl{
		fileMgr:  fm,
		filename: filename,
		page:     page,
	}
	blockNum, err := fm.BlockNum(filename)
	if err != nil {
		return nil, fmt.Errorf("log: cannot get length of file %s: %w", filename, err)
	}
	if blockNum == 0 {
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("log: cannot append new block: %w", err)
		}
		return lm, nil
	}
	lm.currentBlock = file.NewBlockId(filename, blockNum-1)
	if err = fm.Read(lm.currentBlock, lm.page); err != nil {
		return nil, fmt.Errorf("log: cannot read block %s: %w", lm.currentBlock, err)
	}
	return lm, nil
}

// Append appends a record to the log backwardly and returns the LSN of the record.
func (lm *LogMgrImpl) Append(record []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	bytesNeeded := len(record) + OFFSET_SIZE
	if lm.hasInsufficientSpace(bytesNeeded) {
		if err := lm.flush(); err != nil {
			return -1, fmt.Errorf("log: cannot flush log: %w", err)
		}
		var err error
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return -1, fmt.Errorf("log: cannot append new block: %w", err)
		}
	}
	offset := lm.getLastOffset() - bytesNeeded
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

func (lm *LogMgrImpl) flush() error {
	err := lm.fileMgr.Write(lm.currentBlock, lm.page)
	if err != nil {
		return err
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogMgrImpl) Iterator() (LogIterator, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if err := lm.flush(); err != nil {
		return nil, fmt.Errorf("log: cannot flush log: %w", err)
	}
	return NewLogIterator(lm.fileMgr, lm.currentBlock)
}

func (lm *LogMgrImpl) appendNewBlock() (file.BlockId, error) {
	block, err := lm.fileMgr.Append(lm.filename)
	if err != nil {
		return nil, err
	}
	if err = lm.fileMgr.Write(block, lm.page); err != nil {
		return nil, err
	}
	lm.setLastOffset(lm.fileMgr.BlockSize())
	return block, nil
}

func (lm *LogMgrImpl) hasInsufficientSpace(size int) bool {
	return lm.getLastOffset() < OFFSET_SIZE+size
}

func (lm *LogMgrImpl) getLastOffset() int {
	return int(lm.page.GetInt(0))
}

func (lm *LogMgrImpl) setLastOffset(val int) {
	lm.page.SetInt(0, uint32(val))
}

func (lm *LogMgrImpl) setBytes(offset int, value []byte) {
	lm.page.SetBytes(offset, value)
	lm.setLastOffset(offset)
}
