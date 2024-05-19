package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileMgr interface {
	Read(id BlockId, p Page) error
	Write(id BlockId, p Page) error
	Append(filename string) (*BlockIdImpl, error)
	Length(filename string) (int, error)
	BlockSize() int
}

type FileMgrImpl struct {
	dbDir     string
	blockSize int
	isNew     bool
	openFiles map[string]*os.File
	mu        sync.Mutex
}

func NewFileMgr(dbDir string, blockSize int) *FileMgrImpl {
	_, err := os.Stat(dbDir)
	fileExists := !os.IsNotExist(err)

	return &FileMgrImpl{
		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     fileExists,
		openFiles: make(map[string]*os.File),
	}
}

// Read reads a page from the file.
func (m *FileMgrImpl) Read(id BlockId, p Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(id.Filename())
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", id.Filename(), err)
	}

	_, err = f.Seek(int64(id.Number())*int64(m.blockSize), 0)
	if err != nil {
		return fmt.Errorf("cannot seek to block %d: %w", id.Number(), err)
	}

	_, err = f.Read(p.Contents().Bytes())
	return err
}

// Write writes a page to the file.
func (m *FileMgrImpl) Write(id BlockId, p Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(id.Filename())
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", id.Filename(), err)
	}

	_, err = f.Seek(int64(id.Number())*int64(m.blockSize), 0)
	if err != nil {
		return fmt.Errorf("cannot seek to block %d: %w", id.Number(), err)
	}

	_, err = f.Write(p.Contents().Bytes())
	return err
}

// Append appends a new block to the file and returns the block ID.
func (m *FileMgrImpl) Append(filename string) (*BlockIdImpl, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %w", filename, err)
	}

	blockNum := m.getBlockNum(filename)
	block := NewBlockId(filename, blockNum)
	_, err = f.Seek(int64(block.blockNum)*int64(m.blockSize), 0)
	if err != nil {
		return nil, fmt.Errorf("cannot seek to block %d: %w", block.blockNum, err)
	}

	buf := make([]byte, m.blockSize)
	_, err = f.Write(buf)
	if err != nil {
		return nil, fmt.Errorf("cannot write to block %d: %w", block.blockNum, err)
	}

	return block, nil
}

// Length returns the number of blocks in the file.
func (m *FileMgrImpl) Length(filename string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(filename)
	if err != nil {
		return -1, err
	}

	length, err := f.Seek(0, 2) // Seek to end of file
	return int(length) / m.blockSize, err
}

func (m *FileMgrImpl) BlockSize() int {
	return m.blockSize
}

func (mgr *FileMgrImpl) getFile(filename string) (*os.File, error) {
	if f, exists := mgr.openFiles[filename]; exists {
		return f, nil
	}

	filePath := filepath.Join(mgr.dbDir, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	mgr.openFiles[filename] = f
	return f, nil
}

func (m *FileMgrImpl) getBlockNum(filename string) int {
	f, err := m.getFile(filename)
	if err != nil {
		return -1
	}

	info, err := f.Stat()
	if err != nil {
		return -1
	}

	return int(info.Size()) / m.blockSize
}