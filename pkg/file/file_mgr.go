package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileMgr struct {
	dbDir     string
	blockSize int
	isNew     bool
	openFiles map[string]*os.File
	mu        sync.Mutex
}

func NewFileMgr(dbDir string, blockSize int) *FileMgr {
	_, err := os.Stat(dbDir)
	fileExists := !os.IsNotExist(err)

	return &FileMgr{
		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     fileExists,
		openFiles: make(map[string]*os.File),
	}
}

func (m *FileMgr) Read(id *BlockId, p *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(id.filename)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", id.filename, err)
	}

	_, err = f.Seek(int64(id.blockNum)*int64(m.blockSize), 0)
	if err != nil {
		return fmt.Errorf("cannot seek to block %d: %w", id.blockNum, err)
	}

	_, err = f.Read(p.Contents().Bytes())
	return err
}

func (mgr *FileMgr) getFile(filename string) (*os.File, error) {
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

func (m *FileMgr) Write(id *BlockId, p *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(id.filename)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %w", id.filename, err)
	}

	_, err = f.Seek(int64(id.blockNum)*int64(m.blockSize), 0)
	if err != nil {
		return fmt.Errorf("cannot seek to block %d: %w", id.blockNum, err)
	}

	_, err = f.Write(p.Contents().Bytes())
	return err
}

func (m *FileMgr) Append(filename string) (*BlockId, error) {
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

func (mgr *FileMgr) getBlockNum(filename string) int {
	f, err := mgr.getFile(filename)
	if err != nil {
		return -1
	}

	info, err := f.Stat()
	if err != nil {
		return -1
	}

	return int(info.Size()) / mgr.blockSize
}
