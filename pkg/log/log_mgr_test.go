package log

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

const testFilename = "test.log"

func newMockLogMgr(m *mocks) *LogMgrImpl {
	return &LogMgrImpl{
		filename:     testFilename,
		fileMgr:      m.fileMgr,
		page:         m.page,
		currentBlock: m.block,
	}
}

func TestNewLogMgr(t *testing.T) {
	const (
		filename  = "test.log"
		blockSize = 4096
		blockNum  = 0
	)
	blockId := file.NewBlockId(filename, blockNum)
	tests := []struct {
		name   string
		setup  func(m *mocks)
		expect func(lm *LogMgrImpl)
	}{
		{
			name: "block length is 0",
			setup: func(m *mocks) {
				const length = 0
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
				m.fileMgr.EXPECT().Length(filename).Return(length, nil)
				m.fileMgr.EXPECT().Append(filename).Return(blockId, nil)
				m.fileMgr.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
			},
			expect: func(lm *LogMgrImpl) {
				assert.NotNil(t, lm)
				assert.Equal(t, filename, lm.filename)
				assert.Equal(t, blockSize, lm.getLastOffset())
			},
		},
		{
			name: "block length is not 0",
			setup: func(m *mocks) {
				const length = 1
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
				m.fileMgr.EXPECT().Length(filename).Return(length, nil)
				m.fileMgr.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil)
			},
			expect: func(lm *LogMgrImpl) {
				assert.NotNil(t, lm)
				assert.Equal(t, filename, lm.filename)
				assert.Equal(t, 0, lm.getLastOffset())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			tt.setup(m)
			lm, _ := NewLogMgr(m.fileMgr, filename)
			tt.expect(lm)
		})
	}
}

func TestLogMgr_Append(t *testing.T) {
	const (
		filename  = "test.log"
		blockSize = 4096
		blockNum  = 0
	)
	tests := []struct {
		name   string
		record []byte
		setup  func(m *mocks, lm *LogMgrImpl)
		expect func(lm *LogMgrImpl)
	}{
		{
			name:   "has enough space",
			record: []byte("test"),
			setup: func(m *mocks, lm *LogMgrImpl) {
				m.page.EXPECT().GetInt(0).Return(uint32(blockSize)).AnyTimes()
				m.page.EXPECT().SetBytes(blockSize-4, []byte("test"))
				m.page.EXPECT().SetInt(0, uint32(blockSize)-4)
				lm.latestLSN = 99
			},
			expect: func(lm *LogMgrImpl) {
				assert.NotNil(t, lm)
				assert.Equal(t, filename, lm.filename)
				assert.Equal(t, 99+1, lm.latestLSN)
			},
		},
		{
			name:   "has not enough space",
			record: []byte("test"),
			setup: func(m *mocks, lm *LogMgrImpl) {
				m.page.EXPECT().GetInt(0).Return(uint32(6))
				m.fileMgr.EXPECT().Write(m.block, m.page).Return(nil)
				newBlock := file.NewBlockId(filename, blockNum+1)
				m.fileMgr.EXPECT().Append(filename).Return(newBlock, nil)
				m.fileMgr.EXPECT().Write(newBlock, m.page).Return(nil)
				m.fileMgr.EXPECT().BlockSize().Return(blockSize).AnyTimes()
				m.page.EXPECT().SetInt(0, uint32(blockSize))
				m.page.EXPECT().GetInt(0).Return(uint32(blockSize))
				m.page.EXPECT().SetBytes(blockSize-4, []byte("test"))
				m.page.EXPECT().SetInt(0, uint32(blockSize)-4)
				lm.latestLSN = 99
			},
			expect: func(lm *LogMgrImpl) {
				assert.NotNil(t, lm)
				assert.Equal(t, filename, lm.filename)
				assert.Equal(t, 99+1, lm.latestLSN)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			lm := newMockLogMgr(m)
			tt.setup(m, lm)
			lm.Append(tt.record)
			tt.expect(lm)
		})
	}
}

func TestLogMgr_Flush(t *testing.T) {
	tests := []struct {
		name   string
		lsn    int
		setup  func(m *mocks, lm *LogMgrImpl)
		expect func(lm *LogMgrImpl)
	}{
		{
			name: "flush past lsn",
			lsn:  100,
			setup: func(m *mocks, lm *LogMgrImpl) {
				lm.latestLSN = 100
				lm.lastSavedLSN = 99
				m.fileMgr.EXPECT().Write(m.block, m.page).Return(nil)
			},
			expect: func(lm *LogMgrImpl) {
				assert.Equal(t, 100, lm.lastSavedLSN)
			},
		},
		{
			name: "not flush past lsn",
			lsn:  100,
			setup: func(m *mocks, lm *LogMgrImpl) {
				m.fileMgr.EXPECT().Write(m.block, m.page).Return(nil)
				lm.latestLSN = 99
				lm.lastSavedLSN = 99
			},
			expect: func(lm *LogMgrImpl) {
				assert.Equal(t, 99, lm.lastSavedLSN)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			lm := newMockLogMgr(m)
			tt.setup(m, lm)
			lm.Flush(tt.lsn)
			tt.expect(lm)
		})
	}
}
