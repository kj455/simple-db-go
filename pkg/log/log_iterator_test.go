package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewLogIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := newMocks(ctrl)
	m.fileMgr.EXPECT().BlockSize().Return(4096)
	m.fileMgr.EXPECT().Read(m.block, gomock.Any()).Return(nil)

	li, err := NewLogIterator(m.fileMgr, m.block)

	assert.NoError(t, err)
	assert.NotNil(t, li)
	assert.Equal(t, m.fileMgr, li.fm)
	assert.Equal(t, m.block, li.block)
}

func newMockLogIterator(m *mocks) *LogIteratorImpl {
	return &LogIteratorImpl{
		fm:    m.fileMgr,
		block: m.block,
		page:  m.page,
	}
}

func TestLogIterator_HasNext(t *testing.T) {
	const blockSize = 4096
	tests := []struct {
		name   string
		setup  func(m *mocks, li *LogIteratorImpl)
		expect bool
	}{
		{
			name: "has next - offset is less than block size",
			setup: func(m *mocks, li *LogIteratorImpl) {
				li.curOffset = 0
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
			},
			expect: true,
		},
		{
			name: "has next - has next block",
			setup: func(m *mocks, li *LogIteratorImpl) {
				li.curOffset = blockSize
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
				m.block.EXPECT().Number().Return(1)
			},
			expect: true,
		},
		{
			name: "no next",
			setup: func(m *mocks, li *LogIteratorImpl) {
				li.curOffset = blockSize
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
				m.block.EXPECT().Number().Return(0)
			},
			expect: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			li := newMockLogIterator(m)
			tt.setup(m, li)
			assert.Equal(t, tt.expect, li.HasNext())
		})
	}
}

func TestLogIterator_Next(t *testing.T) {
	const (
		blockSize = 4096
		record    = "record"
	)
	tests := []struct {
		name   string
		setup  func(m *mocks, li *LogIteratorImpl)
		expect func(t *testing.T, li *LogIteratorImpl, got []byte)
	}{
		{
			name: "offset is less than block size",
			setup: func(m *mocks, li *LogIteratorImpl) {
				li.curOffset = 10
				m.fileMgr.EXPECT().BlockSize().Return(blockSize)
				m.page.EXPECT().GetBytes(10).Return([]byte(record))
			},
			expect: func(t *testing.T, li *LogIteratorImpl, got []byte) {
				assert.Equal(t, []byte(record), got)
				assert.Equal(t, 10+len(record)+4, li.curOffset)
			},
		},
		{
			name: "block finished",
			setup: func(m *mocks, li *LogIteratorImpl) {
				li.curOffset = blockSize
				m.fileMgr.EXPECT().BlockSize().Return(blockSize).AnyTimes()
				m.block.EXPECT().Filename().Return("test.log")
				m.block.EXPECT().Number().Return(1)
				m.fileMgr.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil)
				m.page.EXPECT().GetInt(0).Return(uint32(99))
				m.page.EXPECT().GetBytes(99).Return([]byte(record))
			},
			expect: func(t *testing.T, li *LogIteratorImpl, got []byte) {
				assert.Equal(t, []byte(record), got)
				assert.Equal(t, 99+len(record)+4, li.curOffset)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			li := newMockLogIterator(m)
			tt.setup(m, li)
			got, err := li.Next()
			tt.expect(t, li, got)
			assert.NoError(t, err)
		})
	}
}
