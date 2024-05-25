package buffer

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	tmock "github.com/kj455/db/pkg/time/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type mocks struct {
	fileMgr *fmock.MockFileMgr
	page    *fmock.MockPage
	block   *fmock.MockBlockId
	logMgr  *lmock.MockLogMgr
	time    *tmock.MockTime
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		fileMgr: fmock.NewMockFileMgr(ctrl),
		page:    fmock.NewMockPage(ctrl),
		block:   fmock.NewMockBlockId(ctrl),
		logMgr:  lmock.NewMockLogMgr(ctrl),
		time:    tmock.NewMockTime(ctrl),
	}
}

func TestNewBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm := fmock.NewMockFileMgr(ctrl)
	lm := lmock.NewMockLogMgr(ctrl)

	b := NewBuffer(fm, lm, 400)

	assert.NotNil(t, b)
	assert.Equal(t, fm, b.fileMgr)
	assert.Equal(t, lm, b.logMgr)
	assert.NotNil(t, b.contents)
	assert.Nil(t, b.block)
	assert.Equal(t, 0, b.pins)
	assert.Equal(t, -1, b.txNum)
	assert.Equal(t, -1, b.lsn)
}

func TestBuffer_IsPinned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm := fmock.NewMockFileMgr(ctrl)
	lm := lmock.NewMockLogMgr(ctrl)

	b := NewBuffer(fm, lm, 400)

	assert.False(t, b.IsPinned())
	b.pins++
	assert.True(t, b.IsPinned())
}

func TestBuffer_WriteContents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm := fmock.NewMockFileMgr(ctrl)
	lm := lmock.NewMockLogMgr(ctrl)

	b := NewBuffer(fm, lm, 400)

	b.WriteContents(1, 2, func(p file.ReadWritePage) {
		p.SetInt(0, 1)
	})
	assert.Equal(t, uint32(1), b.contents.GetInt(0))
	assert.Equal(t, 1, b.txNum)
	assert.Equal(t, 2, b.lsn)
}

func TestBuffer_AssignToBlock(t *testing.T) {
	const (
		blockSize = 400
		tx        = 1
		lsn       = 2
	)
	tests := []struct {
		name   string
		setup  func(m *mocks, b *BufferImpl)
		expect func(res error, b *BufferImpl)
	}{
		{
			name: "assign",
			setup: func(m *mocks, b *BufferImpl) {
				m.fileMgr.EXPECT().Read(nil, gomock.Any()).Return(nil)
			},
			expect: func(res error, b *BufferImpl) {
				assert.Nil(t, res)
				assert.Equal(t, 0, b.pins)
				assert.Equal(t, -1, b.txNum)
			},
		},
		{
			name: "flush and assign",
			setup: func(m *mocks, b *BufferImpl) {
				b.txNum = tx
				b.lsn = lsn
				m.logMgr.EXPECT().Flush(lsn).Return(nil)
				m.fileMgr.EXPECT().Write(nil, gomock.Any()).Return(nil)
				m.fileMgr.EXPECT().Read(nil, gomock.Any()).Return(nil)
			},
			expect: func(res error, b *BufferImpl) {
				assert.Nil(t, res)
				assert.Equal(t, 0, b.pins)
				assert.Equal(t, -1, b.txNum)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			m := newMocks(ctrl)
			b := NewBuffer(m.fileMgr, m.logMgr, blockSize)
			tt.setup(m, b)

			err := b.AssignToBlock(m.block)
			tt.expect(err, b)
		})
	}
}
