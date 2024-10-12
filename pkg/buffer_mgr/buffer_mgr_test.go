package buffermgr

import (
	"os"
	"testing"
	"time"

	"github.com/kj455/db/pkg/buffer"
	bmock "github.com/kj455/db/pkg/buffer/mock"
	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	"github.com/kj455/db/pkg/log"
	lmock "github.com/kj455/db/pkg/log/mock"
	tmock "github.com/kj455/db/pkg/time/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestNewBufferMgr(t *testing.T) {
	const (
		blockSize = 4096
		buffNum   = 3
		waitTime  = 1 * time.Second
	)
	ctrl := gomock.NewController(t)
	tm := tmock.NewMockTime(ctrl)
	lm := lmock.NewMockLogMgr(ctrl)
	fm := fmock.NewMockFileMgr(ctrl)
	buffers := make([]buffer.Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		buffers[i] = buffer.NewBuffer(fm, lm, blockSize)
	}

	bufferMgr := NewBufferMgr(buffers, WithMaxWaitTime(waitTime), WithTime(tm), WithMaxWaitTime(waitTime))

	assert.NotNil(t, bufferMgr)
	assert.Equal(t, buffNum, bufferMgr.AvailableNum())
	assert.Equal(t, buffNum, len(bufferMgr.pool))
	assert.Equal(t, waitTime, bufferMgr.maxWaitTime)
}

func TestBufferFile(t *testing.T) {
	rootDir := "/tmp"
	dir := rootDir + "/.tmp"
	os.MkdirAll(dir, os.ModePerm)
	defer os.RemoveAll(rootDir)

	fm := file.NewFileMgr(dir, 400)
	lm, err := log.NewLogMgr(fm, "testlogfile")
	require.NoError(t, err)
	buffs := make([]buffer.Buffer, 3)
	for i := 0; i < 3; i++ {
		buffs[i] = buffer.NewBuffer(fm, lm, fm.BlockSize())
	}
	bm := NewBufferMgr(buffs)
	blk := file.NewBlockId("testfile", 2)
	pos1 := 88

	b1, err := bm.Pin(blk)
	require.NoError(t, err)
	b1.WriteContents(1, 0, func(p buffer.ReadWritePage) {
		p.SetString(pos1, "abcdefghijklm")
	})
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	b1.WriteContents(1, 0, func(p buffer.ReadWritePage) {
		p.SetInt(pos2, 345)
	})
	bm.Unpin(b1)

	b2, err := bm.Pin(blk)
	require.NoError(t, err)
	p2 := b2.Contents()
	assert.Equal(t, "abcdefghijklm", p2.GetString(pos1))
	assert.Equal(t, uint32(345), p2.GetInt(pos2))
	bm.Unpin(b2)
}

func TestBufferMgrImpl_Pin(t *testing.T) {
	const (
		blockSize = 4096
		buffNum   = 3
	)
	now := time.Date(2024, 5, 25, 0, 0, 0, 0, time.UTC)
	waitTime := 1 * time.Second
	tests := []struct {
		name   string
		setup  func(m *mocks, buffs []*bmock.MockBuffer)
		expect func(t *testing.T, bm *BufferMgrImpl, buff buffer.Buffer, err error)
	}{
		{
			name: "success - no buffer assigned with block",
			setup: func(m *mocks, buffs []*bmock.MockBuffer) {
				m.time.EXPECT().Now().Return(now)
				for i := 0; i < len(buffs); i++ {
					buffs[i].EXPECT().Block().Return(nil)
				}
				buffs[0].EXPECT().IsPinned().Return(true)
				buffs[1].EXPECT().IsPinned().Return(false)
				buffs[1].EXPECT().AssignToBlock(gomock.Any()).Return(nil)
				buffs[1].EXPECT().IsPinned().Return(false)
				buffs[1].EXPECT().Pin().Return()
			},
			expect: func(t *testing.T, bm *BufferMgrImpl, buff buffer.Buffer, err error) {
				assert.NotNil(t, buff)
				assert.NoError(t, err)
				assert.Equal(t, buffNum-1, bm.AvailableNum())
			},
		},
		{
			name: "success - buffer already assigned with block",
			setup: func(m *mocks, buffs []*bmock.MockBuffer) {
				m.time.EXPECT().Now().Return(now)
				buffs[0].EXPECT().Block().Return(nil)
				buffs[1].EXPECT().Block().Return(m.block)
				m.block.EXPECT().Equals(gomock.Any()).Return(true)
				buffs[1].EXPECT().IsPinned().Return(false)
				buffs[1].EXPECT().Pin().Return()
			},
			expect: func(t *testing.T, bm *BufferMgrImpl, buff buffer.Buffer, err error) {
				assert.NotNil(t, buff)
				assert.NoError(t, err)
				assert.Equal(t, buffNum-1, bm.AvailableNum())
			},
		},
		{
			name: "success - already pinned",
			setup: func(m *mocks, buffs []*bmock.MockBuffer) {
				m.time.EXPECT().Now().Return(now)
				for i := 0; i < len(buffs); i++ {
					buffs[i].EXPECT().Block().Return(nil)
				}
				buffs[0].EXPECT().IsPinned().Return(true)
				buffs[1].EXPECT().IsPinned().Return(false)
				buffs[1].EXPECT().AssignToBlock(gomock.Any()).Return(nil)
				buffs[1].EXPECT().IsPinned().Return(true)
				buffs[1].EXPECT().Pin().Return()
			},
			expect: func(t *testing.T, bm *BufferMgrImpl, buff buffer.Buffer, err error) {
				assert.NotNil(t, buff)
				assert.NoError(t, err)
				assert.Equal(t, buffNum, bm.AvailableNum())
			},
		},
		{
			name: "fail - no available buffer",
			setup: func(m *mocks, buffs []*bmock.MockBuffer) {
				m.time.EXPECT().Now().Return(now)
				for i := 0; i < len(buffs); i++ {
					buffs[i].EXPECT().Block().Return(nil)
					buffs[i].EXPECT().IsPinned().Return(true)
				}
				m.time.EXPECT().Since(now).Return(waitTime + 1)
			},
			expect: func(t *testing.T, bm *BufferMgrImpl, buff buffer.Buffer, err error) {
				assert.Error(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newMocks(ctrl)
			buffs := make([]buffer.Buffer, buffNum)
			mockBuffs := make([]*bmock.MockBuffer, buffNum)
			for i := 0; i < buffNum; i++ {
				mb := bmock.NewMockBuffer(ctrl)
				buffs[i] = mb
				mockBuffs[i] = mb
			}
			bm := NewBufferMgr(buffs, WithTime(m.time), WithMaxWaitTime(waitTime))
			tt.setup(m, mockBuffs)

			buff, err := bm.Pin(m.block)

			tt.expect(t, bm, buff, err)
		})
	}
}

func TestBufferMgrImpl_Unpin(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(bm *BufferMgrImpl, b *bmock.MockBuffer)
		expect func(t *testing.T, bm *BufferMgrImpl)
	}{
		{
			name: "success - buffer is pinned",
			setup: func(bm *BufferMgrImpl, b *bmock.MockBuffer) {
				bm.availableNum = 0
				b.EXPECT().Unpin().Return()
				b.EXPECT().IsPinned().Return(true)
			},
			expect: func(t *testing.T, bm *BufferMgrImpl) {
				assert.Equal(t, 0, bm.availableNum)
			},
		},
		{
			name: "success - buffer is unpinned",
			setup: func(bm *BufferMgrImpl, b *bmock.MockBuffer) {
				bm.availableNum = 0
				b.EXPECT().Unpin().Return()
				b.EXPECT().IsPinned().Return(false)
			},
			expect: func(t *testing.T, bm *BufferMgrImpl) {
				assert.Equal(t, 1, bm.availableNum)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			buff := bmock.NewMockBuffer(ctrl)
			bm := NewBufferMgr([]buffer.Buffer{buff})
			tt.setup(bm, buff)

			bm.Unpin(buff)

			tt.expect(t, bm)
		})
	}
}
