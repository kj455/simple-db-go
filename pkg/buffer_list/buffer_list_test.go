package bufferlist

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	bmock "github.com/kj455/db/pkg/buffer/mock"
	bmmock "github.com/kj455/db/pkg/buffer_mgr/mock"
	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestBufferList_NewBufferList(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bm := bmmock.NewMockBufferMgr(ctrl)
	bl := NewBufferList(bm)
	assert.Equal(t, 0, len(bl.buffers))
	assert.Equal(t, 0, len(bl.pins))
	assert.Equal(t, bm, bl.bm)
}

type mocks struct {
	bm     *bmmock.MockBufferMgr
	buffer *bmock.MockBuffer
	block  *fmock.MockBlockId
	block2 *fmock.MockBlockId
	block3 *fmock.MockBlockId
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		bm:     bmmock.NewMockBufferMgr(ctrl),
		buffer: bmock.NewMockBuffer(ctrl),
		block:  fmock.NewMockBlockId(ctrl),
		block2: fmock.NewMockBlockId(ctrl),
		block3: fmock.NewMockBlockId(ctrl),
	}
}

func newMockBufferList(m *mocks) *BufferListImpl {
	return &BufferListImpl{
		buffers: make(map[file.BlockId]buffer.Buffer),
		pins:    make([]file.BlockId, 0),
		bm:      m.bm,
	}
}

func TestBufferList_GetBuffer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *BufferListImpl)
		expect func(*testing.T, buffer.Buffer, bool)
	}{
		{
			name: "found",
			setup: func(m *mocks, bl *BufferListImpl) {
				bl.buffers[m.block] = m.buffer
			},
			expect: func(t *testing.T, buf buffer.Buffer, ok bool) {
				assert.NotNil(t, buf)
				assert.True(t, ok)
			},
		},
		{
			name:  "GetBuffer not found",
			setup: func(m *mocks, bl *BufferListImpl) {},
			expect: func(t *testing.T, buf buffer.Buffer, ok bool) {
				assert.Nil(t, buf)
				assert.False(t, ok)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			bl := newMockBufferList(m)
			tt.setup(m, bl)
			buf, ok := bl.GetBuffer(m.block)
			tt.expect(t, buf, ok)
		})
	}
}

func TestBufferList_Pin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *BufferListImpl)
		expect func(*mocks, *BufferListImpl, file.BlockId)
	}{
		{
			name: "Pin",
			setup: func(m *mocks, bl *BufferListImpl) {
				m.bm.EXPECT().Pin(m.block).Return(m.buffer, nil)
			},
			expect: func(m *mocks, bl *BufferListImpl, b file.BlockId) {
				assert.Equal(t, 1, len(bl.buffers))
				assert.Equal(t, 1, len(bl.pins))
				assert.Equal(t, m.buffer, bl.buffers[b])
				assert.Equal(t, b, bl.pins[0])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			bl := newMockBufferList(m)
			tt.setup(m, bl)
			bl.Pin(m.block)
			tt.expect(m, bl, m.block)
		})
	}
}

func TestBufferList_Unpin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *BufferListImpl)
		expect func(*mocks, *BufferListImpl, file.BlockId)
	}{
		{
			name: "Unpin",
			setup: func(m *mocks, bl *BufferListImpl) {
				bl.buffers[m.block] = m.buffer
				m.bm.EXPECT().Unpin(m.buffer)
				bl.pins = []file.BlockId{m.block, m.block2, m.block3}
				m.block.EXPECT().Equals(m.block).Return(true)
				m.block2.EXPECT().Equals(m.block).Return(false)
				m.block3.EXPECT().Equals(m.block).Return(false)
			},
			expect: func(m *mocks, bl *BufferListImpl, b file.BlockId) {
				assert.Equal(t, 2, len(bl.pins))
				assert.Equal(t, 0, len(bl.buffers))
			},
		},
		{
			name:  "not found",
			setup: func(m *mocks, bl *BufferListImpl) {},
			expect: func(m *mocks, bl *BufferListImpl, b file.BlockId) {
				assert.Equal(t, 0, len(bl.pins))
				assert.Equal(t, 0, len(bl.buffers))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			bl := newMockBufferList(m)
			tt.setup(m, bl)
			bl.Unpin(m.block)
			tt.expect(m, bl, m.block)
		})
	}
}

func TestBufferList_UnpinAll(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *BufferListImpl)
		expect func(*mocks, *BufferListImpl)
	}{
		{
			name: "UnpinAll",
			setup: func(m *mocks, bl *BufferListImpl) {
				bl.pins = []file.BlockId{m.block, m.block2, m.block3}
				bl.buffers[m.block] = m.buffer
				bl.buffers[m.block3] = m.buffer
				m.bm.EXPECT().Unpin(m.buffer).Times(2)
			},
			expect: func(m *mocks, bl *BufferListImpl) {
				assert.Equal(t, 0, len(bl.pins))
				assert.Equal(t, 0, len(bl.buffers))
			},
		},
		{
			name:  "UnpinAll empty",
			setup: func(m *mocks, bl *BufferListImpl) {},
			expect: func(m *mocks, bl *BufferListImpl) {
				assert.Equal(t, 0, len(bl.pins))
				assert.Equal(t, 0, len(bl.buffers))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			bl := newMockBufferList(m)
			tt.setup(m, bl)
			bl.UnpinAll()
			tt.expect(m, bl)
		})
	}
}
