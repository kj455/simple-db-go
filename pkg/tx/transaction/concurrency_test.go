package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConcurrency_NewConcurrencyMgr(t *testing.T) {
	t.Parallel()
	cm := NewConcurrencyMgr()
	assert.Equal(t, 0, len(cm.Locks))
}

func newMockConcurrencyMgr(m *mocks) *ConcurrencyMgrImpl {
	return &ConcurrencyMgrImpl{
		l:     m.lock,
		Locks: make(map[file.BlockId]LockType),
	}
}

func TestConcurrency_SLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *ConcurrencyMgrImpl)
		expect func(*ConcurrencyMgrImpl, file.BlockId)
	}{
		{
			name: "SLock",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {
				m.lock.EXPECT().SLock(m.block).Return(nil)
			},
			expect: func(cm *ConcurrencyMgrImpl, b file.BlockId) {
				assert.Equal(t, 1, len(cm.Locks))
				assert.Equal(t, LOCK_TYPE_SLOCK, cm.Locks[b])
			},
		},
		{
			name: "already SLocked",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {
				cm.Locks[m.block] = LOCK_TYPE_SLOCK
			},
			expect: func(cm *ConcurrencyMgrImpl, b file.BlockId) {
				assert.Equal(t, 1, len(cm.Locks))
				assert.Equal(t, LOCK_TYPE_SLOCK, cm.Locks[b])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			cm := newMockConcurrencyMgr(m)
			tt.setup(m, cm)
			err := cm.SLock(m.block)
			assert.NoError(t, err)
			tt.expect(cm, m.block)
		})
	}
}

func TestConcurrency_XLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *ConcurrencyMgrImpl)
		expect func(*ConcurrencyMgrImpl, file.BlockId)
	}{
		{
			name: "XLock",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {
				m.lock.EXPECT().SLock(m.block).Return(nil)
				m.lock.EXPECT().XLock(m.block).Return(nil)
			},
			expect: func(cm *ConcurrencyMgrImpl, b file.BlockId) {
				assert.Equal(t, 1, len(cm.Locks))
				assert.Equal(t, LOCK_TYPE_XLOCK, cm.Locks[b])
			},
		},
		{
			name: "already XLocked",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {
				cm.Locks[m.block] = LOCK_TYPE_XLOCK
			},
			expect: func(cm *ConcurrencyMgrImpl, b file.BlockId) {
				assert.Equal(t, 1, len(cm.Locks))
				assert.Equal(t, LOCK_TYPE_XLOCK, cm.Locks[b])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			cm := newMockConcurrencyMgr(m)
			tt.setup(m, cm)
			err := cm.XLock(m.block)
			assert.NoError(t, err)
			tt.expect(cm, m.block)
		})
	}
}

func TestConcurrency_Release(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *ConcurrencyMgrImpl)
		expect func(*ConcurrencyMgrImpl)
	}{
		{
			name: "release",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {
				cm.Locks[m.block] = LOCK_TYPE_XLOCK
				m.lock.EXPECT().Unlock(m.block)
			},
			expect: func(cm *ConcurrencyMgrImpl) {
				assert.Equal(t, 0, len(cm.Locks))
			},
		},
		{
			name:  "empty",
			setup: func(m *mocks, cm *ConcurrencyMgrImpl) {},
			expect: func(cm *ConcurrencyMgrImpl) {
				assert.Equal(t, 0, len(cm.Locks))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			cm := newMockConcurrencyMgr(m)
			tt.setup(m, cm)
			cm.Release()
			tt.expect(cm)
		})
	}
}
