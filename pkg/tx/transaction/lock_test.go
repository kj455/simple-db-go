package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/kj455/db/pkg/file"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewLock(t *testing.T) {
	t.Parallel()
	t.Run("default", func(t *testing.T) {
		l := NewLock()
		assert.NotNil(t, l)
		assert.Equal(t, DEFAULT_MAX_WAIT_TIME, l.maxWaitTime)
	})
	t.Run("custom", func(t *testing.T) {
		l := NewLock(WithWaitTime(time.Duration(5)))
		assert.NotNil(t, l)
		assert.Equal(t, time.Duration(5), l.maxWaitTime)
	})
}

func newMockLock(m *mocks) *LockImpl {
	l := &LockImpl{
		time:  m.time,
		locks: make(map[file.BlockId]lockState),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func TestSLock(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 27, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		setup     func(*mocks, *LockImpl)
		expect    func(l *LockImpl, b file.BlockId)
		expectErr bool
	}{
		{
			name: "success - no X lock",
			setup: func(m *mocks, l *LockImpl) {
				m.time.EXPECT().Now().Return(now)
			},
			expect: func(l *LockImpl, b file.BlockId) {
				assert.Equal(t, 1, len(l.locks))
				assert.Equal(t, lockState(1), l.locks[b])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			l := newMockLock(m)
			tt.setup(m, l)
			err := l.SLock(m.block)
			tt.expect(l, m.block)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestSLock_Wait(t *testing.T) {
	const maxWaitTime = 1 * time.Second
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	now := time.Date(2024, 5, 27, 0, 0, 0, 0, time.UTC)
	m.time.EXPECT().Now().Return(now).AnyTimes()
	m.time.EXPECT().Since(now).Return(maxWaitTime + 1)
	l := NewLock(WithTime(m.time))

	// XLock を取得しておく
	err := l.XLock(m.block)
	assert.NoError(t, err)

	// SLock の取得を試みるが、XLock が解放されるまで待機
	done := make(chan bool)
	go func() {
		err := l.SLock(m.block)
		assert.NoError(t, err)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond) // 少し待機

	go func() {
		l.Unlock(m.block) // 別のゴルーチンで XLock を解放
	}()

	select {
	case <-done:
		// SLock が取得できた場合
		assert.Equal(t, 1, len(l.locks))
		assert.Equal(t, lockState(1), l.locks[m.block])
	case <-time.After(1 * time.Second):
		t.Fatal("SLock did not proceed in time")
	}
}

func TestXLock(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 27, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		setup     func(*mocks, *LockImpl)
		expect    func(l *LockImpl, b file.BlockId)
		expectErr bool
	}{
		{
			name: "success - no S lock",
			setup: func(m *mocks, l *LockImpl) {
				m.time.EXPECT().Now().Return(now)
			},
			expect: func(l *LockImpl, b file.BlockId) {
				assert.Equal(t, 1, len(l.locks))
				assert.Equal(t, LOCK_STATE_X_LOCKED, l.locks[b])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			l := newMockLock(m)
			tt.setup(m, l)
			err := l.XLock(m.block)
			tt.expect(l, m.block)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestXLock_Wait(t *testing.T) {
	const (
		lockNum     = 2
		maxWaitTime = 1 * time.Second
	)
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	now := time.Date(2024, 5, 27, 0, 0, 0, 0, time.UTC)
	m.time.EXPECT().Now().Return(now).AnyTimes()
	m.time.EXPECT().Since(now).Return(maxWaitTime + 1)
	l := NewLock(WithTime(m.time))

	// SLock を2つ取得しておく
	for i := 0; i < lockNum; i++ {
		err := l.SLock(m.block)
		assert.NoError(t, err)
	}

	// XLock の取得を試みるが、SLock が解放されるまで待機
	done := make(chan bool)
	go func() {
		err := l.XLock(m.block)
		assert.NoError(t, err)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond) // 少し待機

	// SLock を解放し、Broadcast する
	for i := 0; i < lockNum; i++ {
		l.Unlock(m.block)
	}

	select {
	case <-done:
		// XLock が取得できた場合
		assert.Equal(t, 1, len(l.locks))
		assert.Equal(t, LOCK_STATE_X_LOCKED, l.locks[m.block])
	case <-time.After(1 * time.Second):
		t.Fatal("XLock did not proceed in time")
	}
}

func TestUnlock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(*mocks, *LockImpl)
		expect func(l *LockImpl, b file.BlockId)
	}{
		{
			name: "success - multiple S lock",
			setup: func(m *mocks, l *LockImpl) {
				l.locks[m.block] = 2
			},
			expect: func(l *LockImpl, b file.BlockId) {
				assert.Equal(t, 1, len(l.locks))
				assert.Equal(t, lockState(1), l.locks[b])
			},
		},
		{
			name: "success - single S lock",
			setup: func(m *mocks, l *LockImpl) {
				l.locks[m.block] = 1
			},
			expect: func(l *LockImpl, b file.BlockId) {
				assert.Equal(t, 0, len(l.locks))
				assert.Equal(t, LOCK_STATE_UNLOCKED, l.locks[b])
			},
		},
		{
			name:  "error - no lock",
			setup: func(m *mocks, l *LockImpl) {},
			expect: func(l *LockImpl, b file.BlockId) {
				assert.Equal(t, 0, len(l.locks))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			l := newMockLock(m)
			tt.setup(m, l)
			l.Unlock(m.block)
			tt.expect(l, m.block)
		})
	}
}