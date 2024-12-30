package transaction

import (
	"testing"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/stretchr/testify/assert"
)

func TestConcurrency_NewConcurrencyMgr(t *testing.T) {
	t.Parallel()
	cm := NewConcurrencyMgr()
	assert.Equal(t, 0, len(cm.Locks))
}

func TestConcurrencyMgr_SLock(t *testing.T) {
	t.Parallel()
	const filename = "test_concurrency_slock"
	concurMgr := NewConcurrencyMgr()
	block1 := file.NewBlockId(filename, 1)
	err := concurMgr.SLock(block1)

	// 1st SLock
	assert.NoError(t, err)
	assert.Equal(t, 1, len(concurMgr.Locks))
	assert.Equal(t, LOCK_TYPE_S, concurMgr.Locks[block1])

	// 2nd SLock on the same block
	err = concurMgr.SLock(block1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(concurMgr.Locks))
	assert.Equal(t, LOCK_TYPE_S, concurMgr.Locks[block1])

	concurMgr.Release()
	assert.Equal(t, 0, len(concurMgr.Locks))
}

func TestConcurrencyMgr_XLock(t *testing.T) {
	t.Parallel()
	t.Run("XLock", func(t *testing.T) {
		t.Parallel()
		const filename = "test_concurrency_xlock"
		concurMgr := NewConcurrencyMgr()
		block1 := file.NewBlockId(filename, 1)
		assert.False(t, concurMgr.HasXLock(block1))
		err := concurMgr.XLock(block1)

		// 1st XLock
		assert.NoError(t, err)
		assert.True(t, concurMgr.HasXLock(block1))
		assert.Equal(t, 1, len(concurMgr.Locks))
		assert.Equal(t, LOCK_TYPE_X, concurMgr.Locks[block1])

		// 2nd XLock on the same block
		err = concurMgr.XLock(block1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(concurMgr.Locks))
		assert.Equal(t, LOCK_TYPE_X, concurMgr.Locks[block1])

		concurMgr.Release()
		assert.Equal(t, 0, len(concurMgr.Locks))
	})
}
