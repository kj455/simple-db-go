package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	bmock "github.com/kj455/db/pkg/buffer/mock"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	bmmock "github.com/kj455/db/pkg/buffer_mgr/mock"
	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	"github.com/kj455/db/pkg/log"
	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/kj455/db/pkg/testutil"
	tmock "github.com/kj455/db/pkg/time/mock"
	txmock "github.com/kj455/db/pkg/tx/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type mocks struct {
	page        *fmock.MockPage
	block       *fmock.MockBlockId
	block2      *fmock.MockBlockId
	block3      *fmock.MockBlockId
	fileMgr     *fmock.MockFileMgr
	logMgr      *lmock.MockLogMgr
	logIter     *lmock.MockLogIterator
	buffer      *bmock.MockBuffer
	bufferMgr   *bmmock.MockBufferMgr
	tx          *txmock.MockTransaction
	lock        *txmock.MockLock
	txNumGen    *txmock.MockTxNumberGenerator
	recoveryMgr *txmock.MockRecoveryMgr
	concurMgr   *txmock.MockConcurrencyMgr
	buffList    *txmock.MockBufferList
	time        *tmock.MockTime
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		page:        fmock.NewMockPage(ctrl),
		block:       fmock.NewMockBlockId(ctrl),
		block2:      fmock.NewMockBlockId(ctrl),
		block3:      fmock.NewMockBlockId(ctrl),
		fileMgr:     fmock.NewMockFileMgr(ctrl),
		logMgr:      lmock.NewMockLogMgr(ctrl),
		logIter:     lmock.NewMockLogIterator(ctrl),
		buffer:      bmock.NewMockBuffer(ctrl),
		bufferMgr:   bmmock.NewMockBufferMgr(ctrl),
		tx:          txmock.NewMockTransaction(ctrl),
		lock:        txmock.NewMockLock(ctrl),
		txNumGen:    txmock.NewMockTxNumberGenerator(ctrl),
		recoveryMgr: txmock.NewMockRecoveryMgr(ctrl),
		concurMgr:   txmock.NewMockConcurrencyMgr(ctrl),
		buffList:    txmock.NewMockBufferList(ctrl),
		time:        tmock.NewMockTime(ctrl),
	}
}

const txNum = 1

func newMockTransaction(m *mocks) *TransactionImpl {
	return &TransactionImpl{
		recoveryMgr: m.recoveryMgr,
		concurMgr:   m.concurMgr,
		buffs:       m.buffList,
		bm:          m.bufferMgr,
		fm:          m.fileMgr,
		txNum:       txNum,
	}
}

func TestTransaction_Integration(t *testing.T) {
	t.Parallel()
	const (
		filename    = "testfile"
		logFilename = "testlogfile"
		blockSize   = 400
	)
	rootDir := testutil.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fm, logFilename)
	require.NoError(t, err)

	assert.NoError(t, err)
	const buffNum = 2
	buffs := make([]buffer.Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := NewTxNumberGenerator()

	tx1, err := NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)

	block := file.NewBlockId(filename, 0)
	tx1.Pin(block)
	tx1.SetInt(block, 80, 1, false)
	tx1.SetString(block, 40, "one", false)
	tx1.Commit()

	tx2, err := NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)
	tx2.Pin(block)
	intVal, err := tx2.GetInt(block, 80)
	assert.NoError(t, err)
	assert.Equal(t, 1, intVal)
	strVal, err := tx2.GetString(block, 40)
	assert.NoError(t, err)
	assert.Equal(t, "one", strVal)
	tx2.Commit()

	tx3, err := NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)
	tx3.Pin(block)
	tx3.SetInt(block, 80, 9999, true)
	intVal, err = tx3.GetInt(block, 80)
	assert.NoError(t, err)
	assert.Equal(t, 9999, intVal)
	tx3.Rollback()

	tx4, err := NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)
	tx4.Pin(block)
	intVal, err = tx4.GetInt(block, 4)
	assert.NoError(t, err)
	assert.Equal(t, 0, intVal)
	tx4.Commit()
}

func TestTransaction_NewTransaction(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.txNumGen.EXPECT().Next().Return(1)
	m.logMgr.EXPECT().Append(gomock.Any()).Return(1, nil)

	tx, err := NewTransaction(m.fileMgr, m.logMgr, m.bufferMgr, m.txNumGen)

	assert.NoError(t, err)
	assert.NotNil(t, tx)
	assert.Equal(t, 1, tx.txNum)
}

func TestTransaction_Commit(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.recoveryMgr.EXPECT().Commit().Return(nil)
	m.concurMgr.EXPECT().Release()
	m.buffList.EXPECT().UnpinAll()
	tx := newMockTransaction(m)

	err := tx.Commit()

	assert.NoError(t, err)
}

func TestTransaction_Rollback(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.recoveryMgr.EXPECT().Rollback().Return(nil)
	m.concurMgr.EXPECT().Release()
	m.buffList.EXPECT().UnpinAll()
	tx := newMockTransaction(m)

	err := tx.Rollback()

	assert.NoError(t, err)
}

func TestTransaction_Recover(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
	m.recoveryMgr.EXPECT().Recover().Return(nil)
	tx := newMockTransaction(m)

	err := tx.Recover()

	assert.NoError(t, err)
}

func TestTransaction_Pin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.buffList.EXPECT().Pin(m.block)
	tx := newMockTransaction(m)

	tx.Pin(m.block)
}

func TestTransaction_Unpin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.buffList.EXPECT().Unpin(m.block)
	tx := newMockTransaction(m)

	tx.Unpin(m.block)
}

func TestTransaction_GetInt(t *testing.T) {
	t.Parallel()
	const (
		offset = 0
		intVal = 1
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.concurMgr.EXPECT().SLock(m.block).Return(nil)
	m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
	m.buffer.EXPECT().Contents().Return(m.page)
	m.page.EXPECT().GetInt(offset).Return(uint32(intVal))
	tx := newMockTransaction(m)

	got, err := tx.GetInt(m.block, offset)

	assert.NoError(t, err)
	assert.Equal(t, intVal, got)
}

func TestTransaction_GetString(t *testing.T) {
	t.Parallel()
	const (
		offset = 0
		strVal = "str"
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.concurMgr.EXPECT().SLock(m.block).Return(nil)
	m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
	m.buffer.EXPECT().Contents().Return(m.page)
	m.page.EXPECT().GetString(offset).Return(strVal)
	tx := newMockTransaction(m)

	got, err := tx.GetString(m.block, offset)

	assert.NoError(t, err)
	assert.Equal(t, strVal, got)
}

func TestTransaction_SetInt(t *testing.T) {
	t.Parallel()
	const (
		offset = 0
		intVal = 1
		lsn    = 2
	)
	tests := []struct {
		name    string
		okToLog bool
		setup   func(*mocks)
	}{
		{
			name:    "okToLog",
			okToLog: true,
			setup: func(m *mocks) {
				m.concurMgr.EXPECT().XLock(m.block).Return(nil)
				m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
				m.recoveryMgr.EXPECT().SetInt(m.buffer, offset, intVal).Return(lsn, nil)
				m.buffer.EXPECT().WriteContents(txNum, lsn, gomock.Any())
			},
		},
		{
			name:    "not okToLog",
			okToLog: false,
			setup: func(m *mocks) {
				m.concurMgr.EXPECT().XLock(m.block).Return(nil)
				m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
				m.buffer.EXPECT().WriteContents(txNum, -1, gomock.Any())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			tx := newMockTransaction(m)
			tt.setup(m)

			tx.SetInt(m.block, offset, intVal, tt.okToLog)

			assert.NoError(t, nil)
		})
	}
}

func TestTransaction_SetString(t *testing.T) {
	t.Parallel()
	const (
		offset = 0
		strVal = "str"
		lsn    = 1
	)
	tests := []struct {
		name    string
		okToLog bool
		setup   func(*mocks)
	}{
		{
			name:    "okToLog",
			okToLog: true,
			setup: func(m *mocks) {
				m.concurMgr.EXPECT().XLock(m.block)
				m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
				m.recoveryMgr.EXPECT().SetString(m.buffer, offset, strVal).Return(lsn, nil)
				m.buffer.EXPECT().WriteContents(txNum, lsn, gomock.Any())
			},
		},
		{
			name:    "not okToLog",
			okToLog: false,
			setup: func(m *mocks) {
				m.concurMgr.EXPECT().XLock(m.block)
				m.buffList.EXPECT().GetBuffer(m.block).Return(m.buffer, true)
				m.buffer.EXPECT().WriteContents(txNum, -1, gomock.Any())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			tx := newMockTransaction(m)
			tt.setup(m)

			tx.SetString(m.block, offset, strVal, tt.okToLog)

			assert.NoError(t, nil)
		})
	}
}

func TestTransaction_Size(t *testing.T) {
	t.Parallel()
	const filename = "file"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.concurMgr.EXPECT().SLock(gomock.Any()).Return(nil)
	m.fileMgr.EXPECT().Length(filename).Return(1, nil)
	tx := newMockTransaction(m)

	got, err := tx.Size(filename)

	assert.NoError(t, err)
	assert.Equal(t, 1, got)
}

func TestTransaction_Append(t *testing.T) {
	t.Parallel()
	const filename = "file"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.concurMgr.EXPECT().XLock(gomock.Any()).Return(nil)
	m.fileMgr.EXPECT().Append(filename).Return(m.block, nil)
	tx := newMockTransaction(m)

	got, err := tx.Append(filename)

	assert.NoError(t, err)
	assert.Equal(t, m.block, got)
}

func TestTransaction_BlockSize(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.fileMgr.EXPECT().BlockSize().Return(0)
	tx := newMockTransaction(m)

	got := tx.BlockSize()

	assert.Equal(t, 0, got)
}

func TestTransactionImpl_AvailableBuffs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.bufferMgr.EXPECT().AvailableNum().Return(0)
	tx := newMockTransaction(m)

	got := tx.AvailableBuffs()

	assert.Equal(t, 0, got)
}
