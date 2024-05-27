package recovery

import (
	"fmt"
	"testing"

	bmock "github.com/kj455/db/pkg/buffer/mock"
	bmmock "github.com/kj455/db/pkg/buffer_mgr/mock"
	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/kj455/db/pkg/record"
	tmock "github.com/kj455/db/pkg/tx/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type mocks struct {
	fileMgr   *fmock.MockFileMgr
	buffer    *bmock.MockBuffer
	bufferMgr *bmmock.MockBufferMgr
	page      *fmock.MockPage
	block     *fmock.MockBlockId
	logMgr    *lmock.MockLogMgr
	logIter   *lmock.MockLogIterator
	tx        *tmock.MockTransaction
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		fileMgr:   fmock.NewMockFileMgr(ctrl),
		buffer:    bmock.NewMockBuffer(ctrl),
		bufferMgr: bmmock.NewMockBufferMgr(ctrl),
		page:      fmock.NewMockPage(ctrl),
		block:     fmock.NewMockBlockId(ctrl),
		logMgr:    lmock.NewMockLogMgr(ctrl),
		logIter:   lmock.NewMockLogIterator(ctrl),
		tx:        tmock.NewMockTransaction(ctrl),
	}
}

func TestNewRecoveryMgr(t *testing.T) {
	const txNum = 1
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.logMgr.EXPECT().Append(gomock.Any()).Return(1, nil)

	rm, err := NewRecoveryMgr(m.tx, txNum, m.logMgr, m.bufferMgr)

	assert.Nil(t, err)
	assert.NotNil(t, rm)
	assert.Equal(t, m.logMgr, rm.lm)
	assert.Equal(t, m.bufferMgr, rm.bm)
	assert.Equal(t, m.tx, rm.tx)
	assert.Equal(t, txNum, rm.txNum)
}

func TestRecoveryMgrImpl_Commit(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
	m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
	m.logMgr.EXPECT().Flush(lsn)
	rm := &RecoveryMgrImpl{
		lm:    m.logMgr,
		bm:    m.bufferMgr,
		tx:    m.tx,
		txNum: txNum,
	}

	err := rm.Commit()

	assert.Nil(t, err)
}

func newStartRecordBytes(t *testing.T, txNum int) []byte {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, uint32(record.START))
	p.SetInt(4, uint32(txNum))
	s := record.NewStartRecord(file.NewPageFromBytes(rec))
	str := fmt.Sprintf("<START %d>", txNum)
	assert.Equal(t, str, s.String())
	return rec
}

func newSetIntRecordBytes(t *testing.T, txNum int) []byte {
	const (
		filename = "test"
		blockNum = 0
		offset   = 0
		value    = 1
	)
	rec := make([]byte, 256)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, uint32(record.SET_INT))
	p.SetInt(4, uint32(txNum))
	p.SetString(8, filename)
	p.SetInt(8+file.MaxLength(len(filename)), uint32(blockNum))
	p.SetInt(8+file.MaxLength(len(filename))+4, uint32(offset))
	p.SetInt(8+file.MaxLength(len(filename))+8, uint32(value))
	si := record.NewSetIntRecord(file.NewPageFromBytes(rec))
	str := fmt.Sprintf("<SET_INT %d %s %d %d>", txNum, file.NewBlockId(filename, blockNum), offset, value)
	assert.Equal(t, str, si.String())
	return rec
}

func newCommitRecordBytes(t *testing.T, txNum int) []byte {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, uint32(record.COMMIT))
	p.SetInt(4, uint32(txNum))
	c := record.NewCommitRecord(file.NewPageFromBytes(rec))
	str := fmt.Sprintf("<COMMIT %d>", txNum)
	assert.Equal(t, str, c.String())
	return rec
}

func newCheckpointRecordBytes(t *testing.T) []byte {
	rec := make([]byte, 4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, uint32(record.CHECKPOINT))
	cp := record.NewCheckpointRecord()
	assert.Equal(t, "<CHECKPOINT>", cp.String())
	return rec
}

func TestRecoveryMgrImpl_Rollback(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	tests := []struct {
		name  string
		setup func(m *mocks)
	}{
		{
			name: "rollback - stopped by start record",
			setup: func(m *mocks) {
				// rollback
				m.logMgr.EXPECT().Iterator().Return(m.logIter, nil)
				// 1st iter - setInt
				m.logIter.EXPECT().HasNext().Return(true)
				setIntBytes := newSetIntRecordBytes(t, txNum)
				m.logIter.EXPECT().Next().Return(setIntBytes, nil)
				m.tx.EXPECT().Pin(gomock.Any())
				m.tx.EXPECT().SetInt(gomock.Any(), 0, 1, false)
				m.tx.EXPECT().Unpin(gomock.Any())
				// 2nd iter - other tx setInt - skip
				m.logIter.EXPECT().HasNext().Return(true)
				setIntBytes = newSetIntRecordBytes(t, txNum+1)
				m.logIter.EXPECT().Next().Return(setIntBytes, nil)
				// 3rd iter - start
				m.logIter.EXPECT().HasNext().Return(true)
				startBytes := newStartRecordBytes(t, txNum)
				m.logIter.EXPECT().Next().Return(startBytes, nil)
				// after rollback
				m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
				m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
				m.logMgr.EXPECT().Flush(lsn).Return(nil)
			},
		},
		{
			name: "rollback - stopped by no more records",
			setup: func(m *mocks) {
				m.logMgr.EXPECT().Iterator().Return(m.logIter, nil)
				m.logIter.EXPECT().HasNext().Return(false)
				m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
				m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
				m.logMgr.EXPECT().Flush(lsn).Return(nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			tt.setup(m)
			rm := &RecoveryMgrImpl{
				lm:    m.logMgr,
				bm:    m.bufferMgr,
				tx:    m.tx,
				txNum: txNum,
			}

			err := rm.Rollback()

			assert.Nil(t, err)
		})
	}
}

func TestRecoveryMgrImpl_Recover(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	tests := []struct {
		name  string
		setup func(m *mocks)
	}{
		{
			name: "recover - stopped by start record",
			setup: func(m *mocks) {
				// recover
				m.logMgr.EXPECT().Iterator().Return(m.logIter, nil)
				// uncommitted modification - undo
				m.logIter.EXPECT().HasNext().Return(true)
				m.logIter.EXPECT().Next().Return(newSetIntRecordBytes(t, txNum), nil)
				m.tx.EXPECT().Pin(gomock.Any())
				m.tx.EXPECT().SetInt(gomock.Any(), gomock.Any(), gomock.Any(), false)
				m.tx.EXPECT().Unpin(gomock.Any())
				// commit
				m.logIter.EXPECT().HasNext().Return(true)
				commitBytes := newCommitRecordBytes(t, txNum)
				m.logIter.EXPECT().Next().Return(commitBytes, nil)
				// setInt
				m.logIter.EXPECT().HasNext().Return(true)
				m.logIter.EXPECT().Next().Return(newSetIntRecordBytes(t, txNum), nil)
				// checkpoint
				m.logIter.EXPECT().HasNext().Return(true)
				m.logIter.EXPECT().Next().Return(newCheckpointRecordBytes(t), nil)
				// after recover
				m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
				m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
				m.logMgr.EXPECT().Flush(lsn).Return(nil)
			},
		},
		{
			name: "recover - stopped by no more records",
			setup: func(m *mocks) {
				m.logMgr.EXPECT().Iterator().Return(m.logIter, nil)
				m.logIter.EXPECT().HasNext().Return(false)
				m.bufferMgr.EXPECT().FlushAll(txNum).Return(nil)
				m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
				m.logMgr.EXPECT().Flush(lsn).Return(nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newMocks(ctrl)
			tt.setup(m)
			rm := &RecoveryMgrImpl{
				lm:    m.logMgr,
				bm:    m.bufferMgr,
				tx:    m.tx,
				txNum: txNum,
			}

			err := rm.Recover()

			assert.Nil(t, err)
		})
	}
}

func TestRecoveryMgrImpl_SetInt(t *testing.T) {
	const (
		txNum  = 1
		lsn    = 2
		offset = 0
		val    = 99
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.buffer.EXPECT().Block().Return(m.block)
	m.block.EXPECT().Filename().Return("test").AnyTimes()
	m.block.EXPECT().Number().Return(0)
	m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
	rm := &RecoveryMgrImpl{
		lm:    m.logMgr,
		bm:    m.bufferMgr,
		tx:    m.tx,
		txNum: txNum,
	}

	got, err := rm.SetInt(m.buffer, offset, val)

	assert.Equal(t, lsn, got)
	assert.NoError(t, err)
}

func TestRecoveryMgrImpl_SetString(t *testing.T) {
	const (
		txNum  = 1
		lsn    = 2
		offset = 0
		val    = "test"
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newMocks(ctrl)
	m.buffer.EXPECT().Block().Return(m.block)
	m.block.EXPECT().Filename().Return("test").AnyTimes()
	m.block.EXPECT().Number().Return(0)
	m.logMgr.EXPECT().Append(gomock.Any()).Return(lsn, nil)
	rm := &RecoveryMgrImpl{
		lm:    m.logMgr,
		bm:    m.bufferMgr,
		tx:    m.tx,
		txNum: txNum,
	}

	got, err := rm.SetString(m.buffer, offset, val)

	assert.Equal(t, lsn, got)
	assert.NoError(t, err)
}
