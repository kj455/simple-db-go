package transaction

import (
	bmock "github.com/kj455/db/pkg/buffer/mock"
	bmmock "github.com/kj455/db/pkg/buffer_mgr/mock"
	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	tmock "github.com/kj455/db/pkg/time/mock"
	txmock "github.com/kj455/db/pkg/tx/mock"
	"go.uber.org/mock/gomock"
)

type mocks struct {
	page      *fmock.MockPage
	block     *fmock.MockBlockId
	block2    *fmock.MockBlockId
	block3    *fmock.MockBlockId
	fileMgr   *fmock.MockFileMgr
	logMgr    *lmock.MockLogMgr
	logIter   *lmock.MockLogIterator
	buffer    *bmock.MockBuffer
	bufferMgr *bmmock.MockBufferMgr
	tx        *txmock.MockTransaction
	lock      *txmock.MockLock
	time      *tmock.MockTime
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		page:      fmock.NewMockPage(ctrl),
		block:     fmock.NewMockBlockId(ctrl),
		block2:    fmock.NewMockBlockId(ctrl),
		block3:    fmock.NewMockBlockId(ctrl),
		fileMgr:   fmock.NewMockFileMgr(ctrl),
		logMgr:    lmock.NewMockLogMgr(ctrl),
		logIter:   lmock.NewMockLogIterator(ctrl),
		buffer:    bmock.NewMockBuffer(ctrl),
		bufferMgr: bmmock.NewMockBufferMgr(ctrl),
		tx:        txmock.NewMockTransaction(ctrl),
		lock:      txmock.NewMockLock(ctrl),
		time:      tmock.NewMockTime(ctrl),
	}
}
