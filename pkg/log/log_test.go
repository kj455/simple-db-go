package log

import (
	fmock "github.com/kj455/db/pkg/file/mock"
	"go.uber.org/mock/gomock"
)

type mocks struct {
	fileMgr *fmock.MockFileMgr
	page    *fmock.MockPage
	block   *fmock.MockBlockId
}

func newMocks(ctrl *gomock.Controller) *mocks {
	return &mocks{
		fileMgr: fmock.NewMockFileMgr(ctrl),
		page:    fmock.NewMockPage(ctrl),
		block:   fmock.NewMockBlockId(ctrl),
	}
}
