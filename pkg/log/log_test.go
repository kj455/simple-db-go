package log

import (
	"fmt"
	"testing"

	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	"github.com/stretchr/testify/require"
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

func TestLog(t *testing.T) {
	// rootDir := testutil.ProjectRootDir()
	// dir := rootDir + "/.tmp"
	// fm := file.NewFileMgr(dir, 400)
	// lm, err := NewLogMgr(fm, "testlogfile")
	// require.NoError(t, err)

	// printLogRecords(t, lm, "The initial empty log file:") // print an empty log file
	// t.Logf("done")
	// createRecords(t, lm, 1, 35)
	// printLogRecords(t, lm, "The log file now has these records:")
	// createRecords(t, lm, 36, 70)
	// lm.Flush(65)
	// printLogRecords(t, lm, "The log file now has these records:")

	// t.Error()
}

func printLogRecords(t *testing.T, lm LogMgr, msg string) {
	fmt.Println(msg)
	iter, err := lm.Iterator()
	require.NoError(t, err)
	for iter.HasNext() {
		rec, err := iter.Next()
		require.NoError(t, err)
		p := file.NewPageFromBytes(rec)
		s := p.GetString(0)
		npos := file.MaxLength(len(s))
		val := p.GetInt(npos)
		t.Logf("[%s, %d]\n", s, val)
	}
	t.Logf("\n")
}

func createRecords(t *testing.T, lm LogMgr, start int, end int) {
	t.Logf("Creating records: ")
	for i := start; i <= end; i++ {
		rec := createLogRecord(lm, fmt.Sprintf("record%d", i), i+100)
		lsn, err := lm.Append(rec)
		require.NoError(t, err)
		t.Logf("%d ", lsn)
	}
	fmt.Println()
}

func createLogRecord(lm LogMgr, s string, n int) []byte {
	spos := 0
	npos := spos + file.MaxLength(len(s))
	p := file.NewPage(npos + 4) // assuming int is 4 bytes
	p.SetString(spos, s)
	p.SetInt(npos, uint32(n))
	return p.Contents().Bytes()
}
