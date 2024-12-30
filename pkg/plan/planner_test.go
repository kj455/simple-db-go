package plan

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	buffermgr "github.com/kj455/simple-db/pkg/buffer_mgr"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/metadata"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/require"
)

func TestPlanner(t *testing.T) {
	const (
		dirname     = "studentdb"
		logFileName = "logfile"
		blockSize   = 400
	)
	dir, cleanup := testutil.SetupDir(dirname)
	t.Cleanup(cleanup)
	fm := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fm, logFileName)
	require.NoError(t, err)
	const buffNum = 8
	buffs := make([]buffer.Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fm, lm, bm, txNumGen)
	require.NoError(t, err)
	mdm, err := metadata.NewMetadataMgr(tx)
	require.NoError(t, err)
	qp := NewBasicQueryPlanner(mdm)
	up := NewBasicUpdatePlanner(mdm)
	planner := NewPlanner(qp, up)

	cmd := "create table student(sname varchar(10), gradyear int, majorid int, studentid int)"
	_, err = planner.ExecuteUpdate(cmd, tx)
	require.NoError(t, err)
	tx.Commit()

	const recordNum = 100
	for i := 0; i < recordNum; i++ {
		name := fmt.Sprintf("student%d", i)
		gradYear := int(math.Round(rand.Float64() * 50))
		cmd = fmt.Sprintf("insert into student(sname, gradyear, majorid, studentid) values('%s', %d, %d, %d)", name, gradYear, i, i)
		_, err = planner.ExecuteUpdate(cmd, tx)
		require.NoError(t, err)
	}

	qry := "select sname, gradyear from student"
	p, err := planner.CreateQueryPlan(qry, tx)
	require.NoError(t, err)
	s, err := p.Open()
	require.NoError(t, err)

	for s.Next() {
		sname, err := s.GetString("sname")
		require.NoError(t, err)
		gradyear, err := s.GetInt("gradyear")
		require.NoError(t, err)
		t.Logf("sname=%s, gradyear=%d\n", sname, gradyear)
	}
	s.Close()

	cmd = "delete from STUDENT where majorid = 30"
	num, err := planner.ExecuteUpdate(cmd, tx)
	require.NoError(t, err)
	require.Equal(t, 1, num)

	cmd = "select sname from student where majorid = 30"
	p, err = planner.CreateQueryPlan(cmd, tx)
	require.NoError(t, err)
	s, err = p.Open()
	require.NoError(t, err)
	require.False(t, s.Next())

	tx.Commit()
}
