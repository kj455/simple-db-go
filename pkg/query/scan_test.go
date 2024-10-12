package query

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/metadata"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestScan1(t *testing.T) {
	const blockSize = 400
	rootDir := testutil.RootDir()
	dir := rootDir + "/.tmp/scan1"
	defer testutil.CleanupDir(dir)
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, "testlogfile")
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, _ := transaction.NewTransaction(fm, lm, bm, txNumGen)
	_, _ = metadata.NewMetadataMgr(true, tx)
	tx.Commit()
	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout, _ := record.NewLayoutFromSchema(sch1)
	tableSc1, _ := record.NewTableScan(tx, "T", layout)
	tableSc1.BeforeFirst()
	const (
		length    = 100
		targetIdx = 50
	)
	ints := newShuffledInts(length)
	for i := 0; i < length; i++ {
		err := tableSc1.Insert()
		assert.NoError(t, err)
		tableSc1.SetInt("A", ints[i])
		tableSc1.SetString("B", "rec"+fmt.Sprint(ints[i]))
	}
	tableSc1.Close()

	tableSc2, _ := record.NewTableScan(tx, "T", layout)
	c, _ := constant.NewConstant(constant.KIND_INT, targetIdx)
	term := NewTerm(NewFieldExpression("A"), NewConstantExpression(c))
	pred := NewPredicate(term)
	t.Logf("The predicate is %v", pred)
	selectScan := NewSelectScan(tableSc2, pred)
	s4 := NewProjectScan(selectScan, []string{"A", "B"})
	for s4.Next() {
		iVal, err := s4.GetInt("A")
		assert.NoError(t, err)
		assert.Equal(t, targetIdx, iVal)
		t.Logf("A: %d", iVal)
		sVal, err := s4.GetString("B")
		assert.NoError(t, err)
		t.Logf("B: %s", sVal)
	}
	s4.Close()
	tx.Commit()
}

func TestScan2(t *testing.T) {
	const blockSize = 400
	rootDir := testutil.RootDir()
	dir := rootDir + "/.tmp/scan2"
	defer testutil.CleanupDir(dir)
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, "testlogfile")
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, _ := transaction.NewTransaction(fm, lm, bm, txNumGen)
	_, _ = metadata.NewMetadataMgr(true, tx)

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1, _ := record.NewLayoutFromSchema(sch1)
	us1, _ := record.NewTableScan(tx, "T1", layout1)
	us1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d records into T1.\n", n)
	for i := 0; i < n; i++ {
		us1.Insert()
		us1.SetInt("A", i)
		us1.SetString("B", "bbb"+fmt.Sprint(i))
	}
	us1.Close()

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2, _ := record.NewLayoutFromSchema(sch2)
	us2, _ := record.NewTableScan(tx, "T2", layout2)
	us2.BeforeFirst()
	t.Logf("Inserting %d records into T2.\n", n)
	for i := 0; i < n; i++ {
		us2.Insert()
		us2.SetInt("C", n-i-1)
		us2.SetString("D", "ddd"+fmt.Sprint(n-i-1))
	}
	us2.Close()

	s1, _ := record.NewTableScan(tx, "T1", layout1)
	s2, _ := record.NewTableScan(tx, "T2", layout2)
	s3, _ := NewProductScan(s1, s2)
	term := NewTerm(NewFieldExpression("A"), NewFieldExpression("C"))
	pred := NewPredicate(term)
	t.Log("The predicate is", pred)
	s4 := NewSelectScan(s3, pred)

	fields := []string{"B", "D"}
	s5 := NewProjectScan(s4, fields)
	for s5.Next() {
		bVal, _ := s5.GetString("B")
		dVal, _ := s5.GetString("D")

		t.Logf("%s %s\n", bVal, dVal)
		assert.Equal(t, bVal[3:], dVal[3:])
	}
	s5.Close()
	tx.Commit()
}

func newShuffledInts(length int) []int {
	ints := make([]int, length)
	for i := 0; i < length; i++ {
		ints[i] = i
	}
	rand.Shuffle(length, func(i, j int) {
		ints[i], ints[j] = ints[j], ints[i]
	})
	return ints
}
