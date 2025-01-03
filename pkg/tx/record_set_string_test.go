package tx

import (
	"testing"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewSetStringRecord(t *testing.T) {
	t.Parallel()
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
	)
	page := file.NewPageFromBytes([]byte{
		0, 0, 0, byte(OP_SET_STRING),
		0, 0, 0, txNum, // txNum
		0, 0, 0, byte(len(filename)), // filename length
		'f', 'i', 'l', 'e', 'n', 'a', 'm', 'e', // filename
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		0, 0, 0, blockNum, // blockNum
		0, 0, 0, offset, // offset
		0, 0, 0, byte(len(val)), // val length
		'v', 'a', 'l', 'u', 'e', // val
	})

	record := NewSetStringRecord(page)

	assert.Equal(t, OP_SET_STRING, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.Equal(t, filename, record.block.Filename())
	assert.Equal(t, blockNum, record.block.Number())
	assert.Equal(t, offset, record.offset)
	assert.Equal(t, val, record.val)
	assert.Equal(t, "<SET_STRING 1 [file filename, block 2] 3 value>", record.String())
}

func TestSetStringRecordToString(t *testing.T) {
	t.Parallel()
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
	)
	record := SetStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  file.NewBlockId(filename, blockNum),
	}
	assert.Equal(t, "<SET_STRING 1 [file filename, block 2] 3 value>", record.String())
}

func TestWriteSetStringRecordToLog(t *testing.T) {
	t.Parallel()
	const (
		txNum        = 1
		filename     = "filename"
		blockNum     = 2
		offset       = 3
		val          = "value"
		testFileName = "file"
		blockSize    = 400
	)
	dir, cleanup := testutil.SetupDir("test_write_set_string_record_to_log")
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fileMgr, testFileName)
	assert.NoError(t, err)
	block := file.NewBlockId(filename, blockNum)

	lsn, err := WriteSetStringRecordToLog(lm, txNum, block, offset, val)
	assert.NoError(t, err)
	assert.Equal(t, 1, lsn)

	iter, err := lm.Iterator()
	assert.NoError(t, err)
	assert.True(t, iter.HasNext())

	record, err := iter.Next()
	assert.NoError(t, err)

	page := file.NewPageFromBytes(record)
	setStringRecord := NewSetStringRecord(page)

	assert.Equal(t, OP_SET_STRING, setStringRecord.Op())
	assert.Equal(t, txNum, setStringRecord.TxNum())
	assert.Equal(t, filename, setStringRecord.block.Filename())
	assert.Equal(t, blockNum, setStringRecord.block.Number())
	assert.Equal(t, offset, setStringRecord.offset)
	assert.Equal(t, val, setStringRecord.val)
}
