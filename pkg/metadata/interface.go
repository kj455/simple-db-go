package metadata

import (
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

type TableMgr interface {
	CreateTable(table string, sch record.Schema, tx tx.Transaction) error
	GetLayout(table string, tx tx.Transaction) (record.Layout, error)
	HasTable(tblname string, tx tx.Transaction) (bool, error)
}

type ViewMgr interface {
	CreateView(vname, vdef string, tx tx.Transaction) error
	GetViewDef(vname string, tx tx.Transaction) (string, error)
}

type StatInfo interface {
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fldname string) int
}

type StatMgr interface {
	GetStatInfo(tblname string, layout record.Layout, tx tx.Transaction) (StatInfo, error)
}

type IndexInfo interface {
	IndexName() string
	IdxLayout() record.Layout
	IndexTx() tx.Transaction
	Si() StatInfo
}

type IndexMgr interface {
	CreateIndex(idxname, tblname, fldname string, tx tx.Transaction) error
	GetIndexInfo(tblname string, tx tx.Transaction) (map[string]IndexInfo, error)
}

type MetadataMgr interface {
	CreateTable(tblname string, sch record.Schema, tx tx.Transaction) error
	GetLayout(tblname string, tx tx.Transaction) (record.Layout, error)
	CreateView(viewname string, viewdef string, tx tx.Transaction) error
	GetViewDef(viewname string, tx tx.Transaction) (string, error)
	CreateIndex(idxname string, tblname string, fldname string, tx tx.Transaction) error
	GetIndexInfo(tblname string, tx tx.Transaction) (map[string]IndexInfo, error)
	GetStatInfo(tblname string, layout record.Layout, tx tx.Transaction) (StatInfo, error)
}
