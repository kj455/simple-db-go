package metadata

import (
	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

type TableMgr interface {
	CreateTable(table string, schema record.Schema, tx tx.Transaction) error
	GetLayout(table string, tx tx.Transaction) (record.Layout, error)
	HasTable(table string, tx tx.Transaction) (bool, error)
	TableCatalog() string
	FieldCatalog() string
}

type ViewMgr interface {
	CreateView(name, def string, tx tx.Transaction) error
	GetViewDef(name string, tx tx.Transaction) (string, error)
}

type StatInfo interface {
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(field string) int
}

type StatMgr interface {
	GetStatInfo(table string, layout record.Layout, tx tx.Transaction) (StatInfo, error)
}

type IndexInfo interface {
	IndexName() string
	IdxLayout() record.Layout
	IndexTx() tx.Transaction
	Si() StatInfo
}

type IndexMgr interface {
	CreateIndex(name, table, field string, tx tx.Transaction) error
	GetIndexInfo(table string, tx tx.Transaction) (map[string]IndexInfo, error)
}

type MetadataMgr interface {
	CreateTable(table string, sch record.Schema, tx tx.Transaction) error
	GetLayout(table string, tx tx.Transaction) (record.Layout, error)
	CreateView(name string, def string, tx tx.Transaction) error
	GetViewDef(name string, tx tx.Transaction) (string, error)
	CreateIndex(name string, table string, field string, tx tx.Transaction) error
	GetIndexInfo(table string, tx tx.Transaction) (map[string]IndexInfo, error)
	GetStatInfo(table string, layout record.Layout, tx tx.Transaction) (StatInfo, error)
}
