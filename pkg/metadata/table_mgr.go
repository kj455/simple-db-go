package metadata

import (
	"fmt"
	"sync"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

var (
	errCreateTable = "table-manager: create table: %w"
)

type TableMgrImpl struct {
	tcatLayout, fcatLayout record.Layout
	mu                     sync.Mutex
}

// MaxName is the maximum character length a tablename or fieldname can have.
const MAX_NAME = 16

// NewTableMgr creates a new catalog manager for the database system.
func NewTableMgr(isNew bool, tx tx.Transaction) (TableMgr, error) {
	tm := &TableMgrImpl{}

	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	var err error
	tm.tcatLayout, err = record.NewLayoutFromSchema(tcatSchema)
	if err != nil {
		return nil, fmt.Errorf("table manager: %w", err)
	}

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tm.fcatLayout, err = record.NewLayoutFromSchema(fcatSchema)
	if err != nil {
		return nil, fmt.Errorf("table manager: %w", err)
	}

	if !isNew {
		return tm, nil
	}

	if err := tm.CreateTable("tblcat", tcatSchema, tx); err != nil {
		return nil, err
	}
	if err := tm.CreateTable("fldcat", fcatSchema, tx); err != nil {
		return nil, err
	}
	return tm, nil
}

// CreateTable creates a new table with the given name and schema.
func (tm *TableMgrImpl) CreateTable(tblname string, sch record.Schema, tx tx.Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	layout, err := record.NewLayoutFromSchema(sch)
	if err != nil {
		return fmt.Errorf(errCreateTable, err)
	}

	// insert one record into tblcat
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return fmt.Errorf(errCreateTable, err)
	}
	if err := tcat.Insert(); err != nil {
		return fmt.Errorf(errCreateTable, err)
	}
	if err := tcat.SetString("tblname", tblname); err != nil {
		return fmt.Errorf(errCreateTable, err)
	}
	if err := tcat.SetInt("slotsize", layout.SlotSize()); err != nil {
		return fmt.Errorf(errCreateTable, err)
	}
	tcat.Close()

	// insert a record into fldcat for each field
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return fmt.Errorf(errCreateTable, err)
	}
	for _, fldname := range sch.Fields() {
		schType, err := sch.Type(fldname)
		if err != nil {
			return err
		}
		schLen, err := sch.Length(fldname)
		if err != nil {
			return err
		}

		if err := fcat.Insert(); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
		if err := fcat.SetString("tblname", tblname); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
		if err := fcat.SetString("fldname", fldname); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
		if err := fcat.SetInt("type", int(schType)); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
		if err := fcat.SetInt("length", schLen); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
		if err := fcat.SetInt("offset", layout.Offset(fldname)); err != nil {
			return fmt.Errorf(errCreateTable, err)
		}
	}
	fcat.Close()
	return nil
}

// GetLayout retrieves the layout of the specified table from the catalog.
func (tm *TableMgrImpl) GetLayout(tblname string, tx tx.Transaction) (record.Layout, error) {
	size := -1

	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return nil, fmt.Errorf("table-manager: get layout: %w", err)
	}
	for tcat.Next() {
		name, err := tcat.GetString("tblname")
		if err != nil {
			return nil, fmt.Errorf("table-manager: get layout: %w", err)
		}
		if name != tblname {
			continue
		}
		size, err = tcat.GetInt("slotsize")
		if err != nil {
			return nil, fmt.Errorf("table-manager: get layout: %w", err)
		}
		break
	}
	tcat.Close()

	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return nil, fmt.Errorf("table-manager: get layout: %w", err)
	}
	for fcat.Next() {
		name, err := fcat.GetString("tblname")
		if err != nil {
			return nil, fmt.Errorf("table-manager: get layout: %w", err)
		}
		if name != tblname {
			continue
		}
		fldname, err := fcat.GetString("fldname")
		if err != nil {
			return nil, err
		}
		fldtype, err := fcat.GetInt("type")
		if err != nil {
			return nil, err
		}
		fldlen, err := fcat.GetInt("length")
		if err != nil {
			return nil, err
		}
		offset, err := fcat.GetInt("offset")
		if err != nil {
			return nil, err
		}
		offsets[fldname] = offset
		sch.AddField(fldname, record.SchemaType(fldtype), fldlen)
	}
	fcat.Close()
	return record.NewLayout(sch, offsets, size), nil
}
