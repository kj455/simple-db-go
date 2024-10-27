package metadata

import (
	"fmt"
	"sync"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

const (
	tableFieldCatalog = "fldcat"
	tableTableCatalog = "tblcat"

	fieldTableName = "tblname"
	fieldSlotSize  = "slotsize"
	fieldFieldName = "fldname"
	fieldType      = "type"
	fieldLength    = "length"
	fieldOffset    = "offset"
)

type TableMgrImpl struct {
	tblCatLayout record.Layout
	fldCatLayout record.Layout
	mu           sync.Mutex
}

// MaxName is the maximum character length a tablename or fieldname can have.
const MAX_NAME_LENGTH = 16

// NewTableMgr creates a new catalog manager for the database system.
func NewTableMgr(tx tx.Transaction) (*TableMgrImpl, error) {
	tm := &TableMgrImpl{}

	var err error

	tblCatSchema := tm.newTableCatalogSchema()
	tm.tblCatLayout, err = record.NewLayoutFromSchema(tblCatSchema)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to create table catalog layout from schema: %w", err)
	}

	fldCatSchema := tm.newFieldCatalogSchema()
	tm.fldCatLayout, err = record.NewLayoutFromSchema(fldCatSchema)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to create field catalog layout from schema: %w", err)
	}

	if err := tm.CreateTable(tableTableCatalog, tblCatSchema, tx); err != nil {
		return nil, err
	}
	if err := tm.CreateTable(tableFieldCatalog, fldCatSchema, tx); err != nil {
		return nil, err
	}
	return tm, nil
}

func (tm *TableMgrImpl) newTableCatalogSchema() record.Schema {
	sch := record.NewSchema()
	sch.AddStringField(fieldTableName, MAX_NAME_LENGTH)
	sch.AddIntField(fieldSlotSize)
	return sch
}

func (tm *TableMgrImpl) newFieldCatalogSchema() record.Schema {
	sch := record.NewSchema()
	sch.AddStringField(fieldTableName, MAX_NAME_LENGTH)
	sch.AddStringField(fieldFieldName, MAX_NAME_LENGTH)
	sch.AddIntField(fieldType)
	sch.AddIntField(fieldLength)
	sch.AddIntField(fieldOffset)
	return sch
}

// CreateTable creates a new table with the given name and schema.
func (tm *TableMgrImpl) CreateTable(tblname string, sch record.Schema, tx tx.Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// check if the table already exists
	hasTable, err := tm.HasTable(tblname, tx)
	if err != nil {
		return fmt.Errorf("metadata: failed to check if table exists: %w", err)
	}
	if hasTable {
		return nil
	}

	layout, err := record.NewLayoutFromSchema(sch)
	if err != nil {
		return fmt.Errorf("metadata: failed to create layout from schema: %w", err)
	}

	// add the table to the table/field catalogs
	if err := tm.addToTableCatalog(tblname, layout.SlotSize(), tx); err != nil {
		return err
	}
	return tm.addToFieldCatalog(tblname, sch, tx)
}

func (tm *TableMgrImpl) HasTable(tblname string, tx tx.Transaction) (bool, error) {
	tcat, err := record.NewTableScan(tx, tableTableCatalog, tm.tblCatLayout)
	if err != nil {
		return false, fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer tcat.Close()
	for tcat.Next() {
		name, err := tcat.GetString(fieldTableName)
		if err != nil {
			return false, fmt.Errorf("metadata: failed to get tableName: %w", err)
		}
		if name == tblname {
			return true, nil
		}
	}
	return false, nil
}

func (tm *TableMgrImpl) addToTableCatalog(tblname string, slotSize int, tx tx.Transaction) error {
	tcat, err := record.NewTableScan(tx, tableTableCatalog, tm.tblCatLayout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	if err := tcat.Insert(); err != nil {
		return fmt.Errorf("metadata: failed to insert into table scan: %w", err)
	}
	if err := tcat.SetString(fieldTableName, tblname); err != nil {
		return fmt.Errorf("metadata: failed to set tableName: %w", err)
	}
	if err := tcat.SetInt(fieldSlotSize, slotSize); err != nil {
		return fmt.Errorf("metadata: failed to set slotSize: %w", err)
	}
	tcat.Close()
	return nil
}

func (tm *TableMgrImpl) addToFieldCatalog(tblname string, schema record.Schema, tx tx.Transaction) error {
	layout, err := record.NewLayoutFromSchema(schema)
	if err != nil {
		return fmt.Errorf("metadata: failed to create layout from schema: %w", err)
	}
	fcat, err := record.NewTableScan(tx, tableFieldCatalog, tm.fldCatLayout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer fcat.Close()
	for _, fldname := range schema.Fields() {
		schType, err := schema.Type(fldname)
		if err != nil {
			return err
		}
		schLen, err := schema.Length(fldname)
		if err != nil {
			return err
		}
		if err := fcat.Insert(); err != nil {
			return fmt.Errorf("metadata: failed to insert into table scan: %w", err)
		}
		if err := fcat.SetString(fieldTableName, tblname); err != nil {
			return fmt.Errorf("metadata: failed to set tableName: %w", err)
		}
		if err := fcat.SetString(fieldFieldName, fldname); err != nil {
			return fmt.Errorf("metadata: failed to set fldname: %w", err)
		}
		if err := fcat.SetInt(fieldType, int(schType)); err != nil {
			return fmt.Errorf("metadata: failed to set type: %w", err)
		}
		if err := fcat.SetInt(fieldLength, schLen); err != nil {
			return fmt.Errorf("metadata: failed to set length: %w", err)
		}
		if err := fcat.SetInt(fieldOffset, layout.Offset(fldname)); err != nil {
			return fmt.Errorf("metadata: failed to set offset: %w", err)
		}
	}
	return nil
}

func (tm *TableMgrImpl) DropTable(tblname string, tx tx.Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tcat, err := record.NewTableScan(tx, tableTableCatalog, tm.tblCatLayout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	for tcat.Next() {
		name, err := tcat.GetString(fieldTableName)
		if err != nil {
			return fmt.Errorf("metadata: failed to get tableName: %w", err)
		}
		if name != tblname {
			continue
		}
		tcat.Delete()
		break
	}
	tcat.Close()

	fcat, err := record.NewTableScan(tx, tableFieldCatalog, tm.fldCatLayout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer fcat.Close()
	for fcat.Next() {
		name, err := fcat.GetString(fieldTableName)
		if err != nil {
			return fmt.Errorf("metadata: failed to get tableName: %w", err)
		}
		if name != tblname {
			continue
		}
		fcat.Delete()
	}
	return nil
}

// GetLayout retrieves the layout of the specified table from the catalog.
func (tm *TableMgrImpl) GetLayout(tblname string, tx tx.Transaction) (record.Layout, error) {
	slotSize, err := tm.getTableSlotSize(tblname, tx)
	if err != nil {
		return nil, err
	}
	sch, offsets, err := tm.getTableSchemaOffset(tblname, tx)
	if err != nil {
		return nil, err
	}
	layout := record.NewLayout(sch, offsets, slotSize)
	return layout, nil
}

func (tm *TableMgrImpl) getTableSlotSize(tblname string, tx tx.Transaction) (int, error) {
	tcat, err := record.NewTableScan(tx, tableTableCatalog, tm.tblCatLayout)
	if err != nil {
		return 0, fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer tcat.Close()
	for tcat.Next() {
		name, err := tcat.GetString(fieldTableName)
		if err != nil {
			return 0, fmt.Errorf("metadata: failed to get tableName: %w", err)
		}
		if name != tblname {
			continue
		}
		return tcat.GetInt(fieldSlotSize)
	}
	return 0, fmt.Errorf("metadata: table %s not found", tblname)
}

func (tm *TableMgrImpl) getTableSchemaOffset(tblName string, tx tx.Transaction) (record.Schema, map[string]int, error) {
	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat, err := record.NewTableScan(tx, tableFieldCatalog, tm.fldCatLayout)
	if err != nil {
		return nil, nil, fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer fcat.Close()
	for fcat.Next() {
		name, err := fcat.GetString(fieldTableName)
		if err != nil {
			return nil, nil, fmt.Errorf("metadata: failed to get tableName: %w", err)
		}
		if name != tblName {
			continue
		}
		fldname, err := fcat.GetString(fieldFieldName)
		if err != nil {
			return nil, nil, fmt.Errorf("metadata: failed to get fldname: %w", err)
		}
		fldtype, err := fcat.GetInt(fieldType)
		if err != nil {
			return nil, nil, fmt.Errorf("metadata: failed to get type: %w", err)
		}
		fldlen, err := fcat.GetInt(fieldLength)
		if err != nil {
			return nil, nil, fmt.Errorf("metadata: failed to get length: %w", err)
		}
		offset, err := fcat.GetInt(fieldOffset)
		if err != nil {
			return nil, nil, fmt.Errorf("metadata: failed to get offset: %w", err)
		}
		offsets[fldname] = offset
		sch.AddField(fldname, record.SchemaType(fldtype), fldlen)
	}
	return sch, offsets, nil
}
