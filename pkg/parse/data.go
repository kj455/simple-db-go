package parse

import (
	"fmt"
	"strings"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

type Data interface {
	String() string
}

// QueryData holds data for the SQL select statement.
type QueryData struct {
	Fields []string
	Tables []string
	Pred   query.Predicate
}

// NewQueryData creates a new QueryData instance.
func NewQueryData(fields []string, tables []string, pred query.Predicate) *QueryData {
	return &QueryData{
		Fields: fields,
		Tables: tables,
		Pred:   pred,
	}
}

func (q *QueryData) String() string {
	fields := strings.Join(q.Fields, ", ")
	tables := strings.Join(q.Tables, ", ")
	result := fmt.Sprintf("select %s from %s", fields, tables)
	predString := q.Pred.String()
	if predString == "" {
		return result
	}
	return fmt.Sprintf("%s where %s", result, predString)
}

// InsertData is the data for the SQL "insert" statement.
type InsertData struct {
	Table  string
	Fields []string
	Vals   []*constant.Const
}

func NewInsertData(table string, fields []string, vals []*constant.Const) *InsertData {
	return &InsertData{
		Table:  table,
		Fields: fields,
		Vals:   vals,
	}
}

func (i *InsertData) String() string {
	fields := strings.Join(i.Fields, ", ")
	vals := make([]string, len(i.Vals))
	for j, v := range i.Vals {
		vals[j] = v.ToString()
	}
	values := strings.Join(vals, ", ")
	return fmt.Sprintf("insert into %s(%s) values(%s)", i.Table, fields, values)
}

// ModifyData is the data for the SQL "update" statement.
type ModifyData struct {
	Table string
	Field string
	Expr  query.Expression
	Pred  query.Predicate
}

func NewModifyData(table, field string, expr query.Expression, pred query.Predicate) *ModifyData {
	return &ModifyData{
		Table: table,
		Field: field,
		Expr:  expr,
		Pred:  pred,
	}
}

func (m *ModifyData) String() string {
	expr := m.Expr.ToString()
	pred := m.Pred.String()
	str := fmt.Sprintf("update %s set %s = %s", m.Table, m.Field, expr)
	if pred == "" {
		return str
	}
	return fmt.Sprintf("%s where %s", str, pred)
}

// DeleteData is the data for the SQL "delete" statement.
type DeleteData struct {
	Table string
	Pred  query.Predicate
}

func NewDeleteData(table string, pred query.Predicate) *DeleteData {
	return &DeleteData{
		Table: table,
		Pred:  pred,
	}
}

func (d *DeleteData) String() string {
	pred := d.Pred.String()
	return fmt.Sprintf("delete from %s where %s", d.Table, pred)
}

// CreateTableData is the data for the SQL "create table" statement.
type CreateTableData struct {
	Table  string
	Schema record.Schema
}

func NewCreateTableData(table string, sch record.Schema) *CreateTableData {
	return &CreateTableData{
		Table:  table,
		Schema: sch,
	}
}

func (c *CreateTableData) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("create table %s(", c.Table))
	for i, field := range c.Schema.Fields() {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(field)
		typ, _ := c.Schema.Type(field)
		switch typ {
		case record.SCHEMA_TYPE_INTEGER:
			sb.WriteString(" int")
		case record.SCHEMA_TYPE_VARCHAR:
			sb.WriteString(" varchar(")
			length, _ := c.Schema.Length(field)
			sb.WriteString(fmt.Sprintf("%d", length))
			sb.WriteString(")")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

// CreateViewData is the data for the SQL "create view" statement.
type CreateViewData struct {
	ViewName string
	data     *QueryData
}

func NewCreateViewData(viewName string, data *QueryData) *CreateViewData {
	return &CreateViewData{
		ViewName: viewName,
		data:     data,
	}
}

func (c *CreateViewData) ViewDef() string {
	return c.data.String()
}

func (c *CreateViewData) String() string {
	return fmt.Sprintf("create view %s as %s", c.ViewName, c.ViewDef())
}

// CreateIndexData is the parser for the "create index" statement.
type CreateIndexData struct {
	Idx, Table, Field string
}

func NewCreateIndexData(idx, table, field string) *CreateIndexData {
	return &CreateIndexData{
		Idx:   idx,
		Table: table,
		Field: field,
	}
}

func (c *CreateIndexData) String() string {
	return fmt.Sprintf("create index %s on %s(%s)", c.Idx, c.Table, c.Field)
}
