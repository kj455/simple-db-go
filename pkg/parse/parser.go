package parse

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

// Parser is the SimpleDB parser.
type Parser struct {
	lex *Lexer
}

// NewParser creates a new parser for SQL statement s.
func NewParser(s string) *Parser {
	return &Parser{lex: NewLexer(s)}
}

// Field parses and returns a field.
func (p *Parser) Field() (string, error) {
	return p.lex.EatId()
}

// Constant parses and returns a constant.
func (p *Parser) Constant() (*constant.Const, error) {
	if p.lex.MatchStringConstant() {
		str, err := p.lex.EatStringConstant()
		if err != nil {
			return nil, err
		}
		cons, err := constant.NewConstant(constant.KIND_STR, str)
		if err != nil {
			return nil, err
		}
		return cons, nil
	}
	if p.lex.matchIntConstant() {
		in, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}
		cons, err := constant.NewConstant(constant.KIND_INT, in)
		if err != nil {
			return nil, err
		}
		return cons, nil
	}
	return nil, fmt.Errorf("parse: invalid constant")
}

// Expression parses and returns an expression.
func (p *Parser) Expression() (*query.ExpressionImpl, error) {
	if p.lex.MatchId() {
		f, err := p.Field()
		if err != nil {
			return nil, err
		}
		return query.NewFieldExpression(f), nil
	}
	con, err := p.Constant()
	if err != nil {
		return nil, err
	}
	return query.NewConstantExpression(con), nil
}

// Term parses and returns a term.
func (p *Parser) Term() (*query.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(lhs, rhs), nil
}

// Predicate parses and returns a predicate.
func (p *Parser) Predicate() (*query.PredicateImpl, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(term)
	if p.lex.MatchKeyword("and") {
		if err := p.lex.EatKeyword("and"); err != nil {
			return nil, err
		}
		p, err := p.Predicate()
		if err != nil {
			return nil, err
		}
		pred.ConjoinWith(p)
	}
	return pred, nil
}

// Query parses and returns a query data.
func (p *Parser) Query() (*QueryData, error) {
	if err := p.lex.EatKeyword("select"); err != nil {
		return nil, err
	}
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewQueryData(fields, tables, pred), nil
}

func (p *Parser) selectList() ([]string, error) {
	f, err := p.Field()
	if err != nil {
		return nil, err
	}
	L := []string{f}
	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.selectList()
		if err != nil {
			return nil, err
		}
		L = append(L, list...)
	}
	return L, nil
}

func (p *Parser) tableList() ([]string, error) {
	id, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	L := []string{id}
	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.tableList()
		if err != nil {
			return nil, err
		}
		L = append(L, list...)
	}
	return L, nil
}

// UpdateCmd parses and returns an update command.
func (p *Parser) UpdateCmd() (any, error) {
	if p.lex.MatchKeyword("insert") {
		return p.Insert()
	}
	if p.lex.MatchKeyword("delete") {
		return p.Delete()
	}
	if p.lex.MatchKeyword("update") {
		return p.Modify()
	}
	if p.lex.MatchKeyword("create") {
		return p.create()
	}
	return nil, fmt.Errorf("parse: invalid command")
}

func (p *Parser) create() (any, error) {
	if err := p.lex.EatKeyword("create"); err != nil {
		return nil, err
	}
	if p.lex.MatchKeyword("table") {
		return p.CreateTable()
	}
	if p.lex.MatchKeyword("view") {
		return p.CreateView()
	}
	if p.lex.MatchKeyword("index") {
		return p.CreateIndex()
	}
	return nil, fmt.Errorf("parse: invalid command")
}

// Delete parses and returns a delete data.
func (p *Parser) Delete() (*DeleteData, error) {
	if err := p.lex.EatKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(table, pred), nil
}

// Insert parses and returns an insert data.
func (p *Parser) Insert() (*InsertData, error) {
	if err := p.lex.EatKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("into"); err != nil {
		return nil, err
	}
	table, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("values"); err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	vals, err := p.constList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewInsertData(table, fields, vals), nil
}

func (p *Parser) fieldList() ([]string, error) {
	f, err := p.Field()
	if err != nil {
		return nil, err
	}
	L := []string{f}
	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.fieldList()
		if err != nil {
			return nil, err
		}
		L = append(L, list...)
	}
	return L, nil
}

func (p *Parser) constList() ([]*constant.Const, error) {
	cons, err := p.Constant()
	if err != nil {
		return nil, err
	}
	L := []*constant.Const{cons}
	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.constList()
		if err != nil {
			return nil, err
		}
		L = append(L, list...)
	}
	return L, nil
}

// Modify parses and returns a modify data.
func (p *Parser) Modify() (*ModifyData, error) {
	if err := p.lex.EatKeyword("update"); err != nil {
		return nil, err
	}
	table, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("set"); err != nil {
		return nil, err
	}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}
	expr, err := p.Expression()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewModifyData(table, field, expr, pred), nil
}

// CreateTable parses and returns a create table data.
func (p *Parser) CreateTable() (*CreateTableData, error) {
	if err := p.lex.EatKeyword("table"); err != nil {
		return nil, err
	}
	table, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	sch, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateTableData(table, sch), nil
}

func (p *Parser) fieldDefs() (record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}
		schema2, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}
		if err := schema.AddAll(schema2); err != nil {
			return nil, err
		}
	}
	return schema, nil
}

func (p *Parser) fieldDef() (record.Schema, error) {
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(field)
}

func (p *Parser) fieldType(field string) (record.Schema, error) {
	schema := record.NewSchema()
	if p.lex.MatchKeyword("int") {
		if err := p.lex.EatKeyword("int"); err != nil {
			return nil, err
		}
		schema.AddIntField(field)
		return schema, nil
	}
	if p.lex.MatchKeyword("varchar") {
		if err := p.lex.EatKeyword("varchar"); err != nil {
			return nil, err
		}
		if err := p.lex.EatDelim('('); err != nil {
			return nil, err
		}
		strLen, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}
		if err := p.lex.EatDelim(')'); err != nil {
			return nil, err
		}
		schema.AddStringField(field, strLen)
	}
	return schema, nil
}

// CreateView parses and returns a create view data.
func (p *Parser) CreateView() (*CreateViewData, error) {
	if err := p.lex.EatKeyword("view"); err != nil {
		return nil, err
	}
	viewname, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("as"); err != nil {
		return nil, err
	}
	qd, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewname, qd), nil
}

// CreateIndex parses and returns a create index data.
func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	if err := p.lex.EatKeyword("index"); err != nil {
		return nil, err
	}
	idx, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("on"); err != nil {
		return nil, err
	}
	table, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateIndexData(idx, table, field), nil
}
