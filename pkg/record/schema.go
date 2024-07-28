package record

import "errors"

var (
	errFieldNotFound = errors.New("record: schema: field not found")
)

type FieldInfo struct {
	Type   SchemaType
	Length int
}

type SchemaImpl struct {
	fields []string
	info   map[string]FieldInfo
}

func NewSchema() Schema {
	return &SchemaImpl{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

func (s *SchemaImpl) AddField(field string, typ SchemaType, length int) {
	s.fields = append(s.fields, field)
	s.info[field] = FieldInfo{
		Type:   typ,
		Length: length,
	}
}

func (s *SchemaImpl) AddIntField(field string) {
	s.AddField(field, SCHEMA_TYPE_INTEGER, 0)
}

func (s *SchemaImpl) AddStringField(field string, length int) {
	s.AddField(field, SCHEMA_TYPE_VARCHAR, length)
}

func (s *SchemaImpl) Add(field string, sch Schema) error {
	fieldType, err := sch.Type(field)
	if err != nil {
		return err
	}
	length, err := sch.Length(field)
	if err != nil {
		return err
	}
	s.AddField(field, fieldType, length)
	return nil
}

func (s *SchemaImpl) AddAll(sch Schema) error {
	for _, field := range sch.Fields() {
		if err := s.Add(field, sch); err != nil {
			return err
		}
	}
	return nil
}

func (s *SchemaImpl) Fields() []string {
	return s.fields
}

func (s *SchemaImpl) HasField(field string) bool {
	for _, fld := range s.fields {
		if fld == field {
			return true
		}
	}
	return false
}

func (s *SchemaImpl) Type(field string) (SchemaType, error) {
	info, ok := s.info[field]
	if !ok {
		return 0, errFieldNotFound
	}
	return info.Type, nil
}

func (s *SchemaImpl) Length(field string) (int, error) {
	info, ok := s.info[field]
	if !ok {
		return 0, errFieldNotFound
	}
	return info.Length, nil
}
