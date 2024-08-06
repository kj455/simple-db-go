package constant

import "fmt"

type Kind string

const (
	KIND_INT Kind = "int"
	KIND_STR Kind = "string"
)

// Const denotes values stored in the database.
type Const struct {
	val  any
	kind Kind
}

func NewConstant(kind Kind, val any) (*Const, error) {
	switch kind {
	case KIND_INT:
		if _, ok := val.(int); !ok {
			return nil, fmt.Errorf("constant: value is not an integer")
		}
	case KIND_STR:
		if _, ok := val.(string); !ok {
			return nil, fmt.Errorf("constant: value is not a string")
		}
	default:
	}
	return &Const{
		val:  val,
		kind: kind,
	}, nil
}

// AsInt returns the integer value of the constant.
func (c *Const) AsInt() int {
	if c.kind == KIND_INT {
		return c.val.(int)
	}
	return 0 // or panic/error if you want to handle it strictly
}

// AsString returns the string value of the constant.
func (c *Const) AsString() string {
	if c.kind == KIND_STR {
		return c.val.(string)
	}
	return "" // or panic/error if you want to handle it strictly
}

// Equals checks if two constants are equal.
func (c *Const) Equals(other *Const) bool {
	if c.kind != other.kind {
		return false
	}
	return c.val == other.val
}

// CompareTo returns 0 if two constants are equal, -1 if the receiver is less than the other, and 1 if the receiver is greater than the other.
func (c *Const) CompareTo(other *Const) int {
	if c.kind != other.kind {
		return 0 // or panic/error if you want to handle it strictly
	}
	if c.val == other.val {
		return 0
	}
	// TODO: Implement comparison for other types
	if c.val.(int) < other.val.(int) {
		return -1
	}
	return 1
}

// HashCode returns the hash code of the constant.
func (c *Const) HashCode() int {
	// TODO: Implement more valid hash code
	switch c.kind {
	case KIND_INT:
		return c.AsInt()
	case KIND_STR:
		return len(c.AsString())
	}
	return 0
}

// ToString returns the string representation of the constant.
func (c *Const) ToString() string {
	switch c.kind {
	case KIND_INT:
		return fmt.Sprint(c.AsInt())
	case KIND_STR:
		return c.AsString()
	}
	return ""
}

func (c *Const) Kind() Kind {
	return c.kind
}

func (c *Const) AnyValue() any {
	return c.val
}
