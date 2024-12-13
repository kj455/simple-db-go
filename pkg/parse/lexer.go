package parse

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

type TokenType int

const (
	TokenUnknown TokenType = iota
	TokenEOF
	TokenWord
	TokenNumber
	TokenString
	TokenOther
)

const (
	DelimiterEOF    = -1
	DelimiterSpace  = ' '
	DelimiterSingle = '\''
)

var (
	errBadSyntax = errors.New("parse: bad syntax")
)

var keywords = []string{
	"select",
	"from",
	"where",
	"and",
	"insert",
	"into",
	"values",
	"delete",
	"update",
	"set",
	"create",
	"table",
	"int",
	"varchar",
	"view",
	"as",
	"index",
	"on",
}

// Lexer is the lexical analyzer.
type Lexer struct {
	keywords map[string]bool
	tok      *bufio.Scanner
	typ      TokenType
	strVal   string
	numVal   int
}

func scanSQLChars(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0

	// Skip leading spaces
	for start < len(data) && data[start] == DelimiterSpace {
		start++
	}

	if start >= len(data) {
		return
	}

	// Single character delimiters
	if strings.ContainsRune("(),=", rune(data[start])) {
		return start + 1, data[start : start+1], nil
	}

	// Collect token until delimiter or space
	for i := start; i < len(data); i++ {
		if data[i] == DelimiterSpace || strings.ContainsRune("(),=", rune(data[i])) {
			return i, data[start:i], nil
		}
	}

	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	return
}

// NewLexer creates a new lexical analyzer for SQL statement s.
func NewLexer(s string) *Lexer {
	l := &Lexer{
		keywords: initKeywords(),
		tok:      bufio.NewScanner(strings.NewReader(s)),
	}
	l.tok.Split(scanSQLChars)
	l.nextToken()
	return l
}

func initKeywords() map[string]bool {
	m := make(map[string]bool)
	for _, k := range keywords {
		m[k] = true
	}
	return m
}

// matchDelim returns true if the current token is the specified delimiter character.
func (l *Lexer) MatchDelim(d rune) bool {
	// ttype == 'W and sval == d
	// '=' のケースに対応
	// if l.MatchKeyword(string(d)) && len(l.sval) == 1 {
	// 	return rune(l.sval[0]) == d
	// }
	return d == rune(l.strVal[0])
}

// matchIntConstant returns true if the current token is an integer.
func (l *Lexer) matchIntConstant() bool {
	return l.typ == TokenNumber
}

// matchStringConstant returns true if the current token is a string.
func (l *Lexer) MatchStringConstant() bool {
	// return l.ttype == 'S' // Assuming 'S' represents a string
	return rune(l.strVal[0]) == '\''
}

// matchKeyword returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return l.typ == TokenWord && l.strVal == w
}

// matchId returns true if the current token is a legal identifier.
func (l *Lexer) MatchId() bool {
	return l.typ == TokenWord && !l.keywords[l.strVal]
}

// eatDelim throws an exception if the current token is not the specified delimiter. Otherwise, moves to the next token.
func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return errBadSyntax
	}
	l.nextToken()
	return nil
}

// eatIntConstant throws an exception if the current token is not an integer. Otherwise, returns that integer and moves to the next token.
func (l *Lexer) EatIntConstant() (int, error) {
	if !l.matchIntConstant() {
		return 0, errBadSyntax
	}
	i := l.numVal
	l.nextToken()
	return i, nil
}

// eatStringConstant throws an exception if the current token is not a string. Otherwise, returns that string and moves to the next token.
func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", errBadSyntax
	}
	s := l.strVal
	l.nextToken()
	return s, nil
}

// eatKeyword throws an exception if the current token is not the specified keyword. Otherwise, moves to the next token.
func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return errBadSyntax
	}
	l.nextToken()
	return nil
}

// eatId throws an exception if the current token is not an identifier. Otherwise, returns the identifier string and moves to the next token.
func (l *Lexer) EatId() (string, error) {
	if !l.MatchId() {
		return "", errBadSyntax
	}
	s := l.strVal
	l.nextToken()
	return s, nil
}

func (l *Lexer) nextToken() {
	if !l.tok.Scan() {
		l.typ = TokenEOF
		return
	}
	token := l.tok.Text()
	if numVal, err := strconv.Atoi(token); err == nil {
		l.typ = TokenNumber
		l.numVal = numVal
		return
	}
	if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
		l.typ = TokenString
		l.strVal = token[1 : len(token)-1]
		return
	}
	l.typ = TokenWord
	l.strVal = strings.ToLower(token)
}
