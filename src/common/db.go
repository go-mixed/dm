package common

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

type Table struct {
	Schema string `db:"TABLE_Schema"`
	Table  string `db:"TABLE_NAME"`

	Collation string `db:"TABLE_COLLATION"`

	Columns Columns
}

type Tables map[string]*Table

func (t Tables) ToList() []*Table {
	var list []*Table
	for _, v := range t {
		list = append(list, v)
	}
	return list
}

func (t Tables) Names() []string {
	var list []string
	for _, v := range t {
		list = append(list, v.Table)
	}
	return list
}

type Column struct {
	Schema string `db:"TABLE_SCHEMA"`
	Table  string `db:"TABLE_NAME"`

	Column      string `db:"COLUMN_NAME"`
	Ordinal     int    `db:"ORDINAL_POSITION"`
	RawNullable string `db:"IS_NULLABLE"`
	Nullable    bool   `db:"-"`
	Type        string `db:"DATA_TYPE"`
	Charset     string `db:"CHARACTER_SET_NAME"`
	Collation   string `db:"COLLATION_NAME"`
}

type Columns []*Column

func (c Columns) ToList() []*Column {
	var list []*Column
	for _, v := range c {
		list = append(list, v)
	}
	return list
}

func (c Columns) Names() []string {
	var list []string
	for _, v := range c {
		list = append(list, v.Column)
	}
	return list
}

type BinLogPosition struct {
	File     string `yaml:"file" validate:"min=8"`
	Position uint32 `yaml:"position" validate:"min=0"`
}

func NewBinLogPositions(pos mysql.Position) BinLogPosition {
	return BinLogPosition{
		File:     pos.Name,
		Position: pos.Pos,
	}
}

func (p BinLogPosition) GreaterThan(p1 BinLogPosition) bool {
	return p.ToMysqlPos().Compare(p1.ToMysqlPos()) > 0
}

func (p BinLogPosition) ToMysqlPos() mysql.Position {
	return mysql.Position{
		Name: p.File,
		Pos:  p.Position,
	}
}

func (p BinLogPosition) IsEmpty() bool {
	return p.File == "" && p.Position == 0
}
