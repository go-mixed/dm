package common

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
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

func ToRowMap(cols []any, columns []schema.TableColumn) map[string]any {
	_cols := map[string]any{}
	for i, col := range columns {
		_cols[col.Name] = cols[i]
	}
	return _cols
}

func ToConsumerTable(table *schema.Table) *consumer.Table {
	if table == nil {
		return nil
	}
	return &consumer.Table{
		Schema:          table.Schema,
		Name:            table.Name,
		Columns:         ToConsumerColumns(table.Columns),
		Indices:         ToConsumerTableIndex(table.Indexes),
		PKColumns:       table.PKColumns,
		UnsignedColumns: table.UnsignedColumns,
	}
}

func ToConsumerTableIndex(indices []*schema.Index) []*consumer.TableIndex {
	var _indices []*consumer.TableIndex
	for _, index := range indices {
		_indices = append(_indices, &consumer.TableIndex{
			Name:        index.Name,
			Columns:     index.Columns,
			Cardinality: index.Cardinality,
			NoneUnique:  index.NoneUnique,
		})
	}
	return _indices
}

func ToConsumerColumns(columns []schema.TableColumn) []consumer.TableColumn {
	var _columns []consumer.TableColumn
	for _, col := range columns {
		_columns = append(_columns, consumer.TableColumn{
			Name:       col.Name,
			Type:       col.Type,
			Collation:  col.Collation,
			RawType:    col.RawType,
			IsAuto:     col.IsAuto,
			IsUnsigned: col.IsUnsigned,
			IsVirtual:  col.IsVirtual,
			IsStored:   col.IsStored,
			EnumValues: col.EnumValues,
			SetValues:  col.SetValues,
			FixedSize:  col.FixedSize,
			MaxSize:    col.MaxSize,
		})
	}
	return _columns
}
