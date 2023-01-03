package exporter

import (
	"github.com/go-mysql-org/go-mysql/schema"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
)

// 缓存对应关系
var consumerTables map[*schema.Table]*consumer.Table = map[*schema.Table]*consumer.Table{}

func SetGetTableFn(fn func(string) *schema.Table) {
	consumer.GetTableFn = func(s string) *consumer.Table {
		table := fn(s)
		if table != nil {
			var res *consumer.Table
			var ok bool
			if res, ok = consumerTables[table]; ok {
				return res
			}
			res = ToConsumerTable(table)
			consumerTables[table] = res
			return res
		}

		return nil
	}
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
