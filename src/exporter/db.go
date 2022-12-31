package exporter

import (
	"github.com/go-mysql-org/go-mysql/schema"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/dm.v1/src/common"
)

func SetGetTableFn(fn func(string) *schema.Table) {
	consumer.GetTableFn = func(s string) *consumer.Table {
		table := fn(s)
		if table != nil {
			return common.ToConsumerTable(table)
		}
		return nil
	}
}
