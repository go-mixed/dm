package common

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"go-common/utils/text"
	"golang.org/x/exp/constraints"
	"strings"
)

func BuildTableName(schema, table string, cols []schema.TableColumn) string {
	if cols == nil {
		return schema + "." + table
	}

	sb := strings.Builder{}
	for _, col := range cols {
		sb.WriteString(col.Name)
		sb.WriteString(",")
		sb.WriteString(col.RawType)
		sb.WriteString("|")
	}
	return fmt.Sprintf("%s.%s.%s", schema, table, text_utils.Md5(sb.String()))
}

func Max[T constraints.Integer | constraints.Float](a, b T) T {
	if a >= b {
		return a
	}
	return b
}

func Min[T constraints.Integer | constraints.Float](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func AbsSub[T constraints.Integer | constraints.Float](a, b T) T {
	if a >= b {
		return a - b
	}
	return b - a
}

func DiffCols(cols1 []any, cols2 []any, columns []schema.TableColumn) []int {
	var colIndices []int
	l1 := len(cols1)
	l2 := len(cols2)

	same := true
	for i := 0; i < Min(l1, l2); i++ {
		v1 := cols1[i]
		v2 := cols2[i]

		switch columns[i].Type {
		case schema.TYPE_MEDIUM_INT, schema.TYPE_FLOAT, schema.TYPE_NUMBER, schema.TYPE_DECIMAL:
			same = v1 == v2
		case schema.TYPE_DATETIME, schema.TYPE_DATE, schema.TYPE_TIME, schema.TYPE_TIMESTAMP:
			same = v1 == v2
		case schema.TYPE_STRING, schema.TYPE_ENUM, schema.TYPE_SET, schema.TYPE_BINARY, schema.TYPE_BIT:
			_, ok1 := v1.(string)
			_, ok2 := v2.(string)
			if v1 == nil && v2 == nil {
				same = true
			} else if (v1 == nil && v2 != nil) || (v1 != nil && v2 == nil) {
				same = false
			} else if ok1 || ok2 {
				same = v1 == v2
			} else {
				same = bytes.Compare(v1.([]byte), v2.([]byte)) != 0
			}
		case schema.TYPE_POINT:
			// Todo
			same = true
		}

		if !same {
			colIndices = append(colIndices, i)
		}
	}

	for i := Min(l1, l2); i < Max(l1, l2); i++ {
		colIndices = append(colIndices, i)
	}

	return colIndices
}
