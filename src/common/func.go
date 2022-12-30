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

func SplitTableName(tableName string) (string, string) {
	segments := strings.SplitN(tableName, ".", 2)
	if len(segments) == 2 {
		return segments[0], segments[1]
	}
	return "", tableName
}

func CleanTableName(tableName string) string {
	switch strings.Count(tableName, ".") {
	case 2:
		return tableName[:strings.LastIndex(tableName, ".")]
	default:
		return tableName
	}
}

func BuildEventKey(id uint64, schema, table string, action string) string {
	if schema == "" && table == "" {
		return fmt.Sprintf("%020d/", id)
	}
	return fmt.Sprintf("%020d/%s/%s", id, BuildTableName(schema, table, nil), action)
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

func ToRowMap(cols []any, columns []schema.TableColumn) map[string]any {
	_cols := map[string]any{}
	for i, col := range columns {
		_cols[col.Name] = cols[i]
	}
	return _cols
}

func IsColEmpty(colType int, val any) bool {
	if val == nil {
		return true
	}
	switch colType {
	case schema.TYPE_MEDIUM_INT, schema.TYPE_FLOAT, schema.TYPE_NUMBER, schema.TYPE_DECIMAL:
		return val == 0
	case schema.TYPE_DATETIME, schema.TYPE_DATE, schema.TYPE_TIME, schema.TYPE_TIMESTAMP:
		return val == ""
	case schema.TYPE_STRING, schema.TYPE_ENUM, schema.TYPE_SET, schema.TYPE_BINARY, schema.TYPE_BIT:
		_v, ok := val.(string)
		if ok {
			return _v == ""
		}
		_b, ok := val.([]byte)
		if ok {
			return len(_b) <= 0
		}
	}

	return false
}

func IsColValueEqual(colType int, v1, v2 any) bool {
	same := true
	switch colType {
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

	return same
}

func DiffCols(cols1 []any, cols2 []any, columns []schema.TableColumn) []string {
	var colNames []string
	l1 := len(cols1)
	l2 := len(cols2)

	for i := 0; i < Min(l1, l2); i++ {
		v1 := cols1[i]
		v2 := cols2[i]

		if !IsColValueEqual(columns[i].Type, v1, v2) {
			colNames = append(colNames, columns[i].Name)
		}
	}

	for i := Min(l1, l2); i < Max(l1, l2); i++ {
		colNames = append(colNames, columns[i].Name)
	}

	return colNames
}
