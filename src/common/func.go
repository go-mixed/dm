package common

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"golang.org/x/exp/constraints"
	"gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/conv"
	"gopkg.in/go-mixed/go-common.v1/utils/text"
	"regexp"
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

func BuildKeyPrefix(id int64) string {
	return fmt.Sprintf("%020d/", id)
}
func BuildEventKey(id int64, schema, table string, action string) string {
	return fmt.Sprintf("%020d/event/%s/%s", id, BuildTableName(schema, table, nil), action)
}
func BuildBinLogKey(id int64, pos BinLogPosition) string {
	return fmt.Sprintf("%020d/binlog/%s/%d", id, pos.File, pos.Position)
}

var keyPrefixRegexp = regexp.MustCompile(`^(\d{20})/`)

func GetIDFromKey(key string) int64 {
	if key == "" {
		return 0
	}
	if !keyPrefixRegexp.MatchString(key) {
		return 0
	}
	segments := strings.SplitN(key, "/", 2)
	return conv.Atoi64(segments[0], 0)
}

var binLogKeyRegexp = regexp.MustCompile(`^\d{20}/binlog/`)

func IsBinLogKey(key string) bool {
	return binLogKeyRegexp.MatchString(key)
}

var eventKeyRegexp = regexp.MustCompile(`^\d{20}/event/`)

func IsEventKey(key string) bool {
	return eventKeyRegexp.MatchString(key)
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

func DiffCols(cols1 []any, cols2 []any, columns []schema.TableColumn) []string {
	var colNames []string
	l1 := len(cols1)
	l2 := len(cols2)

	for i := 0; i < Min(l1, l2); i++ {
		v1 := cols1[i]
		v2 := cols2[i]

		if !consumer.IsColValueEqual(columns[i].Type, columns[i].IsUnsigned, v1, v2) {
			colNames = append(colNames, columns[i].Name)
		}
	}

	for i := Min(l1, l2); i < Max(l1, l2); i++ {
		colNames = append(colNames, columns[i].Name)
	}

	return colNames
}
