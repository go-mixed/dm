package exporter

import (
	"github.com/goplus/igop"
	"go.uber.org/zap"
	"go/constant"
	"gopkg.in/go-mixed/dm-consumer.v1"
	cache "gopkg.in/go-mixed/go-common.v1/cache.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/conv"
	"reflect"
)

/*
qexp -outdir . -filename export github.com/fly-studio/dm/src/consumer/conv
*/

func SetRedis(redis cache.ICache) {
	consumer.Redis = ToConsumerICache(redis)
}
func SetEtcd(etcd cache.ICache) {
	consumer.Etcd = ToConsumerICache(etcd)
}
func SetLogger(logger *logger.Logger) {
	consumer.Logger = ToConsumerILogger(logger.With(zap.String("scope", "script")).Sugar())
}

func Export() {
	igop.RegisterPackage(&igop.Package{
		Name: "consumer",
		Path: "gopkg.in/go-mixed/dm-consumer.v1",
		Deps: map[string]string{
			"time": "time",
		},
		Interfaces: map[string]reflect.Type{},
		NamedTypes: map[string]reflect.Type{
			"RowEvent":    reflect.TypeOf((*consumer.RowEvent)(nil)).Elem(),
			"KV":          reflect.TypeOf((*consumer.KV)(nil)).Elem(),
			"KVs":         reflect.TypeOf((*consumer.KVs)(nil)).Elem(),
			"ICache":      reflect.TypeOf((*consumer.ICache)(nil)).Elem(),
			"ILogger":     reflect.TypeOf((*consumer.ILogger)(nil)).Elem(),
			"Table":       reflect.TypeOf((*consumer.Table)(nil)).Elem(),
			"TableColumn": reflect.TypeOf((*consumer.TableColumn)(nil)).Elem(),
			"TableIndex":  reflect.TypeOf((*consumer.TableIndex)(nil)).Elem(),
		},
		AliasTypes: map[string]reflect.Type{},
		Vars: map[string]reflect.Value{
			"Logger": reflect.ValueOf(consumer.Logger),
			"Redis":  reflect.ValueOf(consumer.Redis),
			"Etcd":   reflect.ValueOf(consumer.Etcd),
		},
		Funcs:       map[string]reflect.Value{},
		TypedConsts: map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{
			"TYPE_NUMBER":     {"untyped int", constant.MakeInt64(int64(consumer.TYPE_NUMBER))},
			"TYPE_FLOAT":      {"untyped int", constant.MakeInt64(int64(consumer.TYPE_FLOAT))},
			"TYPE_ENUM":       {"untyped int", constant.MakeInt64(int64(consumer.TYPE_ENUM))},
			"TYPE_SET":        {"untyped int", constant.MakeInt64(int64(consumer.TYPE_SET))},
			"TYPE_STRING":     {"untyped int", constant.MakeInt64(int64(consumer.TYPE_STRING))},
			"TYPE_DATETIME":   {"untyped int", constant.MakeInt64(int64(consumer.TYPE_DATETIME))},
			"TYPE_TIMESTAMP":  {"untyped int", constant.MakeInt64(int64(consumer.TYPE_TIMESTAMP))},
			"TYPE_DATE":       {"untyped int", constant.MakeInt64(int64(consumer.TYPE_DATE))},
			"TYPE_TIME":       {"untyped int", constant.MakeInt64(int64(consumer.TYPE_TIME))},
			"TYPE_BIT":        {"untyped int", constant.MakeInt64(int64(consumer.TYPE_BIT))},
			"TYPE_JSON":       {"untyped int", constant.MakeInt64(int64(consumer.TYPE_JSON))},
			"TYPE_DECIMAL":    {"untyped int", constant.MakeInt64(int64(consumer.TYPE_DECIMAL))},
			"TYPE_MEDIUM_INT": {"untyped int", constant.MakeInt64(int64(consumer.TYPE_MEDIUM_INT))},
			"TYPE_BINARY":     {"untyped int", constant.MakeInt64(int64(consumer.TYPE_BINARY))},
			"TYPE_POINT":      {"untyped int", constant.MakeInt64(int64(consumer.TYPE_POINT))},
		},
	})

	igop.RegisterPackage(&igop.Package{
		Name: "conv",
		Path: "gopkg.in/go-mixed/dm-consumer.v1/conv",
		Deps: map[string]string{
			"encoding/hex": "hex",
			"fmt":          "fmt",
			"strconv":      "strconv",
			"strings":      "strings",
		},
		Interfaces: map[string]reflect.Type{},
		NamedTypes: map[string]reflect.Type{},
		AliasTypes: map[string]reflect.Type{},
		Vars:       map[string]reflect.Value{},
		Funcs: map[string]reflect.Value{
			"AnyToBool":         reflect.ValueOf(conv.AnyToBool),
			"AnyToFloat64":      reflect.ValueOf(conv.AnyToFloat64),
			"AnyToInt64":        reflect.ValueOf(conv.AnyToInt64),
			"AnyToString":       reflect.ValueOf(conv.AnyToString),
			"AnyToUint64":       reflect.ValueOf(conv.AnyToUint64),
			"Atof":              reflect.ValueOf(conv.Atof),
			"Atof64":            reflect.ValueOf(conv.Atof64),
			"Atoi":              reflect.ValueOf(conv.Atoi),
			"Atoi64":            reflect.ValueOf(conv.Atoi64),
			"Atou64":            reflect.ValueOf(conv.Atou64),
			"BytesToHex":        reflect.ValueOf(conv.BytesToHex),
			"Ftoa":              reflect.ValueOf(conv.Ftoa),
			"HexToBytes":        reflect.ValueOf(conv.HexToBytes),
			"HexToInt":          reflect.ValueOf(conv.HexToInt),
			"I64toa":            reflect.ValueOf(conv.I64toa),
			"IntToHex":          reflect.ValueOf(conv.IntToHex),
			"IsInt":             reflect.ValueOf(conv.IsInt),
			"IsInt64":           reflect.ValueOf(conv.IsInt64),
			"IsUint64":          reflect.ValueOf(conv.IsUint64),
			"Itoa":              reflect.ValueOf(conv.Itoa),
			"PaddingInt64":      reflect.ValueOf(conv.PaddingInt64),
			"PaddingUint64":     reflect.ValueOf(conv.PaddingUint64),
			"ParseFloat":        reflect.ValueOf(conv.ParseFloat),
			"ParseInt":          reflect.ValueOf(conv.ParseInt),
			"ParseUint":         reflect.ValueOf(conv.ParseUint),
			"PercentageToFloat": reflect.ValueOf(conv.PercentageToFloat),
			"U64toa":            reflect.ValueOf(conv.U64toa),
		},
		TypedConsts:   map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{},
	})
}
