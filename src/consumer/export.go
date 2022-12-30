package consumer

import (
	"github.com/fly-studio/dm/src/consumer/conv"
	"github.com/goplus/igop"
	cache "go-common-cache"
	"go-common/utils"
	"go.uber.org/zap"
	"go/constant"
	"reflect"
)

/*
qexp -outdir . -filename export github.com/fly-studio/dm/src/consumer/conv
*/

var Redis ICache
var Etcd ICache
var Logger ILogger

func SetRedis(redis cache.ICache) {
	Redis = ToConsumerICache(redis)
}
func SetEtcd(etcd cache.ICache) {
	Etcd = ToConsumerICache(etcd)
}
func SetLogger(logger *utils.Logger) {
	Logger = ToConsumerILogger(logger.With(zap.String("scope", "script")).Sugar())
}

func Export() {
	igop.RegisterPackage(&igop.Package{
		Name: "consumer",
		Path: "github.com/fly-studio/dm/src/consumer",
		Deps: map[string]string{
			"time": "time",
		},
		Interfaces: map[string]reflect.Type{},
		NamedTypes: map[string]reflect.Type{
			"RowEvent":    reflect.TypeOf((*RowEvent)(nil)).Elem(),
			"KV":          reflect.TypeOf((*KV)(nil)).Elem(),
			"KVs":         reflect.TypeOf((*KVs)(nil)).Elem(),
			"ICache":      reflect.TypeOf((*ICache)(nil)).Elem(),
			"IL2Cache":    reflect.TypeOf((*IL2Cache)(nil)).Elem(),
			"ILogger":     reflect.TypeOf((*ILogger)(nil)).Elem(),
			"Table":       reflect.TypeOf((*Table)(nil)).Elem(),
			"TableColumn": reflect.TypeOf((*TableColumn)(nil)).Elem(),
			"TableIndex":  reflect.TypeOf((*TableIndex)(nil)).Elem(),
		},
		AliasTypes: map[string]reflect.Type{},
		Vars: map[string]reflect.Value{
			"Logger": reflect.ValueOf(Logger),
			"Redis":  reflect.ValueOf(Redis),
			"Etcd":   reflect.ValueOf(Etcd),
		},
		Funcs:       map[string]reflect.Value{},
		TypedConsts: map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{
			"TYPE_NUMBER":     {"untyped int", constant.MakeInt64(int64(TYPE_NUMBER))},
			"TYPE_FLOAT":      {"untyped int", constant.MakeInt64(int64(TYPE_FLOAT))},
			"TYPE_ENUM":       {"untyped int", constant.MakeInt64(int64(TYPE_ENUM))},
			"TYPE_SET":        {"untyped int", constant.MakeInt64(int64(TYPE_SET))},
			"TYPE_STRING":     {"untyped int", constant.MakeInt64(int64(TYPE_STRING))},
			"TYPE_DATETIME":   {"untyped int", constant.MakeInt64(int64(TYPE_DATETIME))},
			"TYPE_TIMESTAMP":  {"untyped int", constant.MakeInt64(int64(TYPE_TIMESTAMP))},
			"TYPE_DATE":       {"untyped int", constant.MakeInt64(int64(TYPE_DATE))},
			"TYPE_TIME":       {"untyped int", constant.MakeInt64(int64(TYPE_TIME))},
			"TYPE_BIT":        {"untyped int", constant.MakeInt64(int64(TYPE_BIT))},
			"TYPE_JSON":       {"untyped int", constant.MakeInt64(int64(TYPE_JSON))},
			"TYPE_DECIMAL":    {"untyped int", constant.MakeInt64(int64(TYPE_DECIMAL))},
			"TYPE_MEDIUM_INT": {"untyped int", constant.MakeInt64(int64(TYPE_MEDIUM_INT))},
			"TYPE_BINARY":     {"untyped int", constant.MakeInt64(int64(TYPE_BINARY))},
			"TYPE_POINT":      {"untyped int", constant.MakeInt64(int64(TYPE_POINT))},
		},
	})

	igop.RegisterPackage(&igop.Package{
		Name: "conv",
		Path: "github.com/fly-studio/dm/src/consumer/conv",
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
			"BytesToHex":        reflect.ValueOf(conv.BytesToHex),
			"Ftoa":              reflect.ValueOf(conv.Ftoa),
			"HexToBytes":        reflect.ValueOf(conv.HexToBytes),
			"HexToInt":          reflect.ValueOf(conv.HexToInt),
			"I64toa":            reflect.ValueOf(conv.I64toa),
			"IntToHex":          reflect.ValueOf(conv.IntToHex),
			"IsInt":             reflect.ValueOf(conv.IsInt),
			"IsInt64":           reflect.ValueOf(conv.IsInt64),
			"Itoa":              reflect.ValueOf(conv.Itoa),
			"PaddingInt64":      reflect.ValueOf(conv.PaddingInt64),
			"PaddingUint64":     reflect.ValueOf(conv.PaddingUint64),
			"ParseFloat":        reflect.ValueOf(conv.ParseFloat),
			"ParseInt":          reflect.ValueOf(conv.ParseInt),
			"PercentageToFloat": reflect.ValueOf(conv.PercentageToFloat),
			"Ui64toa":           reflect.ValueOf(conv.Ui64toa),
		},
		TypedConsts:   map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{},
	})
}
