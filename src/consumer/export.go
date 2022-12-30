package consumer

import (
	"github.com/goplus/igop"
	cache "go-common-cache"
	"go-common/utils"
	"go.uber.org/zap"
	"reflect"
)

/*
qexp -outdir . -filename export_cache xxx
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
			"RowEvent": reflect.TypeOf((*RowEvent)(nil)).Elem(),
			"KV":       reflect.TypeOf((*KV)(nil)).Elem(),
			"KVs":      reflect.TypeOf((*KVs)(nil)).Elem(),
			"ICache":   reflect.TypeOf((*ICache)(nil)).Elem(),
			"IL2Cache": reflect.TypeOf((*IL2Cache)(nil)).Elem(),
			"ILogger":  reflect.TypeOf((*ILogger)(nil)).Elem(),
		},
		AliasTypes: map[string]reflect.Type{},
		Vars: map[string]reflect.Value{
			"Logger": reflect.ValueOf(Logger),
			"Redis":  reflect.ValueOf(Redis),
			"Etcd":   reflect.ValueOf(Etcd),
		},
		Funcs:         map[string]reflect.Value{},
		TypedConsts:   map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{},
	})
}
