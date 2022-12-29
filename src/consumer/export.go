package consumer

import (
	"github.com/goplus/igop"
	"reflect"
)

/*
qexp -outdir . -filename export_cache xxx
*/

func Export(vars map[string]reflect.Value) {
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
		},
		AliasTypes:    map[string]reflect.Type{},
		Vars:          vars,
		Funcs:         map[string]reflect.Value{},
		TypedConsts:   map[string]igop.TypedConst{},
		UntypedConsts: map[string]igop.UntypedConst{},
	})
}
