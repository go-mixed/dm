module github.com/fly-studio/dm

require (
	github.com/fly-studio/igop v1.0.1
	github.com/go-mysql-org/go-mysql v1.6.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/goplus/igop v0.9.6
	github.com/jmoiron/sqlx v1.3.4
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pkg/errors v0.9.1
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	github.com/spf13/cobra v1.6.1
	go-common v0.0.0
	go-common-cache v0.0.0
	go-common-storage v0.0.0
	go.etcd.io/bbolt v1.3.6
	go.uber.org/multierr v1.9.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20221217163422-3c43f8badb15
)

require (
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.1 // indirect
	github.com/go-redis/redis/v9 v9.0.0-rc.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/goplus/gop v1.1.3 // indirect
	github.com/goplus/gox v1.11.22 // indirect
	github.com/goplus/ipkg v0.1.3 // indirect
	github.com/goplus/mod v0.9.12 // indirect
	github.com/goplus/reflectx v0.9.8 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/mattn/go-shellwords v1.0.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pingcap/log v0.0.0-20210317133921-96f4fcab92a4 // indirect
	github.com/pingcap/parser v0.0.0-20210415081931-48e7f467fd74 // indirect
	github.com/qiniu/x v1.11.9 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/utahta/go-cronowriter v1.2.0 // indirect
	github.com/visualfc/funcval v0.1.3 // indirect
	github.com/visualfc/goembed v0.3.3 // indirect
	github.com/visualfc/goid v0.2.0 // indirect
	github.com/visualfc/xtype v0.1.2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.6 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.6 // indirect
	go.etcd.io/etcd/client/v3 v3.5.6 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.4.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	golang.org/x/tools v0.4.0 // indirect
	google.golang.org/genproto v0.0.0-20221207170731-23e4bf6bdc37 // indirect
	google.golang.org/grpc v1.51.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go-common => ../go-common

replace go-common-cache => ../go-common/cache

replace go-common-storage => ../go-common/storage

go 1.19
