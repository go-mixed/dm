package settings

import cache "gopkg.in/go-mixed/go-common.v1/cache.v1"

type TargetOptions struct {
	RedisOptions *cache.RedisOptions `yaml:"redis"`
	EtcdOptions  *cache.EtcdConfig   `yaml:"etcd"`
}

func defaultTargetOptions() TargetOptions {
	return TargetOptions{
		RedisOptions: nil,
		EtcdOptions:  nil,
	}
}
