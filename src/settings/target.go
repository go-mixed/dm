package settings

import cache "go-common-cache"

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
