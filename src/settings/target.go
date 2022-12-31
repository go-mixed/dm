package settings

import (
	"gopkg.in/go-mixed/go-common.v1/redis.v1"
)

type TargetOptions struct {
	RedisOptions *redis.RedisOptions `yaml:"redis"`
}

func defaultTargetOptions() TargetOptions {
	return TargetOptions{
		RedisOptions: nil,
	}
}
