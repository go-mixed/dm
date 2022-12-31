package main

import (
	"gopkg.in/go-mixed/dm-consumer.v1"
)

var redis consumer.ICache
var etcd consumer.ICache
var logger consumer.ILogger

func init() {
	redis = consumer.Redis
	etcd = consumer.Etcd
	logger = consumer.Logger

	logger.Debugf("-redis: %t -etcd: %t -logger: %t", redis, etcd, logger)
}
