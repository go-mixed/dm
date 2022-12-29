package main

import "github.com/fly-studio/dm/src/consumer"

var redis consumer.ICache
var etcd consumer.ICache

func init() {
	redis = consumer.Redis
	etcd = consumer.Etcd
}
