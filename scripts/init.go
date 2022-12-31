package main

import (
	"gopkg.in/go-mixed/dm-consumer.v1"
)

var redis consumer.IKV
var logger consumer.ILogger

func init() {
	redis = consumer.Redis
	logger = consumer.Logger

	logger.Debugf("-redis: %t -logger: %t", redis, logger)
}
