package main

import (
	"gopkg.in/go-mixed/dm-consumer.v1"
)

func Consumer(events []consumer.RowEvent, args []string) error {
	logger.Debugf("consumer row events: %d\n", len(events))

	err := redis.SetNoExpiration("test", "value")

	return err
}
