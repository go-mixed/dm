package main

import (
	"github.com/fly-studio/dm/src/consumer"
)

func Consumer(events []consumer.RowEvent, args []string) error {
	logger.Debugf("consumer row events: %d\n", len(events))

	err := redis.SetNoExpiration("test", "value")

	return err
}
