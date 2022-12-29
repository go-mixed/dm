package main

import (
	"fmt"
	"github.com/fly-studio/dm/src/consumer"
)

func Consumer(events []consumer.RowEvent) {
	fmt.Printf("redis: %t\n", redis)
	fmt.Printf("etcd: %t\n", etcd)

	fmt.Printf("row events: %d\n", len(events))
}
