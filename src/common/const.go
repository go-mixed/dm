package common

type TaskMode string

const (
	ALL         TaskMode = "all"
	FULL        TaskMode = "full"
	INCREMENTAL TaskMode = "incremental"
)

const StoragePositionStatusFilename = "position-status.yml"
const StorageEventBucket = "events"

const LogCanalFilename = "canal.log"
