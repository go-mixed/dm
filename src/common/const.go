package common

type TaskMode string

const (
	ALL         TaskMode = "all"
	FULL        TaskMode = "full"
	INCREMENTAL TaskMode = "incremental"
)

const StorageConfFilename = "conf.yml"
const StorageEventBucket = "events"

const LogCanalFilename = "canal.log"
