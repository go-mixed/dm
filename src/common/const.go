package common

type TaskMode string

const (
	ALL         TaskMode = "all"
	FULL        TaskMode = "full"
	INCREMENTAL TaskMode = "incremental"
)

const PositionFilename = "master-info.yml"
const BoltFilename = "data.db"

const StorageTables = "tables"
const StorageEvents = "events"

const LogCanalFilename = "canal.log"
