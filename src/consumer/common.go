package consumer

type RowEvent struct {
	ID                     uint64
	Schema                 string
	Table                  string
	Alias                  string
	OldRow                 []any
	NewRow                 []any
	DifferentColumnIndices []int
	Action                 string
}

type KV struct {
	Key   string
	Value []byte
}

type KVs []*KV
