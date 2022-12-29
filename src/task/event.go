package task

import (
	"github.com/fly-studio/dm/src/common"
	"github.com/fly-studio/dm/src/consumer"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func (t *Task) OnRotate(rotateEvent *replication.RotateEvent) error {
	return nil
}

func (t *Task) OnTableChanged(schema string, table string) error {
	return nil
}

func (t *Task) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (t *Task) OnRow(e *canal.RowsEvent) error {

	n := len(e.Rows)
	var rowEvents []consumer.RowEvent

	alias, _ := t.Storage.GetTable(e.Table)

	switch e.Action {
	case canal.InsertAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:                 nil,
				NewRow:                 e.Rows[i],
				DifferentColumnIndices: common.DiffCols(nil, e.Rows[i], e.Table.Columns),
			})
		}
	case canal.DeleteAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:                 e.Rows[i],
				NewRow:                 nil,
				DifferentColumnIndices: common.DiffCols(e.Rows[i], nil, e.Table.Columns),
			})
		}
	case canal.UpdateAction:
		for i := 0; i < n; i += 2 {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:                 e.Rows[i],
				NewRow:                 e.Rows[i+1],
				DifferentColumnIndices: common.DiffCols(e.Rows[i], e.Rows[i+1], e.Table.Columns),
			})
		}
	}

	t.Storage.SaveEvents(rowEvents)
	t.trigger.OnCountChanged(t.Storage.EventCount())
	return nil
}

func (t *Task) OnXID(nextPos mysql.Position) error {
	return nil
}

func (t *Task) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (t *Task) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	_pos := common.NewBinLogPositions(pos)
	t.Storage.SaveBinLogPosition(_pos)
	t.binLog = _pos

	return nil
}
