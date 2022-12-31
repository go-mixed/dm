package task

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/dm.v1/src/common"
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

	alias := t.Storage.SaveAndGetTableAlias(e.Table)

	switch e.Action {
	case canal.InsertAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:   nil,
				NewRow:   common.ToRowMap(e.Rows[i], e.Table.Columns),
				DiffCols: nil,
			})
		}
	case canal.DeleteAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:   common.ToRowMap(e.Rows[i], e.Table.Columns),
				NewRow:   nil,
				DiffCols: nil,
			})
		}
	case canal.UpdateAction:
		for i := 0; i < n; i += 2 {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				OldRow:   common.ToRowMap(e.Rows[i], e.Table.Columns),
				NewRow:   common.ToRowMap(e.Rows[i+1], e.Table.Columns),
				DiffCols: common.DiffCols(e.Rows[i], e.Rows[i+1], e.Table.Columns),
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
