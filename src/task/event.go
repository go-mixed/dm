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

	alias := t.Storage.UpdateAndGetTableAlias(e.Table)

	switch e.Action {
	case canal.InsertAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				PreviousRow: nil,
				Row:         common.ToRowMap(e.Rows[i], e.Table.Columns),
				DiffCols:    nil,
			})
		}
	case canal.DeleteAction:
		for i := 0; i < n; i++ {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				PreviousRow: nil,
				Row:         common.ToRowMap(e.Rows[i], e.Table.Columns),
				DiffCols:    nil,
			})
		}
	case canal.UpdateAction:
		for i := 0; i < n; i += 2 {
			rowEvents = append(rowEvents, consumer.RowEvent{
				Action: e.Action,
				Schema: e.Table.Schema,
				Table:  e.Table.Name,
				Alias:  alias,

				PreviousRow: common.ToRowMap(e.Rows[i], e.Table.Columns),
				Row:         common.ToRowMap(e.Rows[i+1], e.Table.Columns),
				DiffCols:    common.DiffCols(e.Rows[i], e.Rows[i+1], e.Table.Columns),
			})
		}
	}

	t.Storage.AddEvents(rowEvents)
	t.trigger.OnCountChanged(t.Storage.Conf.EventCount())
	return nil
}

func (t *Task) OnXID(nextPos mysql.Position) error {
	return nil
}

func (t *Task) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (t *Task) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	t.Storage.AddCanalBinLogPosition(common.NewBinLogPositions(pos))
	return nil
}
