package component

import (
	"github.com/fly-studio/dm/src/mysql"
	"github.com/fly-studio/dm/src/settings"
	"github.com/fly-studio/dm/src/storage"
	"github.com/fly-studio/dm/src/target"
	"go-common/utils"
	"go.uber.org/multierr"
)

type Components struct {
	Settings *settings.Settings
	Logger   *utils.Logger

	Mysql   *mysql.MySql
	Target  *target.Target
	Storage *storage.Storage
}

func (c *Components) Close() error {
	var err error
	if c.Mysql != nil {
		err = c.Mysql.Close()
	}
	if c.Storage != nil {
		err = multierr.Append(err, c.Storage.Close())
	}

	if c.Target != nil {
		err = multierr.Append(err, c.Target.Close())
	}

	return err
}
