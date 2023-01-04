package component

import (
	"go.uber.org/multierr"
	"gopkg.in/go-mixed/dm.v1/src/mysql"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/dm.v1/src/storage"
	"gopkg.in/go-mixed/dm.v1/src/target"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
)

type Components struct {
	Settings *settings.Settings
	Logger   *logger.Logger

	Mysql   *mysql.MySql
	Target  *target.Target
	Storage *storage.Storage
}

func (c *Components) CloseComponents() error {
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
