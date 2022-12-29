package mysql

import (
	"fmt"
	"github.com/fly-studio/dm/src/common"
	"github.com/fly-studio/dm/src/settings"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/errors"
	"go-common/utils"
	"go.uber.org/zap"
	"net/url"
	"strings"
)

type MySql struct {
	settings *settings.Settings
	logger   *utils.Logger

	connection *sqlx.DB
}

func NewMySql(settings *settings.Settings, logger *utils.Logger) *MySql {
	return &MySql{
		settings:   settings,
		logger:     logger,
		connection: nil,
	}
}

func (s *MySql) Connect() error {
	param := url.Values{}
	param.Add("parseTime", "true")
	param.Add("loc", s.settings.MySqlOptions.TimeZone)
	param.Add("timeout", fmt.Sprintf("%dms", s.settings.MySqlOptions.ConnectTimeout.Milliseconds()))
	param.Add("charset", s.settings.MySqlOptions.Charset)

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/information_schema?%s", s.settings.MySqlOptions.Username, s.settings.MySqlOptions.Password, s.settings.MySqlOptions.Host, param.Encode())
	s.logger.Info("connecting mysql", zap.String("dns", strings.ReplaceAll(dsn, s.settings.MySqlOptions.Password, "****")))
	var err error
	if s.connection, err = sqlx.Connect("mysql", dsn); err != nil {
		return errors.WithStack(err)
	}

	if s.settings.MySqlOptions.MaxOpenConns > 0 {
		s.connection.SetMaxOpenConns(s.settings.MySqlOptions.MaxOpenConns)
	}
	if s.settings.MySqlOptions.MaxIdleConns > 0 {
		s.connection.SetMaxIdleConns(s.settings.MySqlOptions.MaxIdleConns)
	}
	if s.settings.MySqlOptions.ConnMaxLifetime > 0 {
		s.connection.SetConnMaxLifetime(s.settings.MySqlOptions.ConnMaxLifetime)
	}
	if s.settings.MySqlOptions.ConnMaxIdleTime > 0 {
		s.connection.SetConnMaxIdleTime(s.settings.MySqlOptions.ConnMaxIdleTime)
	}

	databases, err := s.Databases()
	if err != nil {
		return errors.WithStack(err)
	}
	s.logger.Sugar().Debugf("all databases: %+v", databases)

	return nil
}

func (s *MySql) Databases() ([]string, error) {
	type db struct {
		Database string `db:"Database"`
	}
	var dbs []db
	if err := s.connection.Select(&dbs, "SHOW DATABASES;"); err != nil {
		return nil, err
	}

	var _dbs []string
	for _, d := range dbs {
		_dbs = append(_dbs, d.Database)
	}

	return _dbs, nil
}

func (s *MySql) AllTables() (common.Tables, error) {
	var tables []common.Table
	if err := s.connection.Select(&tables, "SELECT * FROM `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_TYPE` = 'BASE TABLE'"); err != nil {
		return nil, err
	}

	var _tables = common.Tables{}
	for _, table := range tables {
		_tables[strings.ToLower(table.Table)] = &table
	}
	return _tables, nil
}

func (s *MySql) Tables(schema string) (common.Tables, error) {
	var tables []common.Table
	if err := s.connection.Select(&tables, "SELECT * FROM `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_TYPE` = 'BASE TABLE' AND `TABLE_SCHEMA` = ?", schema); err != nil {
		return nil, err
	}

	var _tables = common.Tables{}
	for _, table := range tables {
		_tables[strings.ToLower(table.Table)] = &table
	}
	return _tables, nil
}

func (s *MySql) Columns(schema string, table string) (common.Columns, error) {
	var columns common.Columns
	if err := s.connection.Select(&columns, "SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? ORDER BY `ORDINAL_POSITION`", schema, table); err != nil {
		return nil, errors.WithStack(err)
	}

	return columns, nil
}

func (s *MySql) Close() error {
	if s.connection != nil {
		return s.connection.Close()
	}

	return nil
}
