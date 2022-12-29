package settings

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"math/rand"
	"runtime"
	"time"
)

type MySqlOptions struct {
	Host string `yaml:"host" validate:"required,hostname_port"`
	/**
	 * Create A user like this:
	 * CREATE USER canal IDENTIFIED BY 'Your Password';
	 * GRANT SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
	 * FLUSH PRIVILEGES;
	 */
	Username string `yaml:"username" validate:"required"`
	Password string `yaml:"password"`
	ServerID uint32 `yaml:"server_id" validate:"required"`

	// "Asia/Shanghai"
	TimeZone string `yaml:"timezone" validate:"required,timezone"`
	Charset  string `yaml:"charset"`

	ReadTimeout     time.Duration `yaml:"read_timeout"`
	ConnectTimeout  time.Duration `yaml:"connect_timeout"`
	HeartbeatPeriod time.Duration `yaml:"heartbeat_period"`

	MaxReconnectAttempts int `yaml:"max_reconnect_attempts"`

	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`

	// mysql, mariadb
	Flavor string `yaml:"flavor" validate:"required"`
}

func defaultMySqlOptions() MySqlOptions {
	return MySqlOptions{
		Host:     "",
		Username: "",
		Password: "",
		ServerID: uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001,

		TimeZone: time.UTC.String(),
		Charset:  mysql.DEFAULT_CHARSET,

		ReadTimeout:     600_000 * time.Millisecond,
		ConnectTimeout:  60_000 * time.Millisecond,
		HeartbeatPeriod: 30_000 * time.Millisecond,

		MaxReconnectAttempts: 10,

		MaxOpenConns:    runtime.NumCPU() * 2,
		MaxIdleConns:    runtime.NumCPU(),
		ConnMaxLifetime: 200_000 * time.Millisecond,
		ConnMaxIdleTime: 60_000 * time.Millisecond,
	}
}
