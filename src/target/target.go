package target

import (
	"github.com/pingcap/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	cache "gopkg.in/go-mixed/go-common.v1/cache.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
)

type Target struct {
	settings *settings.Settings
	logger   *logger.Logger

	Redis *cache.Redis
	Etcd  *cache.Etcd
}

func NewTarget(settings *settings.Settings, logger *logger.Logger) *Target {
	return &Target{
		settings: settings,
		logger:   logger,

		Redis: nil,
		Etcd:  nil,
	}
}

func (t *Target) Connect() error {
	if t.settings.TargetOptions.RedisOptions != nil {
		t.logger.Info("connecting to redis", zap.Strings("addrs", t.settings.TargetOptions.RedisOptions.Addresses))
		client, err := cache.ConnectToRedis(t.settings.TargetOptions.RedisOptions.ToRedisUniversalOptions(), t.logger.Sugar(), t.settings.TargetOptions.RedisOptions.IsPika)

		if err != nil {
			return err
		}

		t.Redis = client
	}

	if t.settings.TargetOptions.EtcdOptions != nil {
		t.logger.Info("connecting to etcd", zap.Strings("endpoints", t.settings.TargetOptions.EtcdOptions.Endpoints))
		client, err := cache.ConnectToEtcd(t.settings.TargetOptions.EtcdOptions.ToEtcdConfig(t.logger.ZapLogger()), t.logger.Sugar())

		if err != nil {
			return err
		}

		t.Etcd = client
	}

	return nil
}

func (t *Target) Close() error {
	var err error
	if t.Redis != nil {
		err = errors.WithStack(t.Redis.Close())
	}

	if t.Etcd != nil {
		err = multierr.Append(err, errors.WithStack(t.Etcd.Close()))
	}

	return err
}
