package target

import (
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/redis.v1"
)

type Target struct {
	settings *settings.Settings
	logger   *logger.Logger

	Redis *redis.Redis
}

func NewTarget(settings *settings.Settings, logger *logger.Logger) *Target {
	return &Target{
		settings: settings,
		logger:   logger,

		Redis: nil,
	}
}

func (t *Target) Connect() error {
	if t.settings.TargetOptions.RedisOptions != nil {
		t.logger.Info("connecting to redis", zap.Strings("addrs", t.settings.TargetOptions.RedisOptions.Addresses))
		client, err := redis.ConnectToRedis(t.settings.TargetOptions.RedisOptions.ToRedisUniversalOptions(), t.logger.Sugar(), t.settings.TargetOptions.RedisOptions.IsPika)

		if err != nil {
			return err
		}

		t.Redis = client
	}

	return nil
}

func (t *Target) Close() error {
	var err error
	if t.Redis != nil {
		err = errors.WithStack(t.Redis.Close())
	}

	return err
}
