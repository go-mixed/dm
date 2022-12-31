package exporter

import (
	"github.com/pkg/errors"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
	cache "gopkg.in/go-mixed/go-common.v1/cache.v1"
	"gopkg.in/go-mixed/go-common.v1/utils"
	"gopkg.in/go-mixed/go-common.v1/utils/core"
	"time"
)

type iCache struct {
	client cache.ICache
}

func ToConsumerICache(c cache.ICache) consumer.ICache {
	// struct 转为 interface后，nil需要反射判断
	if core.IsInterfaceNil(c) {
		return &iCache{nil}
	}
	return &iCache{c}
}

func toConsumerKV(kv *utils.KV) *consumer.KV {
	if kv == nil {
		return nil
	}
	return &consumer.KV{Key: kv.Key, Value: kv.Value}
}

func toConsumerKVs(kvs utils.KVs) consumer.KVs {
	if kvs == nil {
		return nil
	}
	var _kvs consumer.KVs
	for _, kv := range kvs {
		_kvs = append(_kvs, toConsumerKV(kv))
	}

	return _kvs
}

func (i iCache) Get(key string, actual any) ([]byte, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"Get\""))
	}
	return i.client.Get(key, actual)
}

func (i iCache) MGet(keys []string, actual any) (consumer.KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"MGet\""))
	}
	kvs, err := i.client.MGet(keys, actual)
	return toConsumerKVs(kvs), err
}

func (i iCache) Keys(keyPrefix string) ([]string, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"Keys\""))
	}
	return i.client.Keys(keyPrefix)
}

func (i iCache) Range(keyStart, keyEnd string, keyPrefix string, limit int64) (string, consumer.KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"Range\""))
	}
	s, kvs, err := i.client.Range(keyStart, keyEnd, keyPrefix, limit)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefix(keyPrefix string, actual any) (consumer.KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanPrefix\""))
	}
	kvs, err := i.client.ScanPrefix(keyPrefix, actual)
	return toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefixCallback(keyPrefix string, callback func(*consumer.KV) error) (int64, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanPrefixCallback\""))
	}
	return i.client.ScanPrefixCallback(keyPrefix, func(kv *utils.KV) error {
		return callback(toConsumerKV(kv))
	})
}

func (i iCache) ScanRange(keyStart, keyEnd string, keyPrefix string, limit int64, actual any) (string, consumer.KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanRange\""))
	}
	s, kvs, err := i.client.ScanRange(keyStart, keyEnd, keyPrefix, limit, actual)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanRangeCallback(keyStart, keyEnd string, keyPrefix string, limit int64, callback func(*consumer.KV) error) (string, int64, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanRangeCallback\""))
	}
	return i.client.ScanRangeCallback(keyStart, keyEnd, keyPrefix, limit, func(kv *utils.KV) error {
		return callback(toConsumerKV(kv))
	})
}

func (i iCache) Set(key string, val any, expiration time.Duration) error {
	if i.client == nil {
		panic(errors.New("client is nil in \"Set\""))
	}
	return i.client.Set(key, val, expiration)
}

func (i iCache) SetNoExpiration(key string, val any) error {
	if i.client == nil {
		panic(errors.New("client is nil in \"SetNoExpiration\""))
	}
	return i.client.SetNoExpiration(key, val)
}

func (i iCache) Del(key string) error {
	if i.client == nil {
		panic(errors.New("client is nil in \"Del\""))
	}
	return i.client.Del(key)
}
