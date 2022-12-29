package common

import (
	"github.com/fly-studio/dm/src/consumer"
	cache "go-common-cache"
	"go-common/utils"
	"time"
)

type iCache struct {
	client cache.ICache
}

type iL2Cache struct {
	l2 cache.IL2Cache
}

func ToConsumerICache(c cache.ICache) consumer.ICache {
	return &iCache{c}
}

func toConsumerIL2Cache(l cache.IL2Cache) consumer.IL2Cache {
	return &iL2Cache{l}
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

func (i iCache) L2() consumer.IL2Cache {
	return toConsumerIL2Cache(i.client.L2())
}

func (i iCache) Get(key string, actual any) ([]byte, error) {
	return i.client.Get(key, actual)
}

func (i iCache) MGet(keys []string, actual any) (consumer.KVs, error) {
	kvs, err := i.client.MGet(keys, actual)
	return toConsumerKVs(kvs), err
}

func (i iCache) Keys(keyPrefix string) ([]string, error) {
	return i.client.Keys(keyPrefix)
}

func (i iCache) Range(keyStart, keyEnd string, keyPrefix string, limit int64) (string, consumer.KVs, error) {
	s, kvs, err := i.client.Range(keyStart, keyEnd, keyPrefix, limit)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefix(keyPrefix string, actual any) (consumer.KVs, error) {
	kvs, err := i.client.ScanPrefix(keyPrefix, actual)
	return toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefixCallback(keyPrefix string, callback func(*consumer.KV) error) (int64, error) {
	return i.client.ScanPrefixCallback(keyPrefix, func(kv *utils.KV) error {
		return callback(toConsumerKV(kv))
	})
}

func (i iCache) ScanRange(keyStart, keyEnd string, keyPrefix string, limit int64, actual any) (string, consumer.KVs, error) {
	s, kvs, err := i.client.ScanRange(keyStart, keyEnd, keyPrefix, limit, actual)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanRangeCallback(keyStart, keyEnd string, keyPrefix string, limit int64, callback func(*consumer.KV) error) (string, int64, error) {
	return i.client.ScanRangeCallback(keyStart, keyEnd, keyPrefix, limit, func(kv *utils.KV) error {
		return callback(toConsumerKV(kv))
	})
}

func (i iCache) Set(key string, val any, expiration time.Duration) error {
	return i.client.Set(key, val, expiration)
}

func (i iCache) SetNoExpiration(key string, val any) error {
	return i.client.SetNoExpiration(key, val)
}

func (i iCache) Del(key string) error {
	return i.client.Del(key)
}

func (i iL2Cache) Get(key string, expire time.Duration, actual any) ([]byte, error) {
	return i.l2.Get(key, expire, actual)
}

func (i iL2Cache) MGet(keys []string, expire time.Duration, actual any) (consumer.KVs, error) {
	kvs, err := i.l2.MGet(keys, expire, actual)
	return toConsumerKVs(kvs), err
}

func (i iL2Cache) Keys(keyPrefix string, expire time.Duration) ([]string, error) {
	return i.l2.Keys(keyPrefix, expire)
}

func (i iL2Cache) Delete(keys ...string) {
	i.l2.Delete(keys...)
}

func (i iL2Cache) ScanPrefix(keyPrefix string, expire time.Duration, actual any) (consumer.KVs, error) {
	kvs, err := i.l2.ScanPrefix(keyPrefix, expire, actual)
	return toConsumerKVs(kvs), err
}
