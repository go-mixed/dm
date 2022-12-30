package consumer

import (
	"github.com/pkg/errors"
	cache "go-common-cache"
	"go-common/utils"
	"go-common/utils/core"
	"time"
)

type KV struct {
	Key   string
	Value []byte
}

type KVs []*KV

type IL2Cache interface {
	Get(key string, expire time.Duration, actual any) ([]byte, error)
	MGet(keys []string, expire time.Duration, actual any) (KVs, error)
	Keys(keyPrefix string, expire time.Duration) ([]string, error)
	Delete(keys ...string)

	ScanPrefix(keyPrefix string, expire time.Duration, actual any) (KVs, error)
}

type ICache interface {
	// L2 得到本Cache的二级缓存对象
	L2() IL2Cache
	// Get 查询key的值, 并尝试将其JSON值导出到actual 如果无需导出, actual 传入nil
	Get(key string, actual any) ([]byte, error)
	// MGet 查询多个keys, 返回所有符合要求K/V, 并尝试将JSON数据导出到actual 如果无需导出, actual 传入nil
	// 例子:
	// var result []User
	// RedisGet(keys, &result)
	// 注意: result必须要是slice, 并且只要有一个值无法转换, 都返回错误, 所以这些keys一定要拥有相同的结构
	MGet(keys []string, actual any) (KVs, error)

	// Keys keyPrefix为前缀 返回所有符合要求的keys
	// 注意: 遇到有太多的匹配性, 会阻塞cache的运行
	Keys(keyPrefix string) ([]string, error)
	// Range 在 keyStart~keyEnd中查找符合keyPrefix要求的KV, limit 为 0 表示不限数量
	// 返回nextKey, kv列表, 错误
	Range(keyStart, keyEnd string, keyPrefix string, limit int64) (string, KVs, error)
	// ScanPrefix keyPrefix为前缀, 返回所有符合条件的K/V, 并尝试将JSON数据导出到actual 如果无需导出, actual 传入nil
	// 注意: 不要在keyPrefix中或结尾加入*
	// 例子:
	// var result []User
	// ScanPrefix("users/id/", &result)
	// 注意: result必须要是slice, 并且只要有一个值无法转换, 都返回错误, 所以这些keys一定要拥有相同的结构
	// 注意: 如果有太多的匹配项, 会阻塞cache的运行. 对于大的量级, 尽量使用 ScanPrefixCallback
	ScanPrefix(keyPrefix string, actual any) (KVs, error)
	// ScanPrefixCallback 根据keyPrefix为前缀 查询出所有K/V 遍历调用callback
	// 如果callback返回nil, 会一直搜索直到再无匹配数据; 如果返回错误, 则立即停止搜索
	// 注意: 即使cache中有大量的匹配项, 也不会被阻塞
	ScanPrefixCallback(keyPrefix string, callback func(kv *KV) error) (int64, error)

	// ScanRange 根据keyStart/keyEnd返回所有符合条件的K/V, 并尝试将JSON数据导出到actual 如果无需导出, actual 传入nil
	// 注意: 返回的结果会包含keyStart/keyEnd
	// 如果keyPrefix不为空, 则在keyStart/keyEnd中筛选出符keyPrefix条件的项目
	// 如果limit = 0 表示不限数量
	// 例子:
	// var result []User
	// 从 "users/id/100" 开始, 取前缀为"users/id/"的100个数据
	// ScanRange("users/id/100", "", "users/id/", 100, &result)
	// 比如取a~z的所有数据, 会包含 "a", "a1", "a2xxxxxx", "yyyyyy", "z"
	// ScanRange("a", "z", "", 0, &result)
	// 注意: result必须要是slice, 并且只要有一个值无法转换, 都返回错误, 所以这些keys一定要拥有相同的结构
	ScanRange(keyStart, keyEnd string, keyPrefix string, limit int64, actual any) (string, KVs, error)
	// ScanRangeCallback 根据keyStart/keyEnd返回所有符合条件的K/V, 并遍历调用callback
	// 参数定义参见 ScanRange
	// 如果callback返回nil, 会一直搜索直到再无匹配数据; 如果返回错误, 则立即停止搜索
	ScanRangeCallback(keyStart, keyEnd string, keyPrefix string, limit int64, callback func(kv *KV) error) (string, int64, error)

	// Set 写入KV
	Set(key string, val any, expiration time.Duration) error
	SetNoExpiration(key string, val any) error
	Del(key string) error
}

type iCache struct {
	client cache.ICache
}

type iL2Cache struct {
	l2 cache.IL2Cache
}

func ToConsumerICache(c cache.ICache) ICache {
	// struct 转为 interface后，nil需要反射判断
	if core.IsInterfaceNil(c) {
		return &iCache{nil}
	}
	return &iCache{c}
}

func toConsumerIL2Cache(l cache.IL2Cache) IL2Cache {
	// struct 转为 interface后，nil需要反射判断
	if core.IsInterfaceNil(l) {
		return &iL2Cache{nil}
	}
	return &iL2Cache{l}
}

func toConsumerKV(kv *utils.KV) *KV {
	if kv == nil {
		return nil
	}
	return &KV{Key: kv.Key, Value: kv.Value}
}

func toConsumerKVs(kvs utils.KVs) KVs {
	if kvs == nil {
		return nil
	}
	var _kvs KVs
	for _, kv := range kvs {
		_kvs = append(_kvs, toConsumerKV(kv))
	}

	return _kvs
}

func (i iCache) L2() IL2Cache {
	if i.client == nil {
		panic(errors.New("client is nil in \"L2\""))
	}
	return toConsumerIL2Cache(i.client.L2())
}

func (i iCache) Get(key string, actual any) ([]byte, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"Get\""))
	}
	return i.client.Get(key, actual)
}

func (i iCache) MGet(keys []string, actual any) (KVs, error) {
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

func (i iCache) Range(keyStart, keyEnd string, keyPrefix string, limit int64) (string, KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"Range\""))
	}
	s, kvs, err := i.client.Range(keyStart, keyEnd, keyPrefix, limit)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefix(keyPrefix string, actual any) (KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanPrefix\""))
	}
	kvs, err := i.client.ScanPrefix(keyPrefix, actual)
	return toConsumerKVs(kvs), err
}

func (i iCache) ScanPrefixCallback(keyPrefix string, callback func(*KV) error) (int64, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanPrefixCallback\""))
	}
	return i.client.ScanPrefixCallback(keyPrefix, func(kv *utils.KV) error {
		return callback(toConsumerKV(kv))
	})
}

func (i iCache) ScanRange(keyStart, keyEnd string, keyPrefix string, limit int64, actual any) (string, KVs, error) {
	if i.client == nil {
		panic(errors.New("client is nil in \"ScanRange\""))
	}
	s, kvs, err := i.client.ScanRange(keyStart, keyEnd, keyPrefix, limit, actual)
	return s, toConsumerKVs(kvs), err
}

func (i iCache) ScanRangeCallback(keyStart, keyEnd string, keyPrefix string, limit int64, callback func(*KV) error) (string, int64, error) {
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

func (i iL2Cache) Get(key string, expire time.Duration, actual any) ([]byte, error) {
	if i.l2 == nil {
		panic(errors.New("client l2 is nil in \"Get\""))
	}
	return i.l2.Get(key, expire, actual)
}

func (i iL2Cache) MGet(keys []string, expire time.Duration, actual any) (KVs, error) {
	if i.l2 == nil {
		panic(errors.New("client l2 is nil in \"MGet\""))
	}
	kvs, err := i.l2.MGet(keys, expire, actual)
	return toConsumerKVs(kvs), err
}

func (i iL2Cache) Keys(keyPrefix string, expire time.Duration) ([]string, error) {
	if i.l2 == nil {
		panic(errors.New("client l2 is nil in \"Keys\""))
	}
	return i.l2.Keys(keyPrefix, expire)
}

func (i iL2Cache) Delete(keys ...string) {
	if i.l2 == nil {
		panic(errors.New("client l2 is nil in \"Delete\""))
	}
	i.l2.Delete(keys...)
}

func (i iL2Cache) ScanPrefix(keyPrefix string, expire time.Duration, actual any) (KVs, error) {
	if i.l2 == nil {
		panic(errors.New("client l2 is nil in \"ScanPrefix\""))
	}
	kvs, err := i.l2.ScanPrefix(keyPrefix, expire, actual)
	return toConsumerKVs(kvs), err
}
