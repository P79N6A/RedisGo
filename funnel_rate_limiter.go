package RedisGo

import (
	"encoding/json"
	"git.byted.org/ee/lark/gateway/redis"
	"time"
)

type funnel struct {
	capacity int64     // 最大容量，op
	rate     float64   // 最大频率，op/s
	quota    int64     // 剩余容量，op
	lastTime time.Time // 上次漏水时间
}

// 漏水
func (f *funnel) leak() {
	// 计算截止到上次漏水时间，漏斗漏了多少水
	leakedWater := int64(time.Now().Sub(f.lastTime).Seconds() * f.rate)
	// 如果漏的水不足1，则不记录本次漏水时间，也不做任何操作
	if leakedWater < 1 {
		return
	}
	// 更新漏水时间
	f.lastTime = time.Now()
	// 漏水，并保证剩余容量不会大于最大容量
	f.quota = f.quota + leakedWater
	if f.quota > f.capacity {
		f.quota = f.capacity
	}
}

// 注水
func (f *funnel) infuse(water int64) bool {
	f.leak()
	if f.quota < water {
		return false
	}
	f.quota = f.quota - water
	return true
}

func getFunnel(key string) (*funnel, error) {
	v, err := GetClient().Get(key).Bytes()
	if err != nil {
		if redis.IsNil(err) {
			return nil, nil
		}
		return nil, err
	}
	f := new(funnel)
	err = json.Unmarshal(v, f)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func setFunnel(key string, f *funnel) error {
	v, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return GetClient().Set(key, v, 60*time.Second).Err()
}

func isAllowedByFunnel(key string, rate float64, capacity int64) (bool, error) {
	// 以下行为必须保证是原子操作，所以需要加锁
	if !Lock(key) {
		return false, nil
	}
	defer Unlock(key)
	f, err := getFunnel(key)
	if err != nil {
		return false, err
	}
	if f == nil {
		f = &funnel{capacity, rate, capacity, time.Now()}
	}
	// 检查的同时进行行为注入
	ok := f.infuse(1)
	setFunnel(key, f)
	return ok, nil
}
