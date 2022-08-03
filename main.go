package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Number tracks a numberBucket over a bounded number of
// time buckets. Currently the buckets are one second long and only the last 10 seconds are kept.
type Number struct {
	// 每一个秒级时间戳为一个k-v
	Buckets       map[int64]*numberBucket
	Mutex         *sync.RWMutex
	SlidingWindow int64
}

type numberBucket struct {
	Value int64
}

// NewNumber initializes a RollingNumber struct.
func NewNumber() *Number {
	r := &Number{
		Buckets:       make(map[int64]*numberBucket),
		Mutex:         &sync.RWMutex{},
		SlidingWindow: 10,
	}
	return r
}

// 获取当前时间值
func (r *Number) getCurrentBucket() *numberBucket {
	now := time.Now().Unix()
	var bucket *numberBucket
	var ok bool

	if bucket, ok = r.Buckets[now]; !ok {
		bucket = &numberBucket{}
		r.Buckets[now] = bucket
	}

	return bucket
}

// 删除窗口之前的数据
func (r *Number) removeOldBuckets() {
	now := time.Now().Unix() - r.SlidingWindow

	for timestamp := range r.Buckets {
		// TODO: configurable rolling window
		if timestamp <= now {
			delete(r.Buckets, timestamp)
		}
	}
}

// 更新数据
func (r *Number) Increment(i int64) {
	if i == 0 {
		return
	}

	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	b := r.getCurrentBucket()
	b.Value += i
	r.removeOldBuckets()
}

// Sum sums the values over the buckets in the last 10 seconds.
func (r *Number) Sum(now time.Time) int64 {
	var sum int64

	r.Mutex.RLock()
	defer r.Mutex.RUnlock()

	for timestamp, bucket := range r.Buckets {
		// TODO: configurable rolling window
		if timestamp >= now.Unix()-r.SlidingWindow {
			sum += bucket.Value
		}
	}

	return sum
}

func main() {
	n := NewNumber()
	var g errgroup.Group

	// 每秒随机生成一个值 模拟请求数
	g.Go(func() error {
		for i := 0; i < 100; i++ {
			value := rand.Intn(100)
			n.Increment(int64(value))
			time.Sleep(1 * time.Second)
		}
		return nil
	})

	g.Go(func() error {
		for i := 0; i < 100; i++ {
			fmt.Printf("%d秒内请求书： %d\n", n.SlidingWindow, n.Sum(time.Now()))
			time.Sleep(1 * time.Second)
		}
		return nil
	})
	_ = g.Wait()
}
