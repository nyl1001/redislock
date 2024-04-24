package redislock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDistributedLock struct {
	client *redis.Client
	key    string
	value  string
	ttl    time.Duration
}

func NewRedisDistributedLock(client *redis.Client, key, value string, ttl time.Duration) *RedisDistributedLock {
	return &RedisDistributedLock{
		client: client,
		key:    key,
		value:  value,
		ttl:    ttl,
	}
}

// TryAcquire 尝试获取分布式锁
func (l *RedisDistributedLock) TryAcquire(ctx context.Context) (bool, error) {
	success, err := l.client.SetNX(ctx, l.key, l.value, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}

// Release 释放分布式锁
func (l *RedisDistributedLock) Release(ctx context.Context) error {
	_, err := l.client.EvalSha(
		ctx,
		"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
		[]string{l.key},
		l.value,
	).Result()
	if err != nil {
		return err
	}
	return nil
}

func (l *RedisDistributedLock) executeWithRenewal(ctx context.Context, doSomething func() error) error {
	defer l.Release(ctx)
	done := make(chan error)

	go func() {
		done <- doSomething()
	}()

	var renewalTicker *time.Ticker
	defer func() {
		if renewalTicker != nil {
			renewalTicker.Stop()
		}
	}()

	renewalInterval := l.ttl / 3
	renewalTicker = time.NewTicker(renewalInterval)

	for {
		select {
		case err := <-done:
			// doSomething 函数执行完成
			return err
		case <-renewalTicker.C:
			_, renewErr := l.client.Expire(ctx, l.key, l.ttl).Result()
			if renewErr != nil {
				// 续期失败，可能是锁已经被别人持有，这时我们只需要结束续期循环，等待锁被正常释放
				break
			}
		case <-ctx.Done():
			// 上下文取消时，直接返回错误
			return ctx.Err()
		}
	}
}

func (l *RedisDistributedLock) SafeAcquireAndRelease(ctx context.Context, doSomething func() error) error {
	for {
		acquired, err := l.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if acquired {
			return l.executeWithRenewal(ctx, doSomething)
		}

		// 如果没拿到锁，休眠一段时间后重试
		time.Sleep(time.Millisecond * 100)
	}
}
