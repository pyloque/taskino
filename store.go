package taskino

import (
	"fmt"
	"github.com/go-redis/redis"
	"math/rand"
	"strings"
	"time"
)

type TaskStore interface {
	GetRemoteVersion() int64
	GetAllTriggers() map[string]string
	SaveAllTriggers(version int64, triggers map[string]string)
	GrabTask(name string) bool
	SaveLastRunTime(name string, lastRun *time.Time)
	GetLastRunTime(name string) *time.Time
	GetAllLastRunTimes() map[string] *time.Time
}

type MemoryTaskStore struct {
	triggers map[string]string
	lastRuns map[string]*time.Time
	version  int64
}

func NewMemoryTaskStore() *MemoryTaskStore {
	return &MemoryTaskStore{
		triggers: make(map[string]string),
		lastRuns: make(map[string]*time.Time),
		version:  0,
	}
}

func (s *MemoryTaskStore) GetRemoteVersion() int64 {
	return s.version
}

func (s *MemoryTaskStore) GetAllTriggers() map[string]string {
	return s.triggers
}

func (s *MemoryTaskStore) SaveAllTriggers(version int64, triggers map[string]string) {
	s.triggers = triggers
	s.version = version
}

func (s *MemoryTaskStore) GrabTask(name string) bool {
	return true
}

func (s *MemoryTaskStore) SaveLastRunTime(name string, lastRun *time.Time) {
	s.lastRuns[name] = lastRun
}

func (s *MemoryTaskStore) GetLastRunTime(name string) *time.Time {
	return s.lastRuns[name]
}

func (s *MemoryTaskStore) GetAllLastRunTimes() map[string]*time.Time {
	return s.lastRuns
}

type RedisStore struct {
	clients []*redis.Client
}

func NewRedisStore(clients ...*redis.Client) *RedisStore {
	return &RedisStore{clients: clients}
}

func (s *RedisStore) execute(consumer func(*redis.Client)) {
	var i = rand.Int31n(int32(len(s.clients)))
	var client = s.clients[i]
	consumer(client)
}

type RedisTaskStore struct {
	redis   *RedisStore
	group   string
	lockAge int
}

func NewRedisTaskStore(redis *RedisStore, group string, lockAge int) *RedisTaskStore {
	return &RedisTaskStore{redis, group, lockAge}
}

func (s *RedisTaskStore) GrabTask(name string) bool {
	r := false
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("task_lock", name)
		var cmd = redis.SetNX(key, "true", time.Second*time.Duration(s.lockAge))
		r = cmd.Val()
	})
	return r
}

func (s *RedisTaskStore) keyFor(args ...interface{}) string {
	var params = make([]string, len(args)+1)
	params[0] = s.group
	for i := 0; i < len(args); i++ {
		params[i+1] = fmt.Sprintf("%s", args[i])
	}
	return strings.Join(params, "_")
}

func (s *RedisTaskStore) GetRemoteVersion() int64 {
	var r int64 = 0
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("version")
		var cmd = redis.IncrBy(key, 0)
		r = cmd.Val()
	})
	return r
}

func (s *RedisTaskStore) GetAllTriggers() map[string]string {
	r := make(map[string]string)
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("triggers")
		var cmd = redis.HGetAll(key)
		r = cmd.Val()
	})
	return r
}

func (s *RedisTaskStore) SaveAllTriggers(version int64, triggers map[string]string) {
	var triggersGeneric = make(map[string]interface{})
	for key, value := range triggers {
		triggersGeneric[key] = value
	}
	s.redis.execute(func(redis *redis.Client) {
		var triggersKey = s.keyFor("triggers")
		var lastRunKey = s.keyFor("lastruns")
		var versionKey = s.keyFor("version")
		redis.HMSet(triggersKey, triggersGeneric)
		for _, name := range redis.HKeys(triggersKey).Val() {
			if triggersGeneric[name] == nil {
				redis.HDel(triggersKey, name)
				redis.HDel(lastRunKey, name)
			}
		}
		redis.Set(versionKey, version, 0)
	})
}

func (s *RedisTaskStore) SaveLastRunTime(name string, lastRun *time.Time) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		var raw = lastRun.Format(LayoutISO)
		redis.HSet(key, name, raw)
	})
}

func (s *RedisTaskStore) GetLastRunTime(name string) (r *time.Time) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		var raw = redis.HGet(key, name).Val()
		if raw != "" {
			t, _ := time.Parse(LayoutISO, raw)
			r = &t
		}
	})
	return r
}

func (s *RedisTaskStore) GetAllLastRunTimes() map[string]*time.Time {
	var r = map[string]*time.Time{}
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		for name, raw := range redis.HGetAll(key).Val() {
			t, _ := time.Parse(LayoutISO, raw)
			r[name] = &t
		}
	})
	return r
}