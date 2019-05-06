package taskino

import (
	"fmt"
	"github.com/go-redis/redis"
	"math/rand"
	"strings"
	"time"
)

type TaskStore interface {
	GetRemoteVersion() (int64, error)
	GetAllTriggers() (map[string]string, error)
	SaveAllTriggers(version int64, triggers map[string]string) error
	GrabTask(name string) (bool, error)
	SaveLastRunTime(name string, lastRun *time.Time) error
	GetLastRunTime(name string) (*time.Time, error)
	GetAllLastRunTimes() (map[string]*time.Time, error)
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

func (s *MemoryTaskStore) GetRemoteVersion() (int64, error) {
	return s.version, nil
}

func (s *MemoryTaskStore) GetAllTriggers() (map[string]string, error) {
	return s.triggers, nil
}

func (s *MemoryTaskStore) SaveAllTriggers(version int64, triggers map[string]string) error {
	s.triggers = triggers
	s.version = version
	return nil
}

func (s *MemoryTaskStore) GrabTask(name string) (bool, error) {
	return true, nil
}

func (s *MemoryTaskStore) SaveLastRunTime(name string, lastRun *time.Time) error {
	s.lastRuns[name] = lastRun
	return nil
}

func (s *MemoryTaskStore) GetLastRunTime(name string) (*time.Time, error) {
	return s.lastRuns[name], nil
}

func (s *MemoryTaskStore) GetAllLastRunTimes() (map[string]*time.Time, error) {
	return s.lastRuns, nil
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

func (s *RedisTaskStore) GrabTask(name string) (r bool, e error) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("task_lock", name)
		var cmd = redis.SetNX(key, "true", time.Second*time.Duration(s.lockAge))
		if cmd.Err() != nil {
			e = cmd.Err()
			return
		}
		r = cmd.Val()
	})
	return
}

func (s *RedisTaskStore) keyFor(args ...interface{}) string {
	var params = make([]string, len(args)+1)
	params[0] = s.group
	for i := 0; i < len(args); i++ {
		params[i+1] = fmt.Sprintf("%s", args[i])
	}
	return strings.Join(params, "_")
}

func (s *RedisTaskStore) GetRemoteVersion() (r int64, e error) {
	r = 0
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("version")
		var cmd = redis.IncrBy(key, 0)
		if cmd.Err() != nil {
			e = cmd.Err()
			return
		}
		r = cmd.Val()
	})
	return
}

func (s *RedisTaskStore) GetAllTriggers() (r map[string]string, e error) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("triggers")
		var cmd = redis.HGetAll(key)
		if cmd.Err() != nil {
			e = cmd.Err()
			return
		}
		r = cmd.Val()
	})
	return
}

func (s *RedisTaskStore) SaveAllTriggers(version int64, triggers map[string]string) (r error) {
	var triggersGeneric = make(map[string]interface{})
	for key, value := range triggers {
		triggersGeneric[key] = value
	}
	s.redis.execute(func(redis *redis.Client) {
		var triggersKey = s.keyFor("triggers")
		var lastRunKey = s.keyFor("lastruns")
		var versionKey = s.keyFor("version")
		cmd := redis.HMSet(triggersKey, triggersGeneric)
		if cmd.Err() != nil {
			r = cmd.Err()
			return
		}
		for _, name := range redis.HKeys(triggersKey).Val() {
			if triggersGeneric[name] == nil {
				cmd := redis.HDel(triggersKey, name)
				if cmd.Err() != nil {
					r = cmd.Err()
					return
				}
				cmd = redis.HDel(lastRunKey, name)
				if cmd.Err() != nil {
					r = cmd.Err()
					return
				}
			}
		}
		cmd = redis.Set(versionKey, version, 0)
		r = cmd.Err()
	})
	return
}

func (s *RedisTaskStore) SaveLastRunTime(name string, lastRun *time.Time) (r error) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		var raw = lastRun.Format(LayoutISO)
		cmd := redis.HSet(key, name, raw)
		r = cmd.Err()
	})
	return
}

func (s *RedisTaskStore) GetLastRunTime(name string) (r *time.Time, e error) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		var cmd = redis.HGet(key, name)
		if cmd.Err() != nil {
			e = cmd.Err()
			return
		}
		var raw = cmd.Val()
		if raw != "" {
			t, err := time.Parse(LayoutISO, raw)
			if err != nil {
				e = err
				return
			}
			r = &t
		}
	})
	return
}

func (s *RedisTaskStore) GetAllLastRunTimes() (r map[string]*time.Time, e error) {
	s.redis.execute(func(redis *redis.Client) {
		var key = s.keyFor("lastruns")
		var cmd = redis.HGetAll(key)
		if cmd.Err() != nil {
			e = cmd.Err()
			return
		}
		r = map[string]*time.Time{}
		for name, raw := range cmd.Val() {
			t, err := time.Parse(LayoutISO, raw)
			if err != nil {
				e = err
				r = nil
				return
			}
			r[name] = &t
		}
	})
	return
}
