package taskino

import (
	"testing"

	"github.com/go-redis/redis"
)

func TestMemoryTaskStore(t *testing.T) {
	var store = NewMemoryTaskStore()
	var triggers = map[string]Trigger{}
	triggers["once1"] = OnceOfDelay(5)
	triggers["period2"] = PeriodOfDelay(5, 10)
	triggers["cron3"] = CronOfDays(2, 12, 30)
	var triggersRaw = map[string]string{}
	for name, trigger := range triggers {
		triggersRaw[name] = SerializeTrigger(trigger)
	}
	err := store.SaveAllTriggers(1024, triggersRaw)
	if err != nil {
		t.Errorf("memory save all trigger error %s", err)
	}
	version, err := store.GetRemoteVersion()
	if err != nil {
		t.Errorf("memory get remote version error %s", err)
	}
	if version != 1024 {
		t.Errorf("memory get remote version mismatch")
	}
	for _, name := range []string{"once1", "once1", "once1"} {
		r, err := store.GrabTask(name)
		if err != nil {
			t.Errorf("memory grab task error %s", err)
		}
		if !r {
			t.Errorf("memory grab task failed")
		}
	}
}

func TestRedisTaskStore(t *testing.T) {
	var client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	var store = NewRedisTaskStore(NewRedisStore(client), "test", 5)
	var triggers = map[string]Trigger{}
	triggers["once1"] = OnceOfDelay(5)
	triggers["period2"] = PeriodOfDelay(5, 10)
	triggers["cron3"] = CronOfDays(2, 12, 30)
	var triggersRaw = map[string]string{}
	for name, trigger := range triggers {
		triggersRaw[name] = SerializeTrigger(trigger)
	}
	err := store.SaveAllTriggers(1024, triggersRaw)
	if err != nil {
		t.Errorf("redis save all trigger error %s", err)
	}
	version, err := store.GetRemoteVersion()
	if err != nil {
		t.Errorf("redis get remote version error %s", err)
	}
	if version != 1024 {
		t.Errorf("redis get remote version mismatch")
	}
	r, err := store.GrabTask("once1")
	if err != nil {
		t.Errorf("redis grab task error %s", err)
	}
	if !r {
		t.Errorf("redis grab task failed")
	}
	r, err = store.GrabTask("once1")
	if err != nil {
		t.Errorf("redis grab task error %s", err)
	}
	if r {
		t.Errorf("redis grab task should not be ok here")
	}
}
