package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/pyloque/taskino"
	"log"
	"os"
)

type SampleListener struct {
	scheduler *taskino.DistributedScheduler
}

func NewSampleListener(scheduler *taskino.DistributedScheduler) *SampleListener {
	return &SampleListener{scheduler}
}

func (l *SampleListener) OnComplete(ctx *taskino.TaskContext) {
	fmt.Printf("task %s cost %d millis\n", ctx.Task.Name, ctx.CostInMillis)
}

func (l *SampleListener) OnStartup() {
	fmt.Println("scheduler started")
}

func (l *SampleListener) OnStop() {
	fmt.Println("scheduler stopped")
}

func (l *SampleListener) OnReload() {
	fmt.Println("scheduler reloaded")
}

func main() {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	store := taskino.NewRedisTaskStore(taskino.NewRedisStore(c), "sample", 5)
	logger := log.New(os.Stdout, "taskino", 0)
	scheduler := taskino.NewDistributedScheduler(store, logger)
	once1 := taskino.NewTask("once1", false, func() {
		fmt.Println("once1")
	})
	scheduler.Register(taskino.OnceOfDelay(5), once1)
	period2 := taskino.NewTask("period2", false, func() {
		fmt.Println("period2")
	})
	scheduler.Register(taskino.PeriodOfDelay(5, 5), period2)
	cron3 := taskino.NewTask("cron3", false, func() {
		fmt.Println("cron3")
	})
	scheduler.Register(taskino.CronOfMinutes(1), cron3)
	period4 := taskino.NewTask("period4", false, func() {
		fmt.Println("period4")
	})
	scheduler.Register(taskino.PeriodOfDelay(5, 5), period4)
	stopper := taskino.NewTask("stopper", true, func() {
		scheduler.Stop()
	})
	scheduler.Register(taskino.OnceOfDelay(70), stopper)
	scheduler.SetVersion(2)
	scheduler.AddListener(NewSampleListener(scheduler))
	scheduler.Start()
	scheduler.WaitForever()
}
