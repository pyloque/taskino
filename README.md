## taskino
Micro Distributed Task Scheduler using Redis

### Install

```
go get -u github.com/pyloque/taskino
```

### Example

```go
// 构造 Redis 连接
c:= redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})
store := taskino.NewRedisTaskStore(taskino.NewRedisStore(c), "sample", 5)
// 日志
logger := log.New(os.Stdout, "taskino", 0)
// 创建调度器
scheduler := taskino.NewDistributedScheduler(store, logger)

// hello 循环任务
hello := taskino.NewTask("hello", false, func() {
	fmt.Println("hello world")
})
scheduler.Register(taskino.PeriodOfDelay(1, 5), hello)

// stopper 停止任务（30s后停止调度器）
stopper := taskino.NewTask("stopper", true, func() {
    scheduler.Stop()
})
scheduler.Register(taskino.OnceOfDelay(30), stopper)

// 设置任务全局版本号
scheduler.SetVersion(1)
// 开启调度
scheduler.Start()
// 等待退出
scheduler.WaitForever()
```

### 解决单点故障
多进程调度，挂掉一个其它进程可以继续调度

### 分布式任务锁
多进程同时调度，只有一个进程可以夺取任务执行权，这里使用 Redis 分布式锁来控制并发冲突
如果 `task.Concurrent=true` 那么多进程可以并行运行

### 任务重加载
使用全局版本号来监听任务变更，用来刷新任务调度时间（代码升级）
当任务有变更时，版本号发生变动，老代码进程会自动从 Redis 中同步新的任务调度时间
对有变动的任务进行重新调度

### 事件回调
监听任务运行时间，观察任务运行状态

```go
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

...
scheduler.SetVersion(2)
scheduler.AddListener(NewSampleListener(scheduler))
scheduler.Start()
scheduler.WaitForever()
```

### 支持三种任务类型

1. 单次任务(OnceTrigger)：固定时间运行一次即结束
2. 循环任务(PeriodTrigger)：从起始时间开始间隔循环到结束时间
3. CRON任务(CronTrigger)：CRON表达式控制任务运行时间（最低精度 1 分钟）

```go
taskino.OnceOf(startTime time.Time) *OnceTrigger
taskino.PeriodOf(startTime time.Time, endTime time.Time, period int) *PeriodTrigger
taskino.CronOf(expr string) *CronTrigger
```

### 任务手动运行

```go
scheduler.TriggerTask(name string)
```

### 注意点

```
1. 如果在任务调度点发生网络抖动，Redis 读写出错，可能会引发任务的miss
2. 多机器部署时无比保持时间同步，如果时间差异过大（5s），会导致任务重复执行
```

### Example

[入门实例](github.com/pyloque/taskino/cmd/taskino/main.go)