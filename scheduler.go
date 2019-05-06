package taskino

import (
	"io/ioutil"
	"log"
	"time"
)

type taskGrabber func(task *Task) bool

type SchedulerListener interface {
	OnComplete(*TaskContext)
	OnStartup()
	OnStop()
	OnReload()
}

type DistributedScheduler struct {
	Store             TaskStore
	Version           int64
	AllTasks          map[string]*Task
	Triggers          map[string]Trigger
	executor          *Executor
	reloadingTriggers map[string]Trigger
	listeners         []SchedulerListener
	stop              chan bool
	logger            *log.Logger
}

func NewDistributedScheduler(store TaskStore, logger *log.Logger) *DistributedScheduler {
	var scheduler = &DistributedScheduler{
		Store:             store,
		AllTasks:          make(map[string]*Task),
		Triggers:          make(map[string]Trigger),
		reloadingTriggers: make(map[string]Trigger),
		listeners:         make([]SchedulerListener, 0),
		stop:              make(chan bool, 1),
	}
	if logger != nil {
		scheduler.logger = logger
	} else {
		scheduler.logger = log.New(ioutil.Discard, "scheduler", 0)
	}
	scheduler.executor = NewExecutor(scheduler.logger)
	return scheduler
}

func (s *DistributedScheduler) AddListener(listener SchedulerListener) *DistributedScheduler {
	s.listeners = append(s.listeners, listener)
	return s
}

func (s *DistributedScheduler) Register(trigger Trigger, task *Task) *DistributedScheduler {
	if s.Triggers[task.Name] != nil {
		panic("task name duplicated!")
	}
	s.Triggers[task.Name] = trigger
	s.AllTasks[task.Name] = task
	task.callback = func(ctx *TaskContext) {
		func() {
			defer func() {
				if e := recover(); e != nil {
					s.logger.Printf("save task %s lastrun time error %s\n", task.Name, e)
				}
			}()
			var now = time.Now()
			if err := s.Store.SaveLastRunTime(task.Name, &now); err != nil {
				s.logger.Printf("save task %s last run time error %s\n", task.Name, err)
			}
		}()
		for _, listener := range s.listeners {
			func() {
				defer func() {
					if e := recover(); e != nil {
						s.logger.Printf("invoke task %s listener OnComplete error %s\n", task.Name, e)
					}
				}()
				listener.OnComplete(ctx)
			}()
		}
	}
	return s
}

func (s *DistributedScheduler) TriggerTask(name string) {
	var task = s.AllTasks[name]
	if task != nil {
		task.run()
	}
}

func (s *DistributedScheduler) GetLastRunTime(name string) (*time.Time, error) {
	return s.Store.GetLastRunTime(name)
}

func (s *DistributedScheduler) GetAllLastRunTimes() (map[string]*time.Time, error) {
	return s.Store.GetAllLastRunTimes()
}

func (s *DistributedScheduler) SetVersion(version int64) *DistributedScheduler {
	if version < 0 {
		panic("illegal version!")
	}
	s.Version = version
	return s
}

func (s *DistributedScheduler) Start() error {
	if err := s.saveTriggers(); err != nil {
		return err
	}
	s.scheduleTasks()
	go s.scheduleReload()
	for _, listener := range s.listeners {
		func() {
			defer func() {
				if e := recover(); e != nil {
					s.logger.Printf("invoke listener startup error %s\n", e)
				}
			}()
			listener.OnStartup()
		}()
	}
	return nil
}

func (s *DistributedScheduler) WaitForever() {
	<-s.stop
}

func (s *DistributedScheduler) saveTriggers() error {
	var triggersRaw = map[string]string{}
	for name, trigger := range s.Triggers {
		triggersRaw[name] = SerializeTrigger(trigger)
	}
	err := s.Store.SaveAllTriggers(s.Version, triggersRaw)
	if err != nil {
		s.logger.Printf("save task triggers error %s", err)
	}
	return err
}

func (s *DistributedScheduler) scheduleTasks() {
	for name, trigger := range s.Triggers {
		var task = s.AllTasks[name]
		if task == nil {
			continue
		}
		s.logger.Printf("scheduling task %s\n", name)
		trigger.schedule(s.executor, s.grabTask, task)
	}
}

func (s *DistributedScheduler) grabTask(task *Task) bool {
	if task.Concurrent {
		return true
	}
	r, e := s.Store.GrabTask(task.Name)
	if e != nil {
		s.logger.Printf("grab task %s error %s\n", task.Name, e)
	}
	return r
}

func (s *DistributedScheduler) scheduleReload() {
	var ticker = time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if s.reloadIfChanged() {
				s.rescheduleTasks()
			}
			break
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}

func (s *DistributedScheduler) reloadIfChanged() bool {
	defer func() {
		if e := recover(); e != nil {
			s.logger.Printf("reloading task error %s\n", e)
		}
	}()
	remoteVersion, err := s.Store.GetRemoteVersion()
	if err != nil {
		s.logger.Printf("get remote version error %s\n", err)
		return false
	}
	if remoteVersion > s.Version {
		s.Version = remoteVersion
		s.reload()
		return true
	}
	return false
}

func (s *DistributedScheduler) reload() {
	raws, err := s.Store.GetAllTriggers()
	if err != nil {
		log.Printf("load triggers error %s\n", err)
		return
	}
	var reloadings = map[string]Trigger{}
	for name, raw := range raws {
		if s.AllTasks[name] != nil {
			var trigger = ParseTrigger(raw)
			var oldTrigger = s.Triggers[name]
			if oldTrigger == nil || !oldTrigger.equals(trigger) {
				reloadings[name] = trigger
			}
		}
	}
	for name := range s.Triggers {
		if raws[name] == "" {
			reloadings[name] = nil
		}
	}
	s.reloadingTriggers = reloadings
}

func (s *DistributedScheduler) rescheduleTasks() {
	for name, trigger := range s.reloadingTriggers {
		var task = s.AllTasks[name]
		if trigger == nil {
			s.logger.Printf("cancelling task %s\n", name)
			s.Triggers[name].cancel()
			delete(s.Triggers, name)
		} else {
			var oldTrigger = s.Triggers[name]
			if oldTrigger != nil {
				s.logger.Printf("cancelling task %s\n", name)
				oldTrigger.cancel()
			}
			s.Triggers[name] = trigger
			s.logger.Printf("scheduling task %s\n", name)
			trigger.schedule(s.executor, s.grabTask, task)
		}
	}
	s.reloadingTriggers = map[string]Trigger{}
	for _, listener := range s.listeners {
		func() {
			defer func() {
				if e := recover(); e != nil {
					s.logger.Printf("invoke listener reload error %s\n", e)
				}
			}()
			listener.OnReload()
		}()
	}
}

func (s *DistributedScheduler) cancelAllTasks() {
	for name, trigger := range s.Triggers {
		s.logger.Printf("cancelling task %s\n", name)
		trigger.cancel()
	}
	s.Triggers = map[string]Trigger{}
}

func (s *DistributedScheduler) Stop() {
	close(s.stop)
	s.cancelAllTasks()
	s.executor.shutdown()
	for _, listener := range s.listeners {
		func() {
			defer func() {
				if e := recover(); e != nil {
					s.logger.Printf("invoke listener OnStop error %s\n", e)
				}
			}()
			listener.OnStop()
		}()
	}
}
