package taskino

import "time"

type Runner func()

type TaskCallback func(*TaskContext)

type Task struct {
	Name       string
	Concurrent bool
	runner     Runner
	callback   TaskCallback
}

func NewTask(name string, concurrent bool, runner Runner) *Task {
	return &Task{
		Name:       name,
		Concurrent: concurrent,
		runner:     runner,
	}
}

func TaskOf(name string, runner Runner) *Task {
	return NewTask(name, false, runner)
}

func ConcurrentTask(name string, runner Runner) *Task {
	return NewTask(name, true, runner)
}

func (t *Task) run() {
	startTime := time.Now()
	err := func() (e error) {
		defer func() {
			if r := recover(); r != nil {
				e = r.(error)
			}
		}()
		t.runner()
		return e
	}()
	endTime := time.Now()
	cost := endTime.Sub(startTime).Nanoseconds() / 1000000
	ctx := NewTaskContext(t, cost, err == nil, err)
	if t.callback != nil {
		t.callback(ctx)
	}
}

type TaskContext struct {
	Task         *Task
	CostInMillis int64
	Ok           bool
	Err          error
}

func NewTaskContext(task *Task, costInMillis int64, ok bool, e error) *TaskContext {
	return &TaskContext{
		Task:         task,
		CostInMillis: costInMillis,
		Ok:           ok,
		Err:          e,
	}
}
