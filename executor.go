package taskino

import (
	"log"
	"sync"
)

type Executor struct {
	wg     *sync.WaitGroup
	logger *log.Logger
	stop   bool
}

func NewExecutor(logger *log.Logger) *Executor {
	return &Executor{
		wg:     &sync.WaitGroup{},
		logger: logger,
	}
}

func (e *Executor) submit(runner func()) {
	if e.stop {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				e.logger.Printf("executor run error %s\n", r)
			}
		}()
		runner()
	}()
}

func (e *Executor) shutdown() {
	e.stop = true
	e.wg.Wait()
}
