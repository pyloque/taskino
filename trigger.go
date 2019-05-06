package taskino

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TriggerType string

const (
	ONCE   TriggerType = "ONCE"
	PERIOD TriggerType = "PERIOD"
	CRON   TriggerType = "CRON"
)

const LayoutISO = "2006-01-02T15:04:05-0700"

type Trigger interface {
	kind() TriggerType
	parse(s string)
	serialize() string
	equals(other Trigger) bool
	cancel()
	schedule(executor *Executor, grabber taskGrabber, task *Task) bool
}

type OnceTrigger struct {
	startTime time.Time
	timer     *time.Timer
	stop      chan bool
}

func NewOnceTrigger() *OnceTrigger {
	return &OnceTrigger{}
}

func (t *OnceTrigger) kind() TriggerType {
	return ONCE
}

func (t *OnceTrigger) serialize() string {
	return t.startTime.Format(LayoutISO)
}

func (t *OnceTrigger) parse(s string) {
	startTime, err := time.Parse(LayoutISO, s)
	// should not happen
	if err != nil {
		panic(err)
	}
	t.startTime = startTime
}

func (t *OnceTrigger) equals(other Trigger) bool {
	oo, ok := other.(*OnceTrigger)
	if !ok {
		return false
	}
	return t.startTime.Equal(oo.startTime)
}

func (t *OnceTrigger) cancel() {
	if t.stop != nil {
		close(t.stop)
	}
}

func (t *OnceTrigger) schedule(executor *Executor, grabber taskGrabber, task *Task) bool {
	var gap = t.startTime.Sub(time.Now())
	if gap < 0 {
		return false
	}
	t.stop = make(chan bool)
	t.timer = time.NewTimer(gap)
	go func() {
		select {
		case <-t.timer.C:
			t.timer.Stop()
			executor.submit(func() {
				if grabber(task) {
					task.run()
				}
			})
			break
		case <-t.stop:
			t.timer.Stop()
			break
		}
	}()
	return true
}

func OnceOf(startTime time.Time) *OnceTrigger {
	var trigger = NewOnceTrigger()
	trigger.startTime = startTime
	return trigger
}

func OnceOfDelay(seconds int) *OnceTrigger {
	var startTime = time.Now().Add(time.Duration(seconds) * time.Second)
	return OnceOf(startTime)
}

type PeriodTrigger struct {
	startTime    time.Time
	endTime      time.Time
	period       int
	delayTimer   *time.Timer
	periodTicker *time.Ticker
	stop         chan bool
}

func NewPeriodTrigger() *PeriodTrigger {
	return &PeriodTrigger{}
}

func (t *PeriodTrigger) kind() TriggerType {
	return PERIOD
}

func (t *PeriodTrigger) serialize() string {
	var starts = t.startTime.Format(LayoutISO)
	var ends = t.endTime.Format(LayoutISO)
	return fmt.Sprintf("%s|%s|%d", starts, ends, t.period)
}

func (t *PeriodTrigger) parse(s string) {
	var parts = strings.Split(s, "|")
	startTime, err := time.Parse(LayoutISO, parts[0])
	// should not happen
	if err != nil {
		panic(err)
	}
	endTime, err := time.Parse(LayoutISO, parts[1])
	// should not happen
	if err != nil {
		panic(err)
	}
	t.startTime = startTime
	t.endTime = endTime
	t.period, err = strconv.Atoi(parts[2])
	// should not happend
	if err != nil {
		panic(err)
	}
}

func (t *PeriodTrigger) equals(other Trigger) bool {
	oo, ok := other.(*PeriodTrigger)
	if !ok {
		return false
	}
	if !t.startTime.Equal(oo.startTime) {
		return false
	}
	if !t.endTime.Equal(oo.endTime) {
		return false
	}
	return t.period == oo.period
}

func (t *PeriodTrigger) cancel() {
	if t.stop != nil {
		close(t.stop)
	}
}

func (t *PeriodTrigger) schedule(executor *Executor, grabber taskGrabber, task *Task) bool {
	var now = time.Now()
	if t.endTime.Before(now) {
		return false
	}
	var delay time.Duration
	if t.startTime.After(now) {
		delay = t.startTime.Sub(now)
	} else {
		elapsed := now.Sub(t.startTime).Nanoseconds() % int64(t.period*1000000000)
		if elapsed > 0 {
			delay = time.Duration(t.period)*time.Second - time.Duration(elapsed)*time.Nanosecond
		}
	}
	t.stop = make(chan bool)
	if delay > 0 {
		t.delayTimer = time.NewTimer(delay)
		go t.delayTask(executor, grabber, task)
	} else {
		go t.tickTask(executor, grabber, task)
	}
	return true
}

func (t *PeriodTrigger) delayTask(executor *Executor, grabber taskGrabber, task *Task) {
	stop := false
	select {
	case <-t.delayTimer.C:
		break
	case <-t.stop:
		stop = true
		break
	}
	t.delayTimer.Stop()
	if stop {
		return
	}
	t.tickTask(executor, grabber, task)
}

func (t *PeriodTrigger) tickTask(executor *Executor, grabber taskGrabber, task *Task) {
	t.periodTicker = time.NewTicker(time.Duration(t.period) * time.Second)
	for {
		if time.Now().Sub(t.endTime) >= 0 {
			return
		}
		executor.submit(func() {
			if grabber(task) {
				task.run()
			}
		})
		select {
		case <-t.periodTicker.C:
			break
		case <-t.stop:
			t.periodTicker.Stop()
			return
		}
	}
}

func PeriodOf(startTime time.Time, endTime time.Time, period int) *PeriodTrigger {
	var trigger = NewPeriodTrigger()
	trigger.startTime = startTime
	trigger.endTime = endTime
	trigger.period = period
	return trigger
}

func PeriodOfStart(startTime time.Time, period int) *PeriodTrigger {
	maxTime := time.Date(2048, time.April, 0, 0, 0, 0, 0, time.UTC)
	return PeriodOf(startTime, maxTime, period)
}

func PeriodOfDelay(delay int, period int) *PeriodTrigger {
	var startTime = time.Now().Add(time.Duration(delay) * time.Second)
	return PeriodOfStart(startTime, period)
}

type CronTrigger struct {
	expr         string
	delayTimer   *time.Timer
	periodTicker *time.Ticker
	stop         chan bool
}

func NewCronTrigger() *CronTrigger {
	return &CronTrigger{}
}

func (t *CronTrigger) kind() TriggerType {
	return CRON
}

func (t *CronTrigger) serialize() string {
	return t.expr
}

func (t *CronTrigger) parse(s string) {
	t.expr = s
}

func (t *CronTrigger) equals(other Trigger) bool {
	oo, ok := other.(*CronTrigger)
	if !ok {
		return false
	}
	return t.expr == oo.expr
}

func (t *CronTrigger) cancel() {
	if t.stop != nil {
		close(t.stop)
	}
}

func (t *CronTrigger) schedule(executor *Executor, grabber taskGrabber, task *Task) bool {
	now := time.Now()
	snow := now.Add(-time.Duration(now.Nanosecond()))
	if snow.Second() != 0 {
		snow.Add(-time.Duration(snow.Second()) * time.Second)
		snow.Add(time.Minute)
	}
	delay := snow.Sub(now).Nanoseconds()
	if delay < 0 {
		delay = 0
	}
	if delay > 0 {
		t.delayTimer = time.NewTimer(time.Duration(delay) * time.Nanosecond)
		go t.delayTask(executor, grabber, task)
	} else {
		go t.tickTask(executor, grabber, task)
	}
	return true
}

func (t *CronTrigger) delayTask(executor *Executor, grabber taskGrabber, task *Task) {
	select {
	case <-t.delayTimer.C:
		t.delayTimer.Stop()
		t.tickTask(executor, grabber, task)
		break
	case <-t.stop:
		t.delayTimer.Stop()
		break
	}
}

func (t *CronTrigger) tickTask(executor *Executor, grabber taskGrabber, task *Task) {
	t.periodTicker = time.NewTicker(time.Minute)
	p, _ := NewCronPattern(t.expr)
	for {
		select {
		case <-t.periodTicker.C:
			if p.Matches(time.Now().UnixNano() / 1000000) {
				executor.submit(func() {
					if grabber(task) {
						task.run()
					}
				})
			}
			break
		case <-t.stop:
			t.periodTicker.Stop()
			return
		}
	}
}

func CronOf(expr string) *CronTrigger {
	var trigger = NewCronTrigger()
	trigger.expr = expr
	return trigger
}

func CronOfMinutes(minutes int) *CronTrigger {
	return CronOf(fmt.Sprintf("*/%d * * * *", minutes))
}

func CronOfHours(hours int, minuteOffset int) *CronTrigger {
	return CronOf(fmt.Sprintf("%d */%d * * *", minuteOffset, hours))
}

func CronOfDays(days int, hourOffset int, minuteOffset int) *CronTrigger {
	return CronOf(fmt.Sprintf("%d %d */%d * *", minuteOffset, hourOffset, days))
}

func ParseTrigger(s string) Trigger {
	var parts = strings.SplitN(s, "@", 2)
	var typ = TriggerType(parts[0])
	var trigger Trigger = nil
	switch typ {
	case ONCE:
		trigger = NewOnceTrigger()
		break
	case PERIOD:
		trigger = NewPeriodTrigger()
		break
	case CRON:
		trigger = NewCronTrigger()
		break
	default:
		panic("illegal trigger string")
	}
	trigger.parse(parts[1])
	return trigger
}

func SerializeTrigger(trigger Trigger) string {
	return fmt.Sprintf("%s@%s", trigger.kind(), trigger.serialize())
}
