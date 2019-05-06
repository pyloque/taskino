package taskino

import (
	"github.com/robfig/cron"
	"time"
)

type CronPattern struct {
	s cron.Schedule
}

func NewCronPattern(expr string) (*CronPattern, bool) {
	s, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, false
	}
	return &CronPattern{s}, true
}

func (p *CronPattern) Matches(millis int64) bool {
	var minuteAgoMillis = millis - 60*1000
	var minuteAgoTime = time.Unix(minuteAgoMillis/1000, (minuteAgoMillis%1000)*1000000)
	var minAgoNextTime = p.s.Next(minuteAgoTime)
	var gap = minuteAgoTime.Sub(minAgoNextTime)
	return gap < 60*1000000000
}
