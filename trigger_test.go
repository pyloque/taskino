package taskino

import (
	"testing"
)

func TestTriggerSerialize(t *testing.T) {
	var s1 = OnceOfDelay(5)
	var t1 = ParseTrigger(SerializeTrigger(s1))
	if !s1.equals(t1) {
		t.Error("once trigger serialize error")
		return
	}
	var s2 = PeriodOfDelay(5, 10)
	var t2 = ParseTrigger(SerializeTrigger(s2))
	if !s2.equals(t2) {
		t.Error("period trigger serialize error")
	}
	var s3 = CronOfDays(2, 12, 30)
	var t3 = ParseTrigger(SerializeTrigger(s3))
	if !s3.equals(t3) {
		t.Error("cron trigger serialize error")
	}
}
