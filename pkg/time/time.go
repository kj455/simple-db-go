package time

import (
	ttime "time"
)

type timeImpl struct{}

func NewTime() Time {
	return &timeImpl{}
}

func (t *timeImpl) Now() ttime.Time {
	return ttime.Now()
}

func (t *timeImpl) Sleep(d ttime.Duration) {
	ttime.Sleep(d)
}

func (t *timeImpl) Since(tim ttime.Time) ttime.Duration {
	return ttime.Since(tim)
}
