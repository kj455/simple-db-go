package time

import ttime "time"

type Time interface {
	Now() ttime.Time
	Sleep(d ttime.Duration)
	Since(t ttime.Time) ttime.Duration
}
