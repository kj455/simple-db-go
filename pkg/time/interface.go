//go:generate mkdir -p mock
//go:generate mockgen -source=./interface.go -package=mock -destination=./mock/interface.go
package time

import ttime "time"

type Time interface {
	Now() ttime.Time
	Sleep(d ttime.Duration)
	Since(t ttime.Time) ttime.Duration
}
