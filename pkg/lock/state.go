package lock

import "fmt"

type lockState int64

const (
	X_LOCKED lockState = -1
	UNLOCKED lockState = 0
)

func (ls lockState) Unlocked() bool {
	return ls == UNLOCKED
}

func (ls lockState) IsXLocked() bool {
	return ls == X_LOCKED
}

func (ls lockState) IsSLocked() bool {
	return ls > UNLOCKED
}

func (ls lockState) IsMultipleSLocked() bool {
	return ls > UNLOCKED+1
}

func (ls lockState) Next() (lockState, error) {
	if ls == X_LOCKED {
		return X_LOCKED, fmt.Errorf("lock: cannot get next lock state for X_LOCKED")
	}
	return ls + 1, nil
}

func (ls lockState) Prev() (lockState, error) {
	if ls == UNLOCKED {
		return UNLOCKED, fmt.Errorf("lock: cannot get previous lock state for UNLOCKED")
	}
	return ls - 1, nil
}
