package tx

import "fmt"

type lockState int64

const (
	LOCK_STATE_X_LOCKED lockState = -1
	LOCK_STATE_UNLOCKED lockState = 0
)

func (ls lockState) Unlocked() bool {
	return ls == LOCK_STATE_UNLOCKED
}

func (ls lockState) IsXLocked() bool {
	return ls == LOCK_STATE_X_LOCKED
}

func (ls lockState) IsSLocked() bool {
	return ls > LOCK_STATE_UNLOCKED
}

func (ls lockState) IsMultipleSLocked() bool {
	return ls > LOCK_STATE_UNLOCKED+1
}

func (ls lockState) Next() (lockState, error) {
	if ls == LOCK_STATE_X_LOCKED {
		return LOCK_STATE_X_LOCKED, fmt.Errorf("lock: cannot get next lock state for X_LOCKED")
	}
	return ls + 1, nil
}

func (ls lockState) Prev() (lockState, error) {
	if ls == LOCK_STATE_UNLOCKED {
		return LOCK_STATE_UNLOCKED, fmt.Errorf("lock: cannot get previous lock state for UNLOCKED")
	}
	return ls - 1, nil
}
