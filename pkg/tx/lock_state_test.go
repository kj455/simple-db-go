package tx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnlocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{LOCK_STATE_UNLOCKED, true},
		{LOCK_STATE_X_LOCKED, false},
		{1, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("state: %d", tt.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.state.Unlocked(), tt.expected)
		})
	}
}

func TestIsXLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{LOCK_STATE_UNLOCKED, false},
		{LOCK_STATE_X_LOCKED, true},
		{1, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("state: %d", tt.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.state.IsXLocked(), tt.expected)
		})
	}
}

func TestIsSLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{LOCK_STATE_UNLOCKED, false},
		{LOCK_STATE_X_LOCKED, false},
		{1, true},
		{2, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("state: %d", tt.state), func(t *testing.T) {
			assert.Equal(t, tt.state.IsSLocked(), tt.expected)
		})
	}
}

func TestIsMultipleSLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{LOCK_STATE_UNLOCKED, false},
		{LOCK_STATE_X_LOCKED, false},
		{1, false},
		{2, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("state: %d", tt.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.state.IsMultipleSLocked(), tt.expected)
		})
	}
}

func TestNext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state     lockState
		expected  lockState
		expectErr bool
	}{
		{LOCK_STATE_UNLOCKED, 1, false},
		{1, 2, false},
		{LOCK_STATE_X_LOCKED, LOCK_STATE_X_LOCKED, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("state: %d", test.state), func(t *testing.T) {
			t.Parallel()
			got, err := test.state.Next()
			assert.Equal(t, test.expected, got)
			if test.expectErr {
				assert.NotNil(t, err)
				return
			}
			assert.Nil(t, err)
		})
	}
}
