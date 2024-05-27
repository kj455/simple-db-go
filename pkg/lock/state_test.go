package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnlocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{UNLOCKED, true},
		{X_LOCKED, false},
		{1, false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.state.Unlocked(), tt.expected)
	}
}

func TestIsXLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{UNLOCKED, false},
		{X_LOCKED, true},
		{1, false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.state.IsXLocked(), tt.expected)
	}
}

func TestIsSLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{UNLOCKED, false},
		{X_LOCKED, false},
		{1, true},
		{2, true},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.state.IsSLocked(), tt.expected)
	}
}

func TestIsMultipleSLocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state    lockState
		expected bool
	}{
		{UNLOCKED, false},
		{X_LOCKED, false},
		{1, false},
		{2, true},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.state.IsMultipleSLocked(), tt.expected)
	}
}

func TestNext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state     lockState
		expected  lockState
		expectErr bool
	}{
		{UNLOCKED, 1, false},
		{1, 2, false},
		{X_LOCKED, X_LOCKED, true},
	}

	for _, test := range tests {
		got, err := test.state.Next()
		assert.Equal(t, test.expected, got)
		if test.expectErr {
			assert.NotNil(t, err)
			return
		}
		assert.Nil(t, err)
	}
}
