package database

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ErrNotFound", ErrNotFound, true},
		{"ErrPreconditionFailed", ErrPreconditionFailed, false},
		{"ErrAlreadyExists", ErrAlreadyExists, false},
		{"regular error", errors.New("not found"), false},
		{"nil", nil, false},
		{"wrapped ErrNotFound", fmt.Errorf("wrap: %w", ErrNotFound), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotFoundError(tt.err); got != tt.want {
				t.Errorf("IsNotFoundError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPreconditionFailedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ErrPreconditionFailed", ErrPreconditionFailed, true},
		{"ErrNotFound", ErrNotFound, false},
		{"regular error", errors.New("precondition failed"), false},
		{"nil", nil, false},
		{"wrapped ErrPreconditionFailed", fmt.Errorf("wrap: %w", ErrPreconditionFailed), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPreconditionFailedError(tt.err); got != tt.want {
				t.Errorf("IsPreconditionFailedError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewNotFoundError_IsClassified(t *testing.T) {
	err := NewNotFoundError()
	if !IsNotFoundError(err) {
		t.Error("NewNotFoundError() not classified as NotFound")
	}
	if IsPreconditionFailedError(err) {
		t.Error("NewNotFoundError() classified as PreconditionFailed")
	}
}

func TestNewPreconditionFailedError_IsClassified(t *testing.T) {
	err := NewPreconditionFailedError()
	if !IsPreconditionFailedError(err) {
		t.Error("NewPreconditionFailedError() not classified as PreconditionFailed")
	}
	if IsNotFoundError(err) {
		t.Error("NewPreconditionFailedError() classified as NotFound")
	}
}

func TestNewAlreadyExistsError_IsClassified(t *testing.T) {
	err := NewAlreadyExistsError()
	if !IsAlreadyExistsError(err) {
		t.Error("NewAlreadyExistsError() not classified as AlreadyExists")
	}
	if IsNotFoundError(err) {
		t.Error("NewAlreadyExistsError() classified as NotFound")
	}
}
