package model

import "errors"

var (
	ErrNotFound          = errors.New("backupmesh: not found")
	ErrConflict          = errors.New("backupmesh: state conflict")
	ErrLeaseHeld         = errors.New("backupmesh: lease held")
	ErrLeaseFenced       = errors.New("backupmesh: lease fenced")
	ErrJournalClosed     = errors.New("backupmesh: journal closed")
	ErrInvalidTransition = errors.New("backupmesh: invalid transition")
	ErrCapacity          = errors.New("backupmesh: capacity exceeded")
	ErrCancelled         = errors.New("backupmesh: operation cancelled")
)
