package database

import dyndbu "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-db/dynamodb"

// Sentinel errors — thin wrappers delegating to the central hyperfleet-db/dynamodb lib.
// Callers in this repo use these package-level names unchanged.
var (
	ErrNotFound           = dyndbu.ErrNotFound
	ErrAlreadyExists      = dyndbu.ErrAlreadyExists
	ErrPreconditionFailed = dyndbu.ErrPreconditionFailed
)

func IsNotFoundError(err error) bool           { return dyndbu.IsNotFoundError(err) }
func IsAlreadyExistsError(err error) bool      { return dyndbu.IsAlreadyExistsError(err) }
func IsPreconditionFailedError(err error) bool { return dyndbu.IsPreconditionFailedError(err) }

func NewNotFoundError() error           { return dyndbu.ErrNotFound }
func NewAlreadyExistsError() error      { return dyndbu.ErrAlreadyExists }
func NewPreconditionFailedError() error { return dyndbu.ErrPreconditionFailed }
