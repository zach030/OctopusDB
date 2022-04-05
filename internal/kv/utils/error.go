package utils

import "errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrEmptyKey         = errors.New("Key cannot be empty")
	ErrTruncate         = errors.New("Do truncate")
	ErrKeyNotExist      = errors.New("key not exist")
	ErrKeyNotFound      = errors.New("Key not found")
	ErrStop             = errors.New("Stop")
	ErrDeleteVlogFile   = errors.New("Delete vlog file")
	// compact
	ErrFillTables = errors.New("Unable to fill tables")
)
