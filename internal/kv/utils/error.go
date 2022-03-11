package utils

import "errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate    = errors.New("Do truncate")
	ErrKeyNotExist = errors.New("key not exist")

	ErrStop = errors.New("Stop")

	// compact
	ErrFillTables = errors.New("Unable to fill tables")
)
