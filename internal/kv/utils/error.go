package utils

import "errors"

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("Do truncate")

	ErrStop = errors.New("Stop")
)
