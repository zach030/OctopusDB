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
	ErrTxnTooBig  = errors.New("Txn is too big to fit into one request")
	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New("Value log GC attempt didn't result in any cleanup")
	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected      = errors.New("Value log GC request rejected")
	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")
)
