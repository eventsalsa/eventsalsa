package acl

import "errors"

// ErrEmptyCommand indicates a nil Protobuf command payload was passed across the ACL boundary.
var ErrEmptyCommand = errors.New("command payload must not be nil")
