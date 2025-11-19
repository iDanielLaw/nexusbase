package sys

import "errors"

// ErrPreallocNotSupported is returned when the underlying file or filesystem
// does not support preallocation operations. Callers can treat this as a
// non-fatal, informational condition and avoid noisy warnings.
var ErrPreallocNotSupported = errors.New("preallocation not supported")
