//go:build !(linux || darwin || freebsd)

package main

import "errors"

// diskUsage is not supported on this platform; callers report zero capacity.
func diskUsage(dir string) (total, available int64, err error) {
	return 0, 0, errors.New("disk usage is not supported on this platform")
}
