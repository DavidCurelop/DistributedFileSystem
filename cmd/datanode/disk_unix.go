//go:build linux || darwin || freebsd

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// diskUsage reports the total and available bytes of the filesystem holding dir.
// If dir does not exist yet (it is created on the first stored chunk), the
// nearest existing parent is measured instead.
func diskUsage(dir string) (total, available int64, err error) {
	current, err := filepath.Abs(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("resolving %q: %w", dir, err)
	}

	for {
		var stat syscall.Statfs_t
		err = syscall.Statfs(current, &stat)
		if err == nil {
			blockSize := uint64(stat.Bsize)
			return int64(uint64(stat.Blocks) * blockSize), int64(uint64(stat.Bavail) * blockSize), nil
		}
		parent := filepath.Dir(current)
		if !errors.Is(err, os.ErrNotExist) || parent == current {
			return 0, 0, fmt.Errorf("reading disk usage of %q: %w", current, err)
		}
		current = parent
	}
}
