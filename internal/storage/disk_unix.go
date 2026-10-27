//go:build !windows

package storage

import "syscall"

func diskUsagePct(dir string) float64 {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0
	}
	if fs.Blocks == 0 {
		return 0
	}
	used := float64(fs.Blocks - fs.Bfree)
	return used / float64(fs.Blocks) * 100
}
