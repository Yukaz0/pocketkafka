//go:build windows

package storage

import (
	"syscall"
	"unsafe"
)

var (
	kernel32            = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceExW = kernel32.NewProc("GetDiskFreeSpaceExW")
)

func diskUsagePct(dir string) float64 {
	if dir == "" {
		return 0
	}
	dirPtr, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return 0
	}
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	r1, _, _ := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(dirPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)
	if r1 == 0 || totalNumberOfBytes == 0 {
		return 0
	}
	used := float64(totalNumberOfBytes - totalNumberOfFreeBytes)
	return used / float64(totalNumberOfBytes) * 100
}

func syncDir(dir string) error {
	return nil
}
