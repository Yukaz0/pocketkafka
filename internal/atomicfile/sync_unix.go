//go:build !windows

package atomicfile

import (
	"fmt"
	"os"
)

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("atomicfile: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("atomicfile: sync dir: %w", err)
	}
	return nil
}
