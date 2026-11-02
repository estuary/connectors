//go:build !linux

package connector

import "syscall"

// Pdeathsig is Linux-only; elsewhere the sidecar can outlive a killed connector.
func sidecarSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
