//go:build windows

package main

import "os/exec"

func setTaskProcessGroup(cmd *exec.Cmd) {
	// No-op on Windows for now.
}

func killTaskProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
