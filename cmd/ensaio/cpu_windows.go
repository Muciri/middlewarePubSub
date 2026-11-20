//go:build windows

package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func cpuSeconds() float64 {
	cmd := exec.Command("powershell.exe", "-NoProfile", "-Command", "[Threading.Thread]::CurrentThread.CurrentCulture=[Globalization.CultureInfo]::InvariantCulture; (Get-Process -Id "+strconv.Itoa(os.Getpid())+").TotalProcessorTime.TotalSeconds")
	b, err := cmd.Output()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	return v
}
