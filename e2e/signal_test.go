package e2e

import (
	"os/signal"
	"syscall"
)

func signalIgnoreTerm() { signal.Ignore(syscall.SIGTERM) }
