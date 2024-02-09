package network_tunnel

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
)

func StartServer(ctx context.Context, authorizedKey string) error {
	if err := os.WriteFile("/root/.ssh/authorized_keys", []byte(authorizedKey), 600); err != nil {
		return fmt.Errorf("writing authorized_keys: %w", err)
	}

	//ctx, cancel := context.WithCancel(ctx)
	var cmd = exec.CommandContext(ctx, "/usr/sbin/sshd")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting sshd: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"pid":           cmd.Process.Pid,
		"authorizedKey": authorizedKey,
	}).Info("started sshd")
	go func() {
		if err := cmd.Wait(); err != nil {
			logrus.WithField("error", err).Fatal("sshd process failed")
		} else {
			logrus.Info("sshd process exited normally")
		}
	}()

	return nil
}
