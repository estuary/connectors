package network_tunnel

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

func StartServer(ctx context.Context, authorizedKey string, waitForPort uint16) error {
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
		"waitForPort":   waitForPort,
	}).Info("started sshd")
	go func() {
		if err := cmd.Wait(); err != nil {
			logrus.WithField("error", err).Fatal("sshd process failed")
		} else {
			logrus.Info("sshd process exited normally")
		}
	}()

	var startTime = time.Now()
	for waitForPort > 0 {
		var dialer net.Dialer
		var conn, err = dialer.DialContext(ctx, "tcp", fmt.Sprintf("localhost:%d", waitForPort))
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"port":    waitForPort,
				"elapsed": time.Now().Sub(startTime),
				"error":   err,
			}).Info("waiting for reverse port-forward connection (will retry)")
		} else {
			conn.Close() // ignore error
			logrus.WithFields(logrus.Fields{
				"port":    waitForPort,
				"elapsed": time.Now().Sub(startTime),
			}).Info("reverse port-forward has been started")
			break
		}
	}

	return nil
}
