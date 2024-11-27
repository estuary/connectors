package network_tunnel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

type CloudSQLProxyConfig struct {
	InstanceConnectionName string
	Credentials            []byte
	Port                   string
}

type CloudSQLProxy struct {
	Config      *CloudSQLProxyConfig
	ctx         context.Context
	Cmd         *exec.Cmd
	tmpFileName string
	cancel      context.CancelFunc
}

func (c *CloudSQLProxyConfig) CreateTunnel() *CloudSQLProxy {
	var ctx, cancel = context.WithCancel(context.Background())
	return &CloudSQLProxy{
		Config:      c,
		ctx:         ctx,
		cancel:      cancel,
		tmpFileName: "",
		Cmd:         nil,
	}
}

// Start tunnel and wait until READY signal
func (t *CloudSQLProxy) Start() error {
	if t.Cmd != nil {
		return errors.New("This tunnel has already been started.")
	}

	var tmpKeyFile, err = os.CreateTemp("", "flow-cloud-sql-proxy")
	if err != nil {
		return fmt.Errorf("creating temporary file for credentials file: %w", err)
	}
	t.tmpFileName = tmpKeyFile.Name()

	if _, err := tmpKeyFile.Write(t.Config.Credentials); err != nil {
		return fmt.Errorf("writing credentials to temporary file: %w", err)
	}

	log.WithFields(log.Fields{
		"instance-connection-name": t.Config.InstanceConnectionName,
		"port":                     t.Config.Port,
	}).Info("starting cloud-sql-proxy")

	logLevel := log.GetLevel().String()
	if logLevel == "warning" {
		// Adapt the logrus level to a compatible `env_filter` level of the
		// cloud-sql-proxy `tracing_subscriber`.
		logLevel = "warn"
	}

	t.Cmd = exec.CommandContext(
		t.ctx,
		"cloud-sql-proxy",
		"--port", t.Config.Port,
		"--credentials-file", tmpKeyFile.Name(),
		"--address", "127.0.0.1",
		"--auto-iam-authn",
		// Enables health check endpoints which we use to confirm the proxy is up
		"--health-check",
		t.Config.InstanceConnectionName,
	)
	// Assign process gid to this process and its children
	// so we can kill them together in the end
	t.Cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := t.Cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("could not get stdout pipe for cloud-sql-proxy: %w", err)
	}

	stderr, err := t.Cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("could not get stderr pipe for cloud-sql-proxy: %w", err)
	}
	go func() {
		_, err = io.Copy(os.Stderr, stderr)
		if err != nil {
			log.WithField("error", err).Info("error copying stderr of proxy")
		}
	}()

	go func() {
		_, err = io.Copy(os.Stderr, stdout)
		if err != nil {
			log.WithField("error", err).Info("error copying stdout of proxy")
		}
	}()

	if err := t.Cmd.Start(); err != nil {
		return fmt.Errorf("starting cloud-sql-proxy: %w", err)
	}

	var startupTimeout = 10 * time.Second
	var start = time.Now()
	for {
		resp, err := http.Get("http://localhost:9090/startup")
		if err != nil {
			if time.Since(start) > startupTimeout {
				return fmt.Errorf("cloud-sql-proxy startup timeout: %w", err)
			}
			continue
		}
		defer resp.Body.Close()

		var okBuf = make([]byte, 2)
		var ok = []byte("ok")
		if _, err = io.ReadFull(resp.Body, okBuf); err != nil {
			return fmt.Errorf("cloud-sql-proxy startup reading output: %w", err)
		} else if !bytes.Equal(okBuf, ok) {
			return fmt.Errorf("cloud-sql-proxy returned %v instead of ok", okBuf)
		}

		break
	}

	log.Info("cloud-sql-proxy ready")

	return nil
}

func (t *CloudSQLProxy) Stop() {
	if t.Cmd != nil {
		// Using the negative pid signals kill to a process group.
		// This ensures the children of the process are also killed
		syscall.Kill(-t.Cmd.Process.Pid, syscall.SIGKILL)
	}
	t.cancel()
	// cleanup key file
	if t.tmpFileName != "" {
		os.Remove(t.tmpFileName)
	}
}
