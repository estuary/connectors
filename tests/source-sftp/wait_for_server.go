package main

import (
	"flag"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

var username = flag.String("username", "test", "sftp username")
var password = flag.String("password", "test", "sftp password")
var address = flag.String("address", "localhost:2222", "sftp address (host:port)")
var waitTime = flag.Int("wait-time", 20, "wait time in seconds for the server to be ready")

func main() {
	flag.Parse()

	sshConfig := ssh.ClientConfig{
		User:            *username,
		Auth:            []ssh.AuthMethod{ssh.Password(*password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	for idx := 0; idx < *waitTime; idx++ {
		if _, err := ssh.Dial("tcp", *address, &sshConfig); err == nil {
			os.Exit(0)
		}
		time.Sleep(1 * time.Second)
	}

	os.Exit(1)
}
