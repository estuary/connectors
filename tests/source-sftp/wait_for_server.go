// wait_for_server blocks until the test SFTP server accepts an SSH connection, then prints the host
// key it presented as a known_hosts line for --known-hosts-address, so the test harness can pin it
// in the connector's configuration. The server generates a fresh host key on every container start,
// so the key can only be learned at test time.
//
// The prober dials --address (reachable from the host running the test) while the connector dials
// --known-hosts-address (reachable inside the docker network). Same server, two names: the printed
// line must carry the connector's name or the pin never matches.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var username = flag.String("username", "test", "sftp username")
var password = flag.String("password", "test", "sftp password")
var address = flag.String("address", "localhost:2222", "sftp address (host:port) to dial")
var knownHostsAddress = flag.String("known-hosts-address", "", "address (host:port) to write in the printed known_hosts line; defaults to --address")
var waitTime = flag.Int("wait-time", 20, "wait time in seconds for the server to be ready")

func main() {
	flag.Parse()
	if *knownHostsAddress == "" {
		*knownHostsAddress = *address
	}

	var hostKey ssh.PublicKey
	sshConfig := ssh.ClientConfig{
		User: *username,
		Auth: []ssh.AuthMethod{ssh.Password(*password)},
		// The prober's job is to learn the key, not verify it: this is the trusted CI network,
		// and whatever key the server presents here is what the connector is then pinned to.
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			hostKey = key
			return nil
		},
	}

	for idx := 0; idx < *waitTime; idx++ {
		if conn, err := ssh.Dial("tcp", *address, &sshConfig); err == nil {
			conn.Close()
			fmt.Println(knownhosts.Line([]string{*knownHostsAddress}, hostKey))
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "waiting for sftp server at %s: %v\n", *address, err)
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Fprintf(os.Stderr, "gave up waiting for sftp server at %s after %d seconds\n", *address, *waitTime)
	os.Exit(1)
}
