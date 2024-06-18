from logging import Logger
import os
import stat
import subprocess
import tempfile


def ssh_tunnel(
    log: Logger,
    endpoint: str,
    key: str,
    remote_bind_address: (str, int),
    local_bind_port: int,
):
    log.info("opening ssh forwarding tunnel", {
        'ssh_endpoint': endpoint,
        'remote_bind_address': remote_bind_address,
        'local_bind_address': ("localhost", local_bind_port),
    })
    fp = tempfile.NamedTemporaryFile(delete_on_close=False)
    fp.write(key.encode('utf8'))
    fp.close()
    os.chmod(fp.name, stat.S_IREAD)

    remote_host, remote_port = remote_bind_address
    process = subprocess.Popen(["flow-network-tunnel",
                                "ssh",
                                "--ssh-endpoint", endpoint,
                                "--private-key", fp.name,
                                "--forward-host", remote_host,
                                "--forward-port", str(remote_port),
                                "--local-port", str(local_bind_port)],
                               stdout=subprocess.PIPE)

    ready = process.stdout.read(5)
    if ready == b"READY":
        log.info("network tunnel ready", {
            "forward-host": remote_host,
            "forward-port": remote_port,
            "local-port": local_bind_port,
        })
    else:
        # set non-blocking read so we can read the rest of the output that is available
        # without blocking for process exit
        os.set_blocking(process.stdout.fileno(), False)
        rest = process.stdout.read()
        log.error("network tunnel output unexpected", {
            "output": ready + rest,
        })

    return process
