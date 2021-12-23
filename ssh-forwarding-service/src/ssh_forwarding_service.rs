use super::errors::Error;
use super::interface::SshForwardingConfig;
use super::ssh_agent_service::SshAgentService;

use port_scanner::local_port_available;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

pub struct SshForwardingService {
    deployed_local_port: u16,
    ssh: Child,
}

impl SshForwardingService {
    pub fn create(
        config: &SshForwardingConfig,
        local_port: u16,
        ssh_agent: &SshAgentService,
    ) -> Result<Self, Error> {
        assert!(
            ssh_agent.started(),
            "start local forwarding service before ssh agent is started."
        );

        let deployed_local_port = if local_port == 0 {
            let mut p = 10000;
            loop {
                p += 1;
                if local_port_available(p) {
                    break p;
                }
            }
        } else if local_port_available(local_port) {
            local_port
        } else {
            return Err(Error::LocalPortUnavailableError(local_port));
        };

        let ssh = Command::new("ssh")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // inherit stderr from parent so that errors during serving will sent to the client.
            .stderr(Stdio::inherit())
            .env("SSH_AUTH_SOCK", ssh_agent.auth_sock())
            .env("SSH_AGENT_PID", ssh_agent.pid().to_string())
            .arg("-N")
            .arg("-T")
            .arg("-L")
            .arg(config.tunnel_string(deployed_local_port))
            .arg("-o")
            .arg("StrictHostKeyChecking=no")
            .arg(config.connection_string())
            .arg("-v")
            .spawn()?;
        Ok(SshForwardingService {
            ssh: ssh,
            deployed_local_port: deployed_local_port,
        })
    }

    pub fn poll_until_service_up(&self, max_retry_times: u16) -> Result<(), Error> {
        let mut wait_seconds: u64 = 1;
        let mut retries: u16 = 0;

        while retries <= max_retry_times {
            if !local_port_available(self.deployed_local_port) {
                return Ok(());
            }
            sleep(Duration::from_secs(wait_seconds));
            retries += 1;
            wait_seconds *= 2;
        }
        Err(Error::SshForwardingServiceUnavailable)
    }

    pub fn deployed_local_port(&self) -> u16 {
        self.deployed_local_port
    }

    pub fn stop(&mut self) {
        let _ = self.ssh.kill();
    }
}
