use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Input {
    pub ssh_forwarding_config: SshForwardingConfig,
    pub local_port: u16,
    pub max_polling_retry_times: u16,
}

#[derive(Serialize, Deserialize)]
pub struct Output {
    pub deployed_local_port: u16,
}

#[derive(Serialize, Deserialize, Default)]
pub struct SshForwardingConfig {
    pub ssh_endpoint: String,
    pub ssh_user: String,
    pub ssh_private_key_base64: String,
    pub remote_host: String,
    pub remote_port: u16,
}

impl SshForwardingConfig {
    pub fn tunnel_string(&self, local_port: u16) -> String {
        format!("*:{}:{}", local_port, self.remote_endpoint())
    }

    pub fn connection_string(&self) -> String {
        if self.ssh_user.is_empty() {
            format!("ssh://{}", self.ssh_endpoint)
        } else {
            format!("ssh://{}@{}", self.ssh_user, self.ssh_endpoint)
        }
    }

    fn remote_endpoint(&self) -> String {
        if self.remote_port == 0 {
            self.remote_host.clone()
        } else {
            format!("{}:{}", self.remote_host, self.remote_port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tunnel_string() {
        let mut config = SshForwardingConfig::default();
        config.remote_host = String::from("remote_host.com");

        config.remote_port = 0;
        assert_eq!(
            config.tunnel_string(1234),
            String::from("*:1234:remote_host.com")
        );

        config.remote_port = 5678;
        assert_eq!(
            config.tunnel_string(1234),
            String::from("*:1234:remote_host.com:5678")
        );
    }

    #[test]
    fn test_connection_string() {
        let mut config = SshForwardingConfig::default();
        config.ssh_endpoint = String::from("ssh_host.com");
        assert_eq!(
            config.connection_string(),
            String::from("ssh://ssh_host.com")
        );

        config.ssh_user = String::from("test_user");
        assert_eq!(
            config.connection_string(),
            String::from("ssh://test_user@ssh_host.com")
        )
    }
}
