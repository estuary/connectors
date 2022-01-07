use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Input {
    pub ssh_forwarding_config: SshForwardingConfig,
    pub local_port: u16
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