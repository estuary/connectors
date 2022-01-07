#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SSH endpoint is invalid.")]
    InvalidSshEndpoint,

    #[error("SSH private key is invalid.")]
    InvalidSshCredential,

    #[error("local port {0} is unavailable.")]
    LocalPortUnavailableError(u16),

    #[error("thrussh error.")]
    ThrusshError(#[from] thrussh::Error),

    #[error("io operation error.")]
    IoError(#[from] std::io::Error),

    #[error("openssl error.")]
    OpenSslError(#[from] openssl::error::ErrorStack),

    #[error("base64 decoding error.")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("IP parse error.")]
    IpAddrParseError(#[from] std::net::AddrParseError)
}