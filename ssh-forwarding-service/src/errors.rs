#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SSH endpoint is invalid.")]
    InvalidSshEndpoint,

    #[error("SSH private key is invalid.")]
    InvalidSshCredential,

    #[error("failed in thrussh")]
    ThrusshError(#[from] thrussh::Error),

    #[error("failed in io operations")]
    IoError(#[from] std::io::Error),

    #[error("failed in openssl operations")]
    OpenSslError(#[from] openssl::error::ErrorStack),

    #[error("failed in base64 decoding")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("failed in parsing IP address")]
    IpAddrParseError(#[from] std::net::AddrParseError)
}