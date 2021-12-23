#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("regex parsing yields no result")]
    RegxNoParsingResults,

    #[error("ssh command failed to start forwarding service.")]
    SshForwardingServiceUnavailable,

    #[error("Executing {cmd:?} failed. \n Output: {stdout:?}. \n stderr: {stderr:?}")]
    ExecutionError {
        cmd: &'static str,
        stdout: String,
        stderr: String,
    },

    #[error("Local port {0} is unavailable")]
    LocalPortUnavailableError(u16),
    #[error("Failed to decode base64 string..")]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("regex failed to parse")]
    RegxParsingError(#[from] fancy_regex::Error),
    #[error("failed in io operations")]
    IoError(#[from] std::io::Error),
}
