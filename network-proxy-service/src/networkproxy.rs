use super::errors::Error;

use async_trait::async_trait;

#[async_trait]
pub trait NetworkProxy {
    // Setup the network proxy server.
    async fn prepare(&mut self) -> Result<(), Error>;
    // Start a long-running task that listens and serves proxy requests from clients.
    async fn start_serve(&mut self) -> Result<(), Error>;
}

