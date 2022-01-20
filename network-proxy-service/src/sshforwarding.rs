use super::logging::Must;
use super::errors::Error;
use super::networkproxy::NetworkProxy;

use async_trait::async_trait;
use base64::decode;
use futures::{select, FutureExt};
use std::net::SocketAddr;
use std::sync::Arc;
use thrussh::{client::Handle, client};
use thrussh_keys::key;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use url::Url;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SshForwardingConfig {
    pub ssh_endpoint: String,
    pub ssh_user: String,
    pub ssh_private_key_base64: String,
    pub remote_host: String,
    pub remote_port: u16,
    pub local_port: u16,
}

pub struct SshForwarding {
    config: SshForwardingConfig,
    ssh_client: Option<Handle<ClientHandler>>,
    local_listener: Option<TcpListener>,
}

impl SshForwarding {
    const DEFAULT_SSH_PORT: u16 = 22;

    pub fn new(config: SshForwardingConfig) -> Self {
        return Self { config: config, ssh_client: None, local_listener: None };
    }


    pub async fn prepare_ssh_client(&mut self) -> Result<(), Error> {
        let ssh_addrs = Url::parse(&self.config.ssh_endpoint)?.socket_addrs(|| Some(Self::DEFAULT_SSH_PORT))?;
        let ssh_addr = ssh_addrs.iter().next().ok_or(Error::InvalidSshEndpoint)?;
        let config = Arc::new(client::Config::default());
        let handler = ClientHandler {};
        self.ssh_client = Some(client::connect( config, ssh_addr, handler).await?);

        Ok(())
    }

    pub async fn prepare_local_listener(&mut self) -> Result<(), Error> {
        if self.config.local_port == 0 {
            return Err(Error::ZeroLocalPort);
        }
        let local_listen_addr: SocketAddr = format!("127.0.0.1:{}", self.config.local_port).parse()?;
        self.local_listener = Some(TcpListener::bind(local_listen_addr).await?);

        Ok(())
    }

    pub async fn authenticate(&mut self) -> Result<(), Error> {
        let pem = decode(&self.config.ssh_private_key_base64)?;

        let key_pair = Arc::new(key::KeyPair::RSA {
            key: openssl::rsa::Rsa::private_key_from_pem(&pem)?,
            hash: key::SignatureHash::SHA2_256,
        });

        let sc = self.ssh_client.as_mut().ok_or(Error::SshClientUnInitialized)?;
        if !sc.authenticate_publickey(&self.config.ssh_user, key_pair).await? {
            return Err(Error::InvalidSshCredential)
        }

        Ok(())
    }
}

#[async_trait]
impl NetworkProxy for SshForwarding {
    async fn prepare(&mut self) -> Result<(), Error> {
        self.prepare_ssh_client().await?;
        self.prepare_local_listener().await?;
        self.authenticate().await?;
        Ok(())
    }

    async fn start_serve(&mut self) -> Result<(), Error> {
        let sc = self.ssh_client.as_mut().ok_or(Error::SshClientUnInitialized)?;
        let ll = self.local_listener.as_mut().ok_or(Error::LocalListenerUnInitialized)?;
        loop {
            let (forward_stream, _) = ll.accept().await?;
            let bastion_channel = sc.channel_open_direct_tcpip(
                &self.config.remote_host,
                self.config.remote_port as u32,
                "127.0.0.1", 0).await?;
            tokio::task::spawn(async move {
                tunnel_streaming(forward_stream, bastion_channel).await.or_bail("tunnel_handle failed.");
            });
        }
    }
}

async fn tunnel_streaming(mut forward_stream: TcpStream, mut bastion_channel: client::Channel) -> Result<(), Error>{
    // Allocate a buffer of 128 KiB for forward stream.
    let mut buf_forward_stream = vec![0; 2 << 17];

    loop {
        select! {
            bytes_read = forward_stream.read(&mut buf_forward_stream).fuse() => match bytes_read? {
                0 => {
                    bastion_channel.eof().await?;
                    break
                },
                n => {
                    bastion_channel.data(&buf_forward_stream[..n]).await?;
                },
            },
            bastion_channel_data = bastion_channel.wait().fuse() => match bastion_channel_data {
                None => {},
                Some(chan_data) => match chan_data {
                    thrussh::ChannelMsg::Eof => {
                      forward_stream.flush().await?;
                      break;
                    },

                    thrussh::ChannelMsg::Data { ref data } => {
                        forward_stream.write(&data).await?;
                    },
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

pub struct ClientHandler {}

impl client::Handler for ClientHandler {
    type Error = thrussh::Error;
    type FutureUnit = futures::future::Ready<Result<(Self, client::Session), Self::Error>>;
    type FutureBool = futures::future::Ready<Result<(Self, bool), Self::Error>>;

    // For the tunneling application, trivial functions, which immediately return Ready futures, are sufficient for
    // the default implementations of the other APIs of the client handler.
    fn finished_bool(self, b: bool) -> Self::FutureBool {
        futures::future::ready(Ok((self, b)))
    }
    fn finished(self, session: client::Session) -> Self::FutureUnit {
        futures::future::ready(Ok((self, session)))
    }
    fn check_server_key(self, _server_public_key: &key::PublicKey) -> Self::FutureBool {
        self.finished_bool(true)
    }
}