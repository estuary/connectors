use super::logging::Must;
use super::errors::Error;
use super::networkproxy::NetworkProxy;

use async_trait::async_trait;
use base64::decode;
use core::task::{Context, Poll};
use futures::FutureExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::pin::Pin;
use thrussh::{client::Handle, client, ChannelMsg};
use thrussh_keys::key;
use tokio::net::TcpListener;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, copy_bidirectional};
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
            let (mut forward_stream, _) = ll.accept().await?;
            let bastion_channel = sc.channel_open_direct_tcpip(
                &self.config.remote_host,
                self.config.remote_port as u32,
                "127.0.0.1", 0).await?;
            tokio::task::spawn(async move {
                copy_bidirectional(&mut forward_stream,
                                   &mut ChannelWrapper::new(bastion_channel)
                ).await.or_bail("SSH tunnel handler failed.");
            });
        }
    }
}

struct ChannelWrapper {
    channel: client::Channel,
    channel_eof: bool,
    crypto_vec: Option<thrussh::CryptoVec>,
    crypto_vec_read_start: usize,
}

impl ChannelWrapper {
    pub fn new(channel: client::Channel) -> Self {
        ChannelWrapper{
            channel: channel,
            channel_eof: false,
            crypto_vec: None,
            crypto_vec_read_start: 0
        } 
    }
}

impl ChannelWrapper {
    fn is_channel_eof(&mut self) -> bool {
        return self.channel_eof;
    }

    fn read_from_crypto_vec(&mut self, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.crypto_vec {
            None => Poll::Pending,
            Some(ref v) => {
                let amt = std::cmp::min(v.len() - self.crypto_vec_read_start, buf.remaining());
                let read_end = self.crypto_vec_read_start + amt;
                buf.put_slice(&v[self.crypto_vec_read_start..read_end]);

                if read_end == v.len() {
                    self.crypto_vec = None;
                    self.crypto_vec_read_start = 0;
                } else {
                    self.crypto_vec_read_start = read_end;
                }

                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_channel(&mut self, cx: &mut Context<'_>) {
        if !self.crypto_vec.is_none() {
            return
        }

        loop {
            match self.channel.wait().boxed().poll_unpin(cx) {
                Poll::Pending => return,
                Poll::Ready(channel_data) => match channel_data {
                    None => {}, // Ignore empty message, keep polling.
                    Some(channel_msg) => match channel_msg {
                        ChannelMsg::Eof => {
                            self.channel_eof = true;
                            return
                        },

                        ChannelMsg::Data { data } => {
                            self.crypto_vec = Some(data);
                            return
                        },

                        _ => {} // Ignore the other messages, keep polling.   
                    }
                }
            }
        }
    }

}
impl AsyncRead for ChannelWrapper {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.poll_channel(cx);

        if self.is_channel_eof() {
            Poll::Ready(Ok(()))
        } else {
            self.read_from_crypto_vec(buf)
        }
   }
}

impl AsyncWrite for ChannelWrapper {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.channel.data(buf).boxed().poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
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