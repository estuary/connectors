use super::logging::Must;
use super::errors::Error;

use base64::decode;
use futures::{select, FutureExt};
use port_scanner::local_port_available;
use rand::{thread_rng, Rng};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use thrussh::{client::Handle, client};
use thrussh_keys::key;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};

pub struct ClientHandler {}

impl client::Handler for ClientHandler {
    type Error = thrussh::Error;
    type FutureUnit = futures::future::Ready<Result<(Self, client::Session), Self::Error>>;
    type FutureBool = futures::future::Ready<Result<(Self, bool), Self::Error>>;

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

pub struct SshTunnel {
    ssh_client: Handle<ClientHandler>,
    local_listener: TcpListener,
    deployed_port: u16
}

impl SshTunnel {
    pub async fn create(ssh_endpoint: String, local_port: u16) -> Result<Self, Error> {
        // create ssh client.
        let mut ssh_addrs = ssh_endpoint.to_socket_addrs()?;
        let ssh_addr = ssh_addrs.next().ok_or(Error::InvalidSshEndpoint)?;
        let config = Arc::new(client::Config::default());
        let handler = ClientHandler {};
        let ssh_client = client::connect( config, ssh_addr, handler).await?;

        // create local listener.
        let deployed_port = find_available_port(local_port)?;
        let local_listen_addr: SocketAddr = format!("127.0.0.1:{}", deployed_port)
        .parse()?;
        let listener = TcpListener::bind(local_listen_addr).await?;

        return Ok(Self { ssh_client: ssh_client, local_listener: listener, deployed_port: deployed_port })
    }

    pub async fn authenticate(&mut self, ssh_user: &str, ssh_private_key_base64: &str) -> Result<(), Error> {
        let pem = decode(ssh_private_key_base64)?;

        let key_pair = Arc::new(key::KeyPair::RSA {
            key: openssl::rsa::Rsa::private_key_from_pem(&pem)?,
            hash: key::SignatureHash::SHA2_256,
        });

        match self.ssh_client.authenticate_publickey(ssh_user, key_pair).await? {
            true => Ok(()),
            false => Err(Error::InvalidSshCredential)
        }
    }

    pub async fn start_serve(&mut self, remote_host: &str, remote_port: u16) -> Result<(), Error> {
        loop {
            let (forward_stream, _) = self.local_listener.accept().await?;
            let bastion_channel = self.ssh_client.channel_open_direct_tcpip(
                remote_host, remote_port as u32, "127.0.0.1", 0).await?;

            tokio::task::spawn(async move {
                tunnel_streaming(forward_stream, bastion_channel).await.or_bail("tunnel_handle failed.");
            });
        }
    }

    pub fn deployed_port(&self) -> u16 {
        self.deployed_port
    }
}

fn find_available_port(suggested_port: u16) -> Result<u16, Error> {
    if suggested_port == 0 {
        let mut rng = thread_rng();
        loop {
            let p = 10000 + rng.gen_range(1..10000);
            if local_port_available(p) {
                break Ok(p)
            }
        }
    } else if local_port_available(suggested_port) {
        Ok(suggested_port)
    } else {
        Err(Error::LocalPortUnavailableError(suggested_port))
    }
}

async fn tunnel_streaming(mut forward_stream: TcpStream, mut bastion_channel: client::Channel) -> Result<(), Error>{
    let mut buf_forward_stream = vec![0; 2048];

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