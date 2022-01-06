use thrussh_keys::key;

use super::errors::Error;

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc};
use thrussh::{client::Handle, client};
use base64::decode;
use tokio::net::{TcpListener, TcpStream};
use futures::{select, FutureExt};
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
    local_listener: TcpListener
}

impl SshTunnel {
    pub async fn create(ssh_endpoint: String, local_port: u32) -> Result<Self, Error> {
        // create ssh client.
        let mut ssh_addrs = ssh_endpoint.to_socket_addrs()?;
        let ssh_addr = ssh_addrs.next().ok_or(Error::InvalidSshEndpoint)?;
        let config = Arc::new(client::Config::default());
        let handler = ClientHandler {};
        let ssh_client = client::connect( config, ssh_addr, handler).await?;

        // create local listener.
        let local_listen_addr: SocketAddr = format!("127.0.0.1:{}", local_port)
        .parse()?;
        let listener = TcpListener::bind(local_listen_addr).await.unwrap();

        return Ok(Self { ssh_client: ssh_client, local_listener: listener })
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

    pub async fn start_serve(&mut self, remote_host: &str, remote_port: u32) -> Result<(), Error> {
        loop {
            let (forward_stream, _) = self.local_listener.accept().await.unwrap();
            let bastion_channel = self.ssh_client.channel_open_direct_tcpip(
                remote_host, remote_port, "127.0.0.1", 0).await?;

            tokio::task::spawn(async move {
                tunnel(forward_stream, bastion_channel).await;
            });
        }
    }
}

async fn tunnel(mut forward_stream: TcpStream, mut bastion_channel: client::Channel) {
    let mut buf_forward_stream = vec![0; 2048];

    loop {
        select! {
            ret_forward_stream = forward_stream.read(&mut buf_forward_stream).fuse() => match ret_forward_stream {
                Ok(n) if n == 0 => {
                    break
                },
                Ok(n) => {
                    //println!("forward_stream read {}", n);
                    bastion_channel.data(&buf_forward_stream[..n]).await.map(|_| ()).map_err(|err| {
                        eprintln!("bastion_channel write failed, err {:?}", err);
                        err
                    }).unwrap()
                },
                Err(err) =>  {
                    eprintln!("forward_stream read failed, err {:?}", err);

                    panic!("{}", err);
                }
            },
            bastion_channel_data = bastion_channel.wait().fuse() => match bastion_channel_data.unwrap() {
                thrussh::ChannelMsg::Data { ref data } => {
                forward_stream.write(&data).await.map(|_| ()).map_err(|err| {
                        eprintln!("forward_stream write failed, err {:?}", err);
                        err
                    }).unwrap();
              
                },
                thrussh::ChannelMsg::Eof => {
                  forward_stream.flush().await.unwrap();
                  break;
                },
                _ => {}
            }
        }
    }
}