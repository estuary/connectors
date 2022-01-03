pub mod interface;

use interface::{Input, Output};
use async_io::Async;
use async_ssh2_lite::{AsyncChannel, AsyncSession};
use futures::executor::block_on;
use futures::select;
use futures::{AsyncReadExt, AsyncWriteExt, FutureExt};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::io;
use tokio::time::{sleep, Duration};
use base64::decode;
use std::str;
use std::io::Write;

#[tokio::main]
async fn main() -> io::Result<()> {
    let input: Input = serde_json::from_reader(io::stdin()).expect("valid input.");
    let mut ssh_addrs = input.ssh_forwarding_config.ssh_endpoint.to_socket_addrs().unwrap();

    // Connect to the SSH server
    let tcp = Async::<TcpStream>::connect(ssh_addrs.next().unwrap()).await.unwrap();
    let mut sess = AsyncSession::new(tcp, None).unwrap();

    sess.handshake().await?;
    let key = decode(input.ssh_forwarding_config.ssh_private_key_base64).expect("key is not base64 encoded");

    sess.userauth_pubkey_memory(&input.ssh_forwarding_config.ssh_user, None, str::from_utf8(&key).unwrap(), None)
        .await
        .expect("failed to auth user.");

    // Make sure we succeeded
    assert!(sess.authenticated());

    let local_listen_addr: SocketAddr = format!("127.0.0.1:{}", input.local_port)
        .parse()
        .expect("constant ip string is valid.");

    let listener = Async::<TcpListener>::bind(local_listen_addr).unwrap();

    // Write output to stdio.
    serde_json::to_writer(
        io::stdout(),
        &Output {
            deployed_local_port: input.local_port,
        },
    ).unwrap();

    io::stdout()
        .write_all(&[0])
        .expect("Failed to write to stdout");
    io::stdout().flush().expect("Failed flush output.");

    loop {
        let (mut forward_stream, s) = listener.accept().await.unwrap();
        let mut bastion_channel = match sess.channel_direct_tcpip(&input.ssh_forwarding_config.remote_host, input.ssh_forwarding_config.remote_port, None).await {
            Ok(c) => c,
            Err(e) => {
                // Retry once.
                sess.channel_direct_tcpip(&input.ssh_forwarding_config.remote_host, input.ssh_forwarding_config.remote_port, None)
                    .await
                    .unwrap()
            }
        };

        tokio::task::spawn(async move {
            tunnel(forward_stream, bastion_channel).await;
        });

        // https://stackoverflow.com/questions/59116096/libssh2-session-handshake-return-43
        sleep(Duration::from_millis(50)).await;
    }
}

async fn tunnel(
    mut forward_stream: Async<TcpStream>,
    mut bastion_channel: AsyncChannel<TcpStream>,
) {
    let mut buf_bastion_channel = vec![0; 2048];
    let mut buf_forward_stream = vec![0; 2048];

    loop {
        select! {
            ret_forward_stream = forward_stream.read(&mut buf_forward_stream).fuse() => match ret_forward_stream {
                Ok(n) if n == 0 => {
                    //println!("forward_stream read 0");
                    break
                },
                Ok(n) => {
                    //println!("forward_stream read {}", n);
                    bastion_channel.write(&buf_forward_stream[..n]).await.map(|_| ()).map_err(|err| {
                        eprintln!("bastion_channel write failed, err {:?}", err);
                        err
                    }).unwrap()
                },
                Err(err) =>  {
                    eprintln!("forward_stream read failed, err {:?}", err);

                    panic!("{}", err);
                }
            },
            ret_bastion_channel = bastion_channel.read(&mut buf_bastion_channel).fuse() => match ret_bastion_channel {
                Ok(n) if n == 0 => {
                    //println!("bastion_channel read 0");
                    break
                },
                Ok(n) => {
                    //println!("bastion_channel read {}", n);
                    forward_stream.write(&buf_bastion_channel[..n]).await.map(|_| ()).map_err(|err| {
                        eprintln!("forward_stream write failed, err {:?}", err);
                        err
                    }).unwrap();
                },
                Err(err) => {
                    //eprintln!("bastion_channel read failed, err {:?}", err);
                    panic!("{}", err);
                }
            },
        }
    }
}
