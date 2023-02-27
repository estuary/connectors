use crate::{read_capture_request, to_raw_value, write_capture_response};
use anyhow::Context;
use connector_protocol::capture::{Request, Response};
use serde::Serialize;
use tokio::{
    io,
    sync::{mpsc, oneshot},
};

pub struct Acknowledgements(io::BufReader<io::Stdin>);
impl Acknowledgements {
    pub async fn next_ack(&mut self) -> anyhow::Result<anyhow::Result<()>> {
        let Request::Acknowledge{} = read_capture_request(&mut self.0).await? else {
            anyhow::bail!("expected Acknowledge message");
        };
        Ok(Ok(()))
    }
}

pub struct Emitter(io::Stdout);

impl Emitter {
    pub async fn emit_doc(&mut self, binding: u32, doc: &impl Serialize) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;

        // We intentionally don't use `write_capture_response` here because we don't want to wait
        // for the flush call.
        let doc = to_raw_value(doc).context("serializing output document")?;

        let resp = serde_json::to_vec(&Response::Document { binding, doc })
            .context("serializing response")?;
        self.0.write_all(&resp).await.context("writing response")?;
        self.0
            .write_u8(b'\n')
            .await
            .context("writing response newline")?;
        Ok(())
    }

    pub async fn commit(&mut self, cp: &impl Serialize, merge_patch: bool) -> anyhow::Result<()> {
        let driver_checkpoint = to_raw_value(cp).context("serializing driver checkpoint")?;
        write_capture_response(
            Response::Checkpoint {
                driver_checkpoint,
                merge_patch,
            },
            &mut self.0,
        )
        .await
        .context("writing checkpoint")
    }
}

struct Transaction {
    docs: Vec<(u32, serde_json::Value)>,
    completion_tx: oneshot::Sender<()>,
}

/// A `Transactor` takes ownership of both stdin and stdout and provides an
/// interface to output documents and await acknowledgements of their persistence.
#[derive(Clone)]
pub struct Transactor {
    client_tx: mpsc::Sender<Transaction>,
}

impl Transactor {
    /// Starts the transactor background tasks and returns a new `Transactor` that can
    /// be used to publish documents.
    pub fn start(stdin: io::BufReader<io::Stdin>, stdout: io::Stdout) -> Transactor {
        let acks = Acknowledgements(stdin);
        let emitter = Emitter(stdout);
        let pipeline_limit = 512;
        let (client_tx, client_rx) = mpsc::channel(pipeline_limit);
        let (inner_tx, inner_rx) = mpsc::channel(pipeline_limit);

        tokio::task::spawn(transactor_loop(emitter, client_rx, inner_tx));
        tokio::task::spawn(ack_loop(acks, inner_rx));

        Transactor { client_tx }
    }

    /// Outputs the given documenents, followed by a checkpoint. The future will not complete
    /// until the transactor has read an acknowledgement that Flow transaction has completed,
    /// or an error occurs.
    pub async fn publish(&self, docs: Vec<(u32, serde_json::Value)>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.client_tx
            .send(Transaction {
                docs,
                completion_tx: tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("transactor shutdown"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("failed to publish docs"))
    }
}

async fn ack_loop(
    mut acks: Acknowledgements,
    mut pub_rx: mpsc::Receiver<Transaction>,
) -> anyhow::Result<()> {
    while let Some(txn) = pub_rx.recv().await {
        // some documents have been published! Let's wait for the acknowledgement.
        let result = acks.next_ack().await;
        if let Ok(_) = result {
            // TODO: should probs logs this
            let _ = txn.completion_tx.send(());
        }
    }
    tracing::debug!("ack loop completed normally");
    Ok(())
}

async fn transactor_loop(
    mut emitter: Emitter,
    mut client_rx: mpsc::Receiver<Transaction>,
    mut acks_tx: mpsc::Sender<Transaction>,
) {
    while let Some(txn) = client_rx.recv().await {
        if let Err(err) = try_handle_txn(txn, &mut emitter, &mut acks_tx).await {
            tracing::error!(error = ?err, "failed to puslish documents");
            return;
        }
    }
    tracing::debug!("transactor loop completed without error");
}

async fn try_handle_txn(
    txn: Transaction,
    emitter: &mut Emitter,
    acks_tx: &mut mpsc::Sender<Transaction>,
) -> anyhow::Result<()> {
    for (binding_index, doc) in txn.docs.iter() {
        emitter.emit_doc(*binding_index, doc).await?;
    }
    let checkpoint = serde_json::Value::Object(serde_json::Map::new());
    emitter.commit(&checkpoint, false).await?;

    acks_tx
        .send(txn)
        .await
        .map_err(|_| anyhow::anyhow!("acknowledgements task stopped"))
}
