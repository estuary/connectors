use crate::{read_capture_request, write_capture_response};
use anyhow::Context;
use proto_flow::{
    capture::{
        response::{Captured, Checkpoint},
        Response,
    },
    flow::ConnectorState,
};
use serde::Serialize;
use tokio::{
    io,
    sync::{mpsc, oneshot},
};

pub struct Acknowledgements(io::BufReader<io::Stdin>);
impl Acknowledgements {
    pub async fn next_ack(&mut self) -> anyhow::Result<u32> {
        let req = read_capture_request(&mut self.0).await?.context("expected acknowledgement, not EOF")?;
        let Some(ack) = req.acknowledge else {
            anyhow::bail!("expected Acknowledge message, got: {:?}", req);
        };
        if ack.checkpoints == 0 {
            anyhow::bail!("expected Acknowledge.checkpoints to be > 0, got: {ack:?}");
        };
        Ok(ack.checkpoints)
    }
}

pub struct Emitter(io::Stdout);

impl Emitter {
    pub async fn emit_doc(&mut self, binding: u32, doc_json: String) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;

        let resp = Response {
            captured: Some(Captured { binding, doc_json: doc_json.into() }),
            ..Default::default()
        };
        let resp = serde_json::to_vec(&resp).context("serializing response")?;
        self.0.write_all(&resp).await.context("writing response")?;
        self.0
            .write_u8(b'\n')
            .await
            .context("writing response newline")?;
        Ok(())
    }

    pub async fn commit(&mut self, cp: &impl Serialize, merge_patch: bool) -> anyhow::Result<()> {
        let updated_json = serde_json::to_vec(cp).context("serializing driver checkpoint")?;
        let resp = Response {
            checkpoint: Some(Checkpoint {
                state: Some(ConnectorState {
                    updated_json: updated_json.into(),
                    merge_patch,
                }),
            }),
            ..Default::default()
        };
        write_capture_response(resp, &mut self.0)
            .await
            .context("writing checkpoint")
    }
}

struct Transaction {
    docs: Vec<(u32, String)>,
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

        tokio::task::spawn(async move {
            match transactor_loop(emitter, client_rx, inner_tx).await {
                Ok(()) => tracing::info!("transactor loop exited normally"),
                Err(err) => tracing::error!(error = ?err, "transactor loop exited with error"),
            }
        });
        tokio::task::spawn(async move {
            match ack_loop(acks, inner_rx).await {
                Ok(()) => tracing::info!("ack loop exited normally"),
                Err(err) => tracing::error!(error = ?err, "ack loop exited with error"),
            }
        });

        Transactor { client_tx }
    }

    /// Outputs the given documenents, followed by a checkpoint. The future will not complete
    /// until the transactor has read an acknowledgement that Flow transaction has completed,
    /// or an error occurs.
    pub async fn publish(&self, docs: Vec<(u32, String)>) -> anyhow::Result<()> {
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
    mut pub_rx: mpsc::Receiver<oneshot::Sender<()>>,
) -> anyhow::Result<()> {
    // Counter of checkpoints that have been acknowledged already.
    let mut ack_count: u32 = 0;
    while let Some(completion_tx) = pub_rx.recv().await {
        // Do we need to wait for an acknowledgement?
        if ack_count == 0 {
            ack_count = acks.next_ack().await.context("awaiting acknowledgement")?;
            tracing::debug!(checkpoint_count = ack_count, "received ACK");
        }
        let _ = completion_tx.send(());
        ack_count -= 1;
    }
    Ok(())
}

async fn transactor_loop(
    mut emitter: Emitter,
    mut client_rx: mpsc::Receiver<Transaction>,
    mut acks_tx: mpsc::Sender<oneshot::Sender<()>>,
) -> anyhow::Result<()> {
    while let Some(txn) = client_rx.recv().await {
        try_handle_txn(txn, &mut emitter, &mut acks_tx).await?;
    }
    Ok(())
}

async fn try_handle_txn(
    mut txn: Transaction,
    emitter: &mut Emitter,
    acks_tx: &mut mpsc::Sender<oneshot::Sender<()>>,
) -> anyhow::Result<()> {
    for (binding_index, doc) in std::mem::take(&mut txn.docs) {
        emitter.emit_doc(binding_index, doc).await?;
    }
    let checkpoint = serde_json::Value::Object(serde_json::Map::new());
    emitter.commit(&checkpoint, false).await?;

    acks_tx
        .send(txn.completion_tx)
        .await
        .map_err(|_| anyhow::anyhow!("acknowledgements task stopped"))
}
