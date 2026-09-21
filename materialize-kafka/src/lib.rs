use anyhow::Result;
use apply::do_apply;
use bytes::BytesMut;
use configuration::{schema_for, EndpointConfig, Resource};
use prost::Message;
use proto_flow::flow;
use proto_flow::materialize::{
    request,
    response::{Applied, Spec, Validated},
    Request, Response,
};
use state::ConnectorState;
use std::io::{self, BufRead, BufReader, Read, StdoutLock, Write};
use transactor::run_transactions;
use validate::do_validate;

pub mod apply;
pub mod binding_info;
pub mod configuration;
pub mod state;
pub mod transactor;
pub mod validate;

const KAFKA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub async fn run_connector(mut input: Input, mut output: Output) -> Result<()> {
    while let Some(request) = input.read()? {
        if request.spec.is_some() {
            let res = Response {
                spec: Some(Spec {
                    protocol: 3032023,
                    config_schema_json: serde_json::to_string(&schema_for::<EndpointConfig>())?.into(),
                    resource_config_schema_json: serde_json::to_string(&schema_for::<Resource>())?.into(),
                    documentation_url: "https://go.estuary.dev/materialize-kafka".to_string(),
                    oauth2: None,
                }),
                ..Default::default()
            };

            output.send(res)?;
        } else if let Some(validate) = request.validate {
            let res = Response {
                validated: Some(Validated {
                    bindings: do_validate(validate).await?,
                }),
                ..Default::default()
            };

            output.send(res)?;
        } else if let Some(apply) = request.apply {
            let state = decide_state(&apply)?;
            let res = Response {
                applied: Some(Applied {
                    action_description: do_apply(apply).await?,
                    state,
                }),
                ..Default::default()
            };

            output.send(res)?;
        } else if let Some(open) = request.open {
            run_transactions(&mut input, &mut output, open).await?;
        } else {
            anyhow::bail!("invalid request, expected spec|validate|apply|open");
        }
    }

    Ok(())
}

// A task whose state has not yet decided how Avro schemas are registered
// decides it now. A task with no previously applied spec is new and takes
// logical types; any other task keeps the string encoding its topics already
// carry.
fn decide_state(apply: &request::Apply) -> Result<Option<flow::ConnectorState>> {
    let state = ConnectorState::parse(&apply.state_json)?;
    if state.avro_logical_types.is_some() {
        return Ok(None);
    }

    let decided = ConnectorState {
        avro_logical_types: Some(apply.last_materialization.is_none()),
    };
    Ok(Some(flow::ConnectorState {
        updated_json: serde_json::to_vec(&decided)?.into(),
        merge_patch: true,
    }))
}

pub struct Input {
    r: BufReader<io::Stdin>,
}

#[allow(clippy::new_without_default)]
impl Input {
    pub fn new() -> Input {
        Input {
            r: BufReader::with_capacity(1024 * 1024, io::stdin()),
        }
    }

    pub fn read(&mut self) -> Result<Option<Request>> {
        let mut length_bytes = [0; 4];
        match self.r.read_exact(&mut length_bytes) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let length = u32::from_le_bytes(length_bytes[0..4].try_into().unwrap()) as usize;

        let buffered = self.r.buffer();
        if buffered.len() >= length {
            // Having just read the message length, the message itself is
            // probably already in the buffer. Decode directly from the buffer
            // without copying.
            let res = Request::decode(&buffered[..length])?;
            self.r.consume(length);
            return Ok(Some(res));
        }

        let mut buf = vec![0; length];
        match self.r.read_exact(&mut buf) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        Ok(Some(Request::decode(buf.as_ref())?))
    }
}

pub struct Output {
    w: StdoutLock<'static>,
    buf: BytesMut,
}

#[allow(clippy::new_without_default)]
impl Output {
    pub fn new() -> Output {
        Output {
            w: io::stdout().lock(),
            buf: BytesMut::new(),
        }
    }

    pub fn send(&mut self, response: Response) -> Result<()> {
        let length = response.encoded_len();
        self.buf.reserve(4 + length);
        self.buf.extend_from_slice(&(length as u32).to_le_bytes());
        response.encode(&mut self.buf)?;
        self.w.write_all(&self.buf)?;
        self.w.flush()?;

        self.buf.clear();
        Ok(())
    }
}
