use anyhow::Result;
use proto_flow::materialize::{
    request,
    request::Open,
    response,
    response::{Acknowledged, Flushed, Opened, StartedCommit},
    Response,
};

use crate::{Input, Output};

pub async fn run_transactions(input: &mut Input, output: &mut Output, _open: Open) -> Result<()> {
    output.send(Response {
        kind: Some(response::Kind::Opened(Opened {
            runtime_checkpoint: None,
            ..Default::default()
        })),
        ..Default::default()
    })?;

    loop {
        let request = match input.read()? {
            Some(req) => req,
            None => return Ok(()),
        };

        match request.kind {
            Some(request::Kind::Flush(_)) => {
                output.send(Response {
                    kind: Some(response::Kind::Flushed(Flushed { state: None })),
                    ..Default::default()
                })?;
            }
            Some(request::Kind::Store(_)) => {
                // Storage isn't implemented yet: documents are received and discarded.
            }
            Some(request::Kind::StartCommit(_)) => {
                output.send(Response {
                    kind: Some(response::Kind::StartedCommit(StartedCommit { state: None })),
                    ..Default::default()
                })?;
            }
            Some(request::Kind::Acknowledge(_)) => {
                output.send(Response {
                    kind: Some(response::Kind::Acknowledged(Acknowledged { state: None })),
                    ..Default::default()
                })?;
            }
            _ => {}
        }
    }
}
