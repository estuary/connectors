use std::io::Write;

use proto_flow::capture::{Response, request};
use proto_flow::flow::{CaptureSpec, RangeSpec};

pub trait ConnectorConfig: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    fn parse(reader: &str) -> Result<Self, Self::Error>;
}

pub trait Connector {
    type Config: ConnectorConfig;
    type State: ConnectorConfig;

    fn spec(output: &mut dyn Write) -> eyre::Result<()>;
    fn validate(output: &mut dyn Write, validate: request::Validate) -> eyre::Result<()>;
    fn discover(output: &mut dyn Write, discover: request::Discover) -> eyre::Result<()>;
    fn apply(output: &mut dyn Write, config: Self::Config) -> eyre::Result<()>;
    fn read(
        output: &mut dyn Write,
        config: Self::Config,
        capture: CaptureSpec,
        range: Option<RangeSpec>,
        state: Option<Self::State>,
    ) -> eyre::Result<()>;
}

#[derive(Debug, thiserror::Error)]
pub enum StdoutError {
    #[error("failed to write message to output")]
    Output(#[source] serde_json::Error),

    #[error("failed to emit a newline between messages")]
    OutputNewline(#[source] std::io::Error),
}

pub fn write_message(
    mut output: &mut dyn Write,
    message: Response,
) -> Result<(), StdoutError> {
    serde_json::to_writer(&mut output, &message)
        .map_err(StdoutError::Output)?;

    // Include a newline to break up the document stream.
    writeln!(&mut output).map_err(StdoutError::OutputNewline)?;
    Ok(())
}
