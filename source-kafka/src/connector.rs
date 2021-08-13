use std::fs::File;
use std::io::{stdout, Read, Write};
use std::path::{Path, PathBuf};

use color_eyre::eyre::Context;
use structopt::StructOpt;

use crate::airbyte;

#[derive(Debug, StructOpt)]
#[structopt(name = "source-kafka", about = "A streaming Kafka connector.")]
pub enum Command {
    /// Outputs the connector specification used to create the config file.
    Spec,

    /// Validates the config file parameters. Connects to Kafka validate the
    /// connection can be made. Outputs the connection status.
    Check {
        /// Path to the connector config file.
        #[structopt(long, parse(from_os_str))]
        config: PathBuf,
    },

    /// Inspects the running Kafka instance for available topics and partitions.
    /// Outputs the catalog describing the data.
    Discover {
        /// Path to the connector config file.
        #[structopt(long, parse(from_os_str))]
        config: PathBuf,
    },

    /// Consumes data from Kafka according to the catalog spec.
    Read {
        /// Path to the connector config file.
        #[structopt(long, parse(from_os_str))]
        config: PathBuf,

        /// Path to the catalog file.
        #[structopt(long, parse(from_os_str))]
        catalog: PathBuf,

        /// Path to the state file.
        #[structopt(long, parse(from_os_str))]
        state: Option<PathBuf>,
    },
}

impl Command {
    pub fn execute<C: Connector>(&self) -> color_eyre::Result<()> {
        let stdout_guard = stdout();
        let mut stdout = Box::new(stdout_guard.lock());

        let result = match self {
            Command::Spec => C::spec(&mut stdout),
            Command::Check { config } => {
                C::check(&mut stdout, C::Config::parse(open_file(config)?)?)
            }
            Command::Discover { config } => {
                C::discover(&mut stdout, C::Config::parse(open_file(config)?)?)
            }
            Command::Read {
                config,
                catalog,
                state,
            } => {
                let state = match state {
                    None => None,
                    Some(s) => {
                        // Can't use Option::map with `?`, so we get to match here.
                        Some(C::State::parse(open_file(s)?)?)
                    }
                };

                C::read(
                    &mut stdout,
                    C::Config::parse(open_file(config)?)?,
                    C::ConfiguredCatalog::parse(open_file(catalog)?)?,
                    state,
                )
            }
        };

        match result {
            Err(e) if e.is::<StdoutError>() => {
                // Stdout has been closed, so we should gracefully shut down rather than panicking.
                Ok(())
            }
            otherwise => otherwise,
        }
    }
}

fn open_file(path: impl AsRef<Path>) -> color_eyre::Result<File> {
    File::open(&path).wrap_err(format!("failed to read {:?}", path.as_ref()))
}

pub trait ConnectorConfig: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    fn parse(reader: impl Read) -> Result<Self, Self::Error>;
}

pub trait Connector {
    type Config: ConnectorConfig;
    type ConfiguredCatalog: ConnectorConfig;
    type State: ConnectorConfig;

    fn spec(output: &mut dyn Write) -> color_eyre::Result<()>;
    fn check(output: &mut dyn Write, config: Self::Config) -> color_eyre::Result<()>;
    fn discover(output: &mut dyn Write, config: Self::Config) -> color_eyre::Result<()>;
    fn read(
        output: &mut dyn Write,
        config: Self::Config,
        catalog: Self::ConfiguredCatalog,
        state: Option<Self::State>,
    ) -> color_eyre::Result<()>;
}

#[derive(Debug, thiserror::Error)]
pub enum StdoutError {
    #[error("failed to write message to output")]
    Output(#[source] serde_json::Error),

    #[error("failed to emit a newline between messages")]
    OutputNewline(#[source] std::io::Error),
}

pub fn write_message<M: airbyte::Message>(
    mut output: &mut dyn Write,
    message: M,
) -> Result<(), StdoutError> {
    serde_json::to_writer(&mut output, &airbyte::Envelope::from(message))
        .map_err(StdoutError::Output)?;

    // Include a newline to break up the document stream.
    writeln!(&mut output).map_err(StdoutError::OutputNewline)?;
    Ok(())
}
