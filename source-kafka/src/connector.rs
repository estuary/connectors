use std::path::PathBuf;

use structopt::StructOpt;

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
