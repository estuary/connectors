use structopt::StructOpt;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use source_kafka::connector::Command;
fn main() -> eyre::Result<()> {
    setup_tracing();

    let cmd = Command::from_args();
    cmd.execute::<source_kafka::KafkaConnector>()?;

    Ok(())
}

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
