use std::io::{stdout, stdin};

use proto_flow::capture::Request;
use source_kafka::connector::{Connector, StdoutError, ConnectorConfig};
use source_kafka::KafkaConnector;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

fn main() -> eyre::Result<()> {
    setup_tracing();

    let stdout_guard = stdout();
    let mut stdout = Box::new(stdout_guard.lock());

    let lines = stdin().lines();

    for line in lines {
        let request: Request = serde_json::from_str(&line.unwrap())?;

        let result = if let Some(_spec) = request.spec {
            <KafkaConnector as Connector>::spec(&mut stdout)
        } else if let Some(validate) = request.validate {
            <KafkaConnector as Connector>::validate(&mut stdout, validate)
        } else if let Some(discover) = request.discover {
            <KafkaConnector as Connector>::discover(&mut stdout, discover)
        } else if let Some(apply) = request.apply {
            let capture = apply.capture.expect("empty capture");
            <KafkaConnector as Connector>::apply(&mut stdout, <KafkaConnector as Connector>::Config::parse(&capture.config_json)?)
        } else if let Some(open) = request.open {
            let state = <KafkaConnector as Connector>::State::parse(&open.state_json)?;
            let capture = open.capture.expect("empty capture");

            <KafkaConnector as Connector>::read(
                &mut stdout,
                <KafkaConnector as Connector>::Config::parse(&capture.config_json)?,
                capture,
                open.range,
                Some(state),
            )
        } else {
            Ok(())
        };

        match result {
            Err(e) if e.is::<StdoutError>() => {
                // Stdout has been closed, so we should gracefully shut down rather than panicking.
                return Ok(());
            }
            otherwise => otherwise.expect("error"),
        }
    }

    Ok(())
}

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
