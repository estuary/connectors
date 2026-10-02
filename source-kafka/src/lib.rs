use std::io::{BufWriter, Stdout, Write};

use anyhow::{Context, Result};
use configuration::{schema_for, EndpointConfig, Resource, SchemaRegistryConfig};
use discover::do_discover;
use proto_flow::capture::{
    request::Validate,
    response::{
        validated::Binding as ValidatedBinding, Applied, Discovered, Opened, Spec, Validated,
    },
    Request, Response,
};
use pull::do_pull;
use rdkafka::consumer::Consumer;
use schema_registry::SchemaRegistryClient;
use tokio::io::{self, AsyncBufReadExt};

pub mod configuration;
pub mod discover;
pub mod document;
pub mod msk_oauthbearer;
pub mod protobuf;
pub mod pull;
pub mod schema_registry;

const KAFKA_METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    mut stdout: BufWriter<Stdout>,
) -> Result<(), anyhow::Error> {
    tracing::info!("running connector");

    let mut line = String::new();

    while stdin.read_line(&mut line).await? != 0 {
        let request: Request = serde_json::from_str(&line)?;
        line.clear();

        if request.spec.is_some() {
            let res = Response {
                spec: Some(Spec {
                    protocol: 3032023,
                    config_schema_json: serde_json::to_string(&schema_for::<EndpointConfig>())?.into(),
                    resource_config_schema_json: serde_json::to_string(&schema_for::<Resource>())?.into(),
                    documentation_url: "https://go.estuary.dev/source-kafka".to_string(),
                    oauth2: None,
                    resource_path_pointers: vec!["/topic".to_string()],
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.discover {
            let res = Response {
                discovered: Some(Discovered {
                    bindings: do_discover(req).await?,
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.validate {
            tracing::info!(eventType = "connectorStatus", "Validating capture configuration");

            let res = Response {
                validated: Some(Validated {
                    bindings: do_validate(req).await?,
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if request.apply.is_some() {
            tracing::info!(eventType = "connectorStatus", "Applying capture configuration");

            let res = Response {
                applied: Some(Applied {
                    action_description: String::new(),
                    state: None,
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.open {
            write_capture_response(
                Response {
                    opened: Some(Opened {
                        explicit_acknowledgements: false,
                    }),
                    ..Default::default()
                },
                &mut stdout,
            )?;

            let num_bindings = req.capture.as_ref().map_or(0, |spec| spec.bindings.len());
            if num_bindings == 0 {
                tracing::info!(
                    eventType = "connectorStatus",
                    "Starting capture, no bindings are enabled"
                );
            } else {
                tracing::info!(
                    eventType = "connectorStatus",
                    bindings = num_bindings,
                    "Starting capture"
                );
            }

            let eof = tokio::spawn(async move {
                let mut line_string = String::new();
                match stdin.read_line(&mut line_string).await? {
                    0 => Ok(()),
                    n => anyhow::bail!(
                        "read {} bytes from stdin when explicit acknowledgements were not requested",
                        n
                    ),
                }
            });

            let pull = tokio::spawn(do_pull(req, stdout));

            tokio::select! {
                pull_res = pull => pull_res??,
                eof_res = eof => eof_res??,
            }

            return Ok(());
        } else {
            anyhow::bail!("invalid request, expected spec|discover|validate|apply|open");
        }
    }

    Ok(())
}

pub fn write_capture_response<W: Write>(
    response: Response,
    out: &mut W,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, &response).context("serializing response")?;
    writeln!(out).context("writing response newline")?;

    // Captured documents accumulate in the buffer and are flushed by the next
    // Checkpoint. This batches many documents into a few large writes instead
    // of one syscall per document.
    if response.captured.is_none() {
        out.flush().context("flushing output")?;
    }
    Ok(())
}

/// Write a `Captured` response for `doc_json` as raw bytes: the line that
/// serializing a `Response` would produce, without building one.
///
/// The `Response` path re-parses `doc_json` as a `RawValue` to validate it,
/// a second scan of every document. The bytes here come straight from our
/// own serializer, so that scan is skipped; the tests pin the two outputs
/// byte for byte.
///
/// Does not flush. The next checkpoint flushes the buffered documents.
pub fn write_captured<W: Write>(binding: u32, doc_json: &[u8], out: &mut W) -> anyhow::Result<()> {
    use serde_json::ser::{CompactFormatter, Formatter};

    // Mirror the generated serializer: it omits `binding` when it is zero and
    // `doc` when it is empty.
    out.write_all(b"{\"captured\":{")?;
    if binding != 0 {
        out.write_all(b"\"binding\":")?;
        CompactFormatter.write_u32(out, binding)?;
    }
    if !doc_json.is_empty() {
        if binding != 0 {
            out.write_all(b",")?;
        }
        out.write_all(b"\"doc\":")?;
        out.write_all(doc_json)?;
    }
    out.write_all(b"}}\n")
        .context("writing captured response")?;
    Ok(())
}

async fn do_validate(req: Validate) -> Result<Vec<ValidatedBinding>> {
    let config: EndpointConfig = serde_json::from_slice(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    consumer
        .fetch_metadata(None, KAFKA_METADATA_TIMEOUT)
        .context("Could not connect to bootstrap server with the provided configuration. This may be due to an incorrect configuration for authentication or bootstrap servers. Double check your configuration and try again.")?;

    match config.schema_registry {
        SchemaRegistryConfig::ConfluentSchemaRegistry {
            endpoint,
            username,
            password,
        } => {
            let client = SchemaRegistryClient::new(endpoint, username, password);
            client
                .schemas_for_topics(&[])
                .await
                .context("Could not connect to the configured schema registry. Double check your configuration and try again.")?;
        }
        SchemaRegistryConfig::NoSchemaRegistry { .. } => (),
    };

    req.bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_slice(&binding.resource_config_json)?;
            Ok(ValidatedBinding {
                resource_path: vec![res.topic],
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_flow::capture::response::Captured;

    fn via_response(binding: u32, doc_json: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_capture_response(
            Response {
                captured: Some(Captured {
                    binding,
                    doc_json: doc_json.to_vec().into(),
                }),
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        out
    }

    fn via_raw(binding: u32, doc_json: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_captured(binding, doc_json, &mut out).unwrap();
        out
    }

    /// Documents shaped to trip a hand-written envelope: braces and brackets
    /// inside strings, escapes, non-ASCII, a non-object value, and a large one.
    fn documents() -> Vec<Vec<u8>> {
        let big = format!(
            r#"{{"body":"{}","_meta":{{"offset":1}}}}"#,
            "x".repeat(2 * 1024 * 1024)
        );
        vec![
            br#"{}"#.to_vec(),
            br#"{"_meta":{"topic":"t","partition":0,"offset":0,"op":"u"}}"#.to_vec(),
            br#"{"s":"}]}\",\\ \u0000 \n","n":[1,2,{"k":"]"}],"_meta":{}}"#.to_vec(),
            "{\"s\":\"héllo ☃ 🦀\",\"_meta\":{}}".as_bytes().to_vec(),
            br#"{"f":1e-45,"g":-0.0,"h":"18446744073709551615","_meta":{}}"#.to_vec(),
            br#"null"#.to_vec(),
            br#""a string document""#.to_vec(),
            big.into_bytes(),
        ]
    }

    #[test]
    fn captured_matches_response_serialization() {
        for doc in documents() {
            for binding in [0u32, 1, 3, u32::MAX] {
                assert_eq!(
                    String::from_utf8_lossy(&via_raw(binding, &doc)),
                    String::from_utf8_lossy(&via_response(binding, &doc)),
                    "binding {binding}, doc {}",
                    String::from_utf8_lossy(&doc[..doc.len().min(60)]),
                );
            }
        }
        // The generated serializer omits an empty doc as well.
        assert_eq!(via_raw(0, b""), via_response(0, b""));
        assert_eq!(via_raw(7, b""), via_response(7, b""));
    }

    /// The runtime's own decoder must read back exactly what was written.
    #[test]
    fn captured_round_trips_through_the_response_decoder() {
        for doc in documents() {
            for binding in [0u32, 1, u32::MAX] {
                let line = via_raw(binding, &doc);
                let response: Response = serde_json::from_slice(&line).unwrap();
                let captured = response.captured.expect("a captured response");
                assert_eq!(captured.binding, binding);
                assert_eq!(captured.doc_json.as_ref(), doc.as_slice());
            }
        }
    }

    #[test]
    fn captured_does_not_flush() {
        struct CountFlushes(usize);
        impl Write for CountFlushes {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut out = CountFlushes(0);
        write_captured(1, br#"{"a":1}"#, &mut out).unwrap();
        assert_eq!(out.0, 0, "captured documents must not flush");
    }
}
