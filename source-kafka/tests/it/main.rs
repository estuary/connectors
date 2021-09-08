use std::fs::File;
use std::ops::RangeInclusive;

use serde_json::Value;
use source_kafka::catalog;
use source_kafka::configuration;
use source_kafka::connector::Connector;
use source_kafka::connector::ConnectorConfig;
use source_kafka::state;
use support::{assert_valid_json, mock_stdout};

use crate::support::{assert_each_line_valid_json, assert_empty};

mod support;

#[test]
fn spec_test() {
    let mut stdout = mock_stdout();

    source_kafka::KafkaConnector::spec(&mut stdout).expect("spec command to succeed");

    assert_valid_json(&stdout);
}

#[test]
fn check_test() {
    let mut stdout = mock_stdout();
    let config = local_config();

    source_kafka::KafkaConnector::check(&mut stdout, config).expect("check command to succeed");

    assert_valid_json(&stdout);
}

#[test]
fn discover_test() {
    let mut stdout = mock_stdout();
    let config = local_config();

    source_kafka::KafkaConnector::discover(&mut stdout, config)
        .expect("discover command to succeed");

    assert_valid_json(&stdout);
}

#[test]
fn read_empty_catalog_test() {
    let mut stdout = mock_stdout();
    let config = local_config();
    let catalog = catalog::ConfiguredCatalog::default();

    source_kafka::KafkaConnector::read(&mut stdout, config, catalog, None)
        .expect("read command to succeed");

    assert_empty(&stdout);
}

#[test]
fn read_simple_catalog_test() {
    let mut stdout = mock_stdout();
    let config = local_config();
    let catalog = local_catalog("todo-list", false, 0..=0xffffffff);

    source_kafka::KafkaConnector::read(&mut stdout, config, catalog, None)
        .expect("read command to succeed");

    assert_each_line_valid_json(&stdout);
}

#[test]
fn read_resume_from_state_test() {
    let mut stdout = mock_stdout();
    let config = local_config();
    let catalog = local_catalog("todo-list", false, 0xd0000000..=0xffffffff);

    let mut state = state::TopicSet::default();
    state.add_new(state::Topic::new(
        "todo-list",
        3,
        state::Offset::UpThrough(37),
    ));

    source_kafka::KafkaConnector::read(&mut stdout, config, catalog, Some(state))
        .expect("read command to succeed");

    let messages: Vec<Value> = std::str::from_utf8(&stdout)
        .unwrap()
        .lines()
        .filter(|line| line.find("STATE").is_none())
        .map(|doc| serde_json::from_str(doc).unwrap())
        .collect();

    eprintln!(
        "{}",
        serde_json::to_string_pretty(&messages.clone()).unwrap()
    );
    assert_eq!(2, messages.len());
}

fn local_config() -> configuration::Configuration {
    let file = File::open("tests/test-config.json").expect("to open test config file");
    configuration::Configuration::parse(file).expect("to parse test config file")
}

fn local_catalog(name: &str, tail: bool, range: RangeInclusive<u32>) -> catalog::ConfiguredCatalog {
    catalog::ConfiguredCatalog {
        streams: vec![catalog::ConfiguredStream {
            stream: catalog::Stream {
                name: name.to_owned(),
            },
        }],
        tail,
        range,
    }
}
