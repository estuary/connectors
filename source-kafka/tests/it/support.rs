use serde_json::Value;

pub(crate) fn assert_valid_json(output: &[u8]) {
    serde_json::from_reader::<_, Value>(output).expect("output to be valid json");
}

pub(crate) fn mock_stdout() -> Vec<u8> {
    Vec::new()
}

pub(crate) fn parse_from_output(stdout: &[u8]) -> Value {
    let mut messages = parse_messages_from_output(stdout);
    assert_eq!(messages.len(), 1);
    messages.pop().unwrap()
}

pub(crate) fn parse_messages_from_output(stdout: &[u8]) -> Vec<Value> {
    std::str::from_utf8(stdout)
        .unwrap()
        .lines()
        .map(|doc| serde_json::from_str(doc).unwrap())
        .collect()
}
