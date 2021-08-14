use serde_json::Value;

pub(crate) fn assert_valid_json(output: &[u8]) {
    serde_json::from_reader::<_, Value>(&output[..]).expect("output to be valid json");
}

pub(crate) fn assert_each_line_valid_json(output: &[u8]) {
    for line in std::str::from_utf8(&output).unwrap().lines() {
        assert_valid_json(&line.as_bytes());
    }
}

/// Verifies the output was empty. If this check fails, you get useful output to
/// help debug the error.
pub(crate) fn assert_empty(output: &[u8]) {
    assert_eq!("", std::str::from_utf8(output).unwrap());
}

pub(crate) fn mock_stdout() -> Box<Vec<u8>> {
    Box::new(Vec::new())
}
