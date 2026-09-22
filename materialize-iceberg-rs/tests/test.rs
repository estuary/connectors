use core::str;

#[test]
fn test_spec() {
    let output = std::process::Command::new("flowctl")
        .args([
            "raw",
            "spec",
            "--source",
            "tests/test.flow.yaml",
            "--name",
            "acmeCo/materialize-iceberg-rs",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let got: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    insta::assert_snapshot!(serde_json::to_string_pretty(&got).unwrap());
}

#[test]
fn test_materialization() {
    let output = std::process::Command::new("flowctl")
        .args([
            "raw",
            "preview-next",
            "--source",
            "tests/test.flow.yaml",
            "--fixture",
            "tests/fixture.json",
            "--name",
            "acmeCo/materialize-iceberg-rs",
        ])
        .output()
        .unwrap();

    println!("{}", str::from_utf8(&output.stderr).unwrap());
    assert!(output.status.success());
}
