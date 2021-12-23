use std::io::Read;
use std::process::Child;

pub fn get_child_stdout(process: &mut Child) -> String {
    let mut stdout = String::new();
    process
        .stdout
        .take()
        .expect("missing stdout")
        .read_to_string(&mut stdout)
        .expect("fail in reading stdout");
    stdout
}

pub fn get_child_stderr(process: &mut Child) -> String {
    let mut stderr = String::new();
    process
        .stderr
        .take()
        .expect("missing stderr")
        .read_to_string(&mut stderr)
        .expect("fail in reading stderr");
    stderr
}
