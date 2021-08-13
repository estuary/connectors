pub mod connector;

pub fn run(cmd: connector::Command) {
    println!("You have selected {:?}", cmd);
}
