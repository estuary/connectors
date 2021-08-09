use structopt::StructOpt;

use source_kafka::connector::Command;

fn main() -> color_eyre::Result<()> {
    let cmd = Command::from_args();
    source_kafka::run(cmd)
}
