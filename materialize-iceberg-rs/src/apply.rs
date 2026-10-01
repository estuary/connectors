use anyhow::Result;
use proto_flow::materialize::request::Apply;

pub async fn do_apply(_req: Apply) -> Result<String> {
    Ok(String::new())
}
