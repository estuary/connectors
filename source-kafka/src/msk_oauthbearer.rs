use reqwest::Url;
use base64::prelude::{Engine as _, BASE64_STANDARD};

const EXPIRES_KEY: &str = "X-Amz-Expires";
pub fn token(region: &str, access_key_id: &str, secret_access_key: &str) -> anyhow::Result::<(String, i64)> {
    let endpoint = format!("https://kafka.{}.amazonaws.com/", region);

    let mut url = Url::parse(&endpoint).unwrap();
    { 
        let mut query = url.query_pairs_mut();

        query.append_pair("Action", "kafka-cluster:Connect");
        query.append_pair(EXPIRES_KEY, "900");
    }

    let now = chrono::Utc::now();
    let headers = reqwest::header::HeaderMap::new();
    let s = aws_sign_v4::AwsSign::new(
        "GET",
        url.as_str(),
        &now,
        &headers,
        region,
        access_key_id,
        secret_access_key,
        "flow-kafka-connector",
        ""
    );
    let mut signed_url = Url::parse(&s.sign())?;

    {
        let mut signed_query = signed_url.query_pairs_mut();
        signed_query.append_pair("UserAgent", "Estuary Flow Kafka Connector");
    }

    let (_, expires_in_raw) = signed_url.query_pairs().find(|(key, _)| key == EXPIRES_KEY).unwrap();
    let expires_in = chrono::DateTime::parse_from_rfc3339(&expires_in_raw)?;

    return Ok((BASE64_STANDARD.encode(signed_url.as_str()), expires_in.timestamp_millis()));
}
