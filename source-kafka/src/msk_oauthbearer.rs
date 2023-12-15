use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
use aws_sdk_iam::config::Credentials;
use aws_sigv4::http_request::{sign, SigningSettings, SignableBody, SignableRequest, SignatureLocation};
use aws_sigv4::sign::v4;
use http;
use std::time::{SystemTime, Duration};

const DEFAULT_EXPIRY_SECONDS: u64 = 900;
pub fn token(region: &str, access_key_id: &str, secret_access_key: &str) -> anyhow::Result::<(String, u128)> {
    let endpoint = format!("https://kafka.{}.amazonaws.com/?Action=kafka-cluster%3AConnect", region);
    let expiry_duration = Duration::new(DEFAULT_EXPIRY_SECONDS, 0);
    let now = SystemTime::now();

    // Set up information and settings for the signing
    let identity = Credentials::new(
        access_key_id,
        secret_access_key,
        None,
        None,
        "user credentials"
    ).into();
    let mut signing_settings = SigningSettings::default();
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(expiry_duration);

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("kafka-cluster")
        .time(now)
        .settings(signing_settings)
        .build()
        .unwrap()
        .into();

    // Convert the HTTP request into a signable request
    let signable_request = SignableRequest::new(
        "GET",
        &endpoint,
        std::iter::empty(),
        SignableBody::Bytes(&[])
    ).expect("signable request");

    let mut signed_req = http::Request::builder()
        .method("GET")
        .uri(&endpoint)
        .body("")
        .unwrap();

    // Sign and then apply the signature to the request
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();
    eprintln!("{:#?}, {:#?}", signing_instructions, _signature);
    signing_instructions.apply_to_request_http0x(&mut signed_req);

    let signed_url = format!("{}&User-Agent=EstuaryFlowCapture", signed_req.uri().to_string());

    eprintln!("signed request {:#?}", signed_url);
    let token = BASE64_URL_SAFE_NO_PAD.encode(signed_url);
    let expires_in = (now + expiry_duration).duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();

    return Ok((token, expires_in))
}
