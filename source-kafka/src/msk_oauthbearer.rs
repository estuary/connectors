use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
use aws_sdk_iam::config::Credentials;
use aws_sigv4::http_request::{sign, SigningSettings, SignableBody, SignableRequest, SignatureLocation};
use aws_sigv4::sign::v4;
use http;
use std::time::{SystemTime, Duration};

/* Generate a token for Amazon Streaming Kafka service.
 * This is based on AWS V4: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
 *
 * This process works by us first preparing a draft request for connecting to kafka (action=kafka-cluster:Connect),
 * and then we use a library to generate the signature for it, which itself becomes part of the query parameters, so we end
 * up with a GET request url that includes X-Amz-Signature query parameter (along with other new
 * query parameters). Finally we add the user agent after the signature has been computed (this is in line with official sdks of AWS,
 * see: https://github.com/aws/aws-msk-iam-sasl-signer-go/blob/main/signer/msk_auth_token_provider.go#L188-L191
 *
 * For more detailed specifics on how the signature itself is calculated, see:
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
 *
 * The output is a url-safe base64 encoding of the GET request URL and an expiry timestamp
 * (milliseconds since Unix epoch)
 */

// Taken from the Go SDK's implementation
// https://github.com/aws/aws-msk-iam-sasl-signer-go/blob/main/signer/msk_auth_token_provider.go#L33
const DEFAULT_EXPIRY_SECONDS: u64 = 900;
pub fn token(region: &str, access_key_id: &str, secret_access_key: &str) -> eyre::Result::<(String, i64)> {
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

    // The default behaviour of the signing library is to put the signature in headers.
    // This ensures that the signature is placed as X-Amz-Signature in GET
    // query parameters instead. This is important because the token we use for OAuth Bearer
    // is the base64 encoding of a presigned GET URL.
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(expiry_duration);

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        // This is the constant name of the service for which we are signing the token
        .name("kafka-cluster")
        .time(now)
        .settings(signing_settings)
        .build()
        .unwrap()
        .into();

    // Prepare a signable HTTP request
    let signable_request = SignableRequest::new(
        "GET",
        &endpoint,
        std::iter::empty(),
        SignableBody::Bytes(&[])
    ).expect("signable request");

    // Create an empty draft HTTP request. The `sign` function provides us with a bunch of
    // "signing instructions" which we then apply to this draft HTTP request. The signing
    // instructions basically add a bunch of query parameters to this that have been computed to
    // this http request.
    let mut signed_req = http::Request::builder()
        .method("GET")
        .uri(&endpoint)
        .body("")
        .unwrap();

    // Sign and then apply the signature to the request
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();
    signing_instructions.apply_to_request_http0x(&mut signed_req);

    // Finally add User Agent to the final signed url. This is based on the Go SDK that does this
    // after signing: https://github.com/aws/aws-msk-iam-sasl-signer-go/blob/main/signer/msk_auth_token_provider.go#L188
    let signed_url = format!("{}&User-Agent=EstuaryFlowCapture", signed_req.uri().to_string());

    let token = BASE64_URL_SAFE_NO_PAD.encode(signed_url);
    let expires_in = (now + expiry_duration).duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();

    return Ok((token, expires_in.try_into()?))
}
