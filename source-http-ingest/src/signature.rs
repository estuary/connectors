use std::str;

use anyhow::{bail, Context, Result};
use axum::http::HeaderMap;
use base64::prelude::*;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use hmac::{Hmac, KeyInit, Mac};
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SignatureAlgorithm {
    #[serde(rename = "ecdsa")]
    Ecdsa,
    #[serde(rename = "hmac_sha256")]
    HmacSha256,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SignatureEncoding {
    Hex,
    Base64,
}

impl Default for SignatureEncoding {
    fn default() -> Self {
        Self::Hex
    }
}

impl SignatureEncoding {
    fn decode(&self, s: &str) -> Result<Vec<u8>> {
        let s = s.trim();
        match self {
            Self::Hex => hex::decode(s).context("Failed to hex-decode signature"),
            Self::Base64 => BASE64_STANDARD
                .decode(s)
                .context("Failed to base64-decode signature"),
        }
    }
}

type HeaderExtractor = Box<dyn Fn(&str) -> Result<String> + Send + Sync>;

fn extract_identity() -> HeaderExtractor {
    Box::new(|raw| Ok(raw.to_string()))
}

fn extract_prefixed(prefix: &'static str) -> HeaderExtractor {
    Box::new(move |raw| {
        raw.trim()
            .strip_prefix(prefix)
            .map(str::to_string)
            .with_context(|| {
                format!(
                    "Header value did not start with expected prefix '{}'",
                    prefix
                )
            })
    })
}

fn extract_regex(pattern: &'static str) -> HeaderExtractor {
    let re = Regex::new(pattern).expect("invalid regex pattern");
    Box::new(move |raw| {
        let cap = re
            .captures(raw)
            .context("Regex did not match header value")?;
        let val = cap.get(1).context("Regex must contain a capture group")?;
        Ok(val.as_str().to_string())
    })
}

#[derive(Debug, Clone, Copy)]
enum TimestampUnit {
    Seconds,
    Milliseconds,
}

#[derive(Debug, Clone)]
enum TemplateSegment {
    Literal(String),
    Timestamp,
    Payload,
}

#[derive(Debug, Clone)]
struct SigningStringTemplate(Vec<TemplateSegment>);

impl SigningStringTemplate {
    fn render(&self, request_body: &[u8], timestamp: Option<&[u8]>) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(request_body.len() + 32);
        for seg in &self.0 {
            match seg {
                TemplateSegment::Literal(b) => out.extend_from_slice(b.as_bytes()),
                TemplateSegment::Payload => out.extend_from_slice(request_body),
                TemplateSegment::Timestamp => out.extend_from_slice(
                    timestamp.context("template requires timestamp but none provided")?,
                ),
            }
        }
        Ok(out)
    }
}

impl TryFrom<&str> for SigningStringTemplate {
    type Error = anyhow::Error;

    fn try_from(input: &str) -> Result<Self> {
        let regex = Regex::new(r"\{(PAYLOAD|TIMESTAMP)\}").unwrap();
        let mut segments = Vec::new();
        let mut cursor = 0;

        for cap in regex.captures_iter(input) {
            let m = cap.get(0).unwrap();
            if m.start() > cursor {
                segments.push(TemplateSegment::Literal(
                    input[cursor..m.start()].to_string(),
                ));
            }
            segments.push(match &cap[1] {
                "PAYLOAD" => TemplateSegment::Payload,
                "TIMESTAMP" => TemplateSegment::Timestamp,
                _ => unreachable!(),
            });
            cursor = m.end();
        }
        if cursor < input.len() {
            segments.push(TemplateSegment::Literal(input[cursor..].to_string()));
        }

        if !segments
            .iter()
            .any(|s| matches!(s, TemplateSegment::Payload))
        {
            bail!(
                "Hash message format must include at least {{PAYLOAD}}. \
                Supported placeholders: {{PAYLOAD}}, {{TIMESTAMP}}. \
                Ex: \"{{TIMESTAMP}}:{{PAYLOAD}}\""
            );
        }

        Ok(SigningStringTemplate(segments))
    }
}

fn default_max_signature_age() -> u64 {
    300
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum WebhookSignatureConfig {
    #[serde(rename = "none")]
    None {},

    #[serde(rename = "zoom")]
    Zoom {
        #[serde(rename = "publicKey")]
        public_key: String,
        #[serde(default = "default_max_signature_age", rename = "maxSignatureAge")]
        max_signature_age: u64,
    },

    #[serde(rename = "knock")]
    Knock {
        #[serde(rename = "publicKey")]
        public_key: String,
        #[serde(default = "default_max_signature_age", rename = "maxSignatureAge")]
        max_signature_age: u64,
    },

    #[serde(rename = "twilio")]
    Twilio {
        #[serde(rename = "publicKey")]
        public_key: String,
        #[serde(default = "default_max_signature_age", rename = "maxSignatureAge")]
        max_signature_age: u64,
    },

    #[serde(rename = "custom")]
    Custom {
        algorithm: SignatureAlgorithm,
        #[serde(rename = "publicKey")]
        public_key: String,
        #[serde(rename = "signatureHeader")]
        signature_header: String,
        #[serde(default, rename = "signatureEncoding")]
        signature_encoding: SignatureEncoding,
        #[serde(default, rename = "timestampHeader")]
        timestamp_header: Option<String>,
        #[serde(default = "default_max_signature_age", rename = "maxSignatureAge")]
        max_signature_age: u64,
        #[serde(rename = "signingStringTemplate")]
        template: String,
    },
}

impl Default for WebhookSignatureConfig {
    fn default() -> Self {
        Self::None {}
    }
}

pub trait WebhookSignatureVerifier: Send + Sync {
    fn verify_request(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes>;
}

struct NoopVerifier;

impl WebhookSignatureVerifier for NoopVerifier {
    fn verify_request(&self, _: &HeaderMap, body: Bytes) -> Result<Bytes> {
        Ok(body)
    }
}

/// Utility to protect against replay attacks.
struct TimestampVerifier {
    header: String,
    extract: HeaderExtractor,
    unit: TimestampUnit,
    max_age: TimeDelta,
}

impl std::fmt::Debug for TimestampVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampVerifier")
            .field("header", &self.header)
            .field("unit", &self.unit)
            .field("max_age", &self.max_age)
            .finish_non_exhaustive()
    }
}

impl TimestampVerifier {
    fn new(header: String, max_age_seconds: u64) -> Result<Self> {
        Self::with_options(
            header,
            max_age_seconds,
            extract_identity(),
            TimestampUnit::Seconds,
        )
    }

    fn with_options(
        header: String,
        max_age_seconds: u64,
        extract: HeaderExtractor,
        unit: TimestampUnit,
    ) -> Result<Self> {
        if header.trim().is_empty() {
            anyhow::bail!("Timestamp header name was provided but was empty");
        }
        Ok(Self {
            header,
            extract,
            unit,
            max_age: TimeDelta::seconds(max_age_seconds as i64),
        })
    }

    fn value(&self, header_map: &HeaderMap) -> Result<String> {
        let raw = header_map
            .get(&self.header)
            .with_context(|| {
                format!(
                    "A timestamp was expected at header {} but was not found",
                    &self.header
                )
            })?
            .to_str()
            .context("Timestamp header is not valid UTF-8")?;
        (self.extract)(raw)
    }

    fn verify(&self, header_map: &HeaderMap) -> Result<()> {
        let ts_str = self.value(header_map)?;
        let ts_unix: i64 = ts_str.parse().with_context(|| {
            format!(
                "Failed to parse timestamp header as integer: \"{}\"",
                ts_str
            )
        })?;
        let ts = match self.unit {
            TimestampUnit::Seconds => DateTime::from_timestamp(ts_unix, 0),
            TimestampUnit::Milliseconds => DateTime::from_timestamp_millis(ts_unix),
        }
        .with_context(|| format!("Invalid Unix timestamp: \"{}\"", ts_unix))?;
        let age = Utc::now().signed_duration_since(ts);

        if age > self.max_age {
            bail!("Signature expired. Signed at: {}", ts)
        }
        if age < -TimeDelta::seconds(15) {
            bail!("Signature is in the future. Signed at: {}", ts)
        }

        Ok(())
    }
}

#[derive(Debug)]
struct EcdsaVerifier {
    verifying_key: VerifyingKey,
    signature_header: String,
    timestamp_verifier: Option<TimestampVerifier>,
    template: SigningStringTemplate,
}

impl EcdsaVerifier {
    pub fn new(
        public_key: &str,
        signature_header: String,
        timestamp_verifier: Option<TimestampVerifier>,
        template: &str,
    ) -> anyhow::Result<Self> {
        let public_key = public_key.trim();
        if public_key.is_empty() {
            anyhow::bail!("Public key is required but was empty");
        }

        if signature_header.trim().is_empty() {
            anyhow::bail!("Signature header name is required but was empty");
        }

        let template = SigningStringTemplate::try_from(template)?;

        if timestamp_verifier.is_none()
            && template
                .0
                .iter()
                .any(|s| matches!(s, TemplateSegment::Timestamp))
        {
            anyhow::bail!(
                "{{TIMESTAMP}} placeholder was defined in hashed message format \
                but no timestamp parsing details were included"
            )
        }

        let verifying_key = {
            if public_key.starts_with("-----BEGIN PUBLIC KEY") {
                public_key
                    .parse::<VerifyingKey>()
                    .context("Failed to parse PEM as a P-256 ECDSA public key in SPKI format")?
            } else if public_key.starts_with("-----BEGIN") {
                anyhow::bail!(
                    "Unsupported PEM type. Only '-----BEGIN PUBLIC KEY-----' (SPKI) format is accepted"
                );
            } else {
                let bytes = BASE64_STANDARD.decode(public_key).context(
                    "Public key is not in PEM format and could not be decoded as base64. \
                     Accepted formats: PEM ('-----BEGIN PUBLIC KEY-----') or base64-encoded SPKI DER",
                )?;

                VerifyingKey::from_public_key_der(&bytes).context(
                    "Failed to parse base64-decoded bytes as a P-256 ECDSA public key in SPKI DER format",
                )?
            }
        };

        Ok(Self {
            verifying_key,
            signature_header,
            timestamp_verifier,
            template,
        })
    }
}

impl WebhookSignatureVerifier for EcdsaVerifier {
    fn verify_request(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes> {
        let signature = {
            let b64 = headers.get(&self.signature_header).with_context(|| {
                format!(
                    "A signature value was expected at header {} but was not found",
                    self.signature_header,
                )
            })?;
            let bytes = BASE64_STANDARD.decode(b64).with_context(|| {
                format!(
                    "Failed to decode base64 signature from header '{}'",
                    self.signature_header
                )
            })?;
            Signature::from_der(&bytes).with_context(|| {
                format!(
                    "Failed to parse DER-encoded ECDSA signature from header '{}'",
                    self.signature_header
                )
            })?
        };

        let ts_str = self
            .timestamp_verifier
            .as_ref()
            .map(|tv| tv.value(headers))
            .transpose()?;
        let payload = self
            .template
            .render(&body, ts_str.as_deref().map(str::as_bytes))
            .with_context(|| "Failed to build the hash value")?;

        self.verifying_key.verify(&payload, &signature).context(
            "Signature verification failed. Check that the public key matches your webhook provider's key",
        )?;

        // Verify timestamp freshness after the cryptographic signature check.
        // This prevents unauthenticated callers from probing the server's
        // max-age window via crafted timestamp headers.
        if let Some(v) = &self.timestamp_verifier {
            v.verify(headers)?;
        }

        Ok(body)
    }
}

struct HmacSha256Verifier {
    mac: Hmac<Sha256>,
    signature_header: String,
    signature_extractor: HeaderExtractor,
    signature_encoding: SignatureEncoding,
    timestamp_verifier: Option<TimestampVerifier>,
    template: SigningStringTemplate,
}

impl std::fmt::Debug for HmacSha256Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacSha256Verifier")
            .field("signature_header", &self.signature_header)
            .field("timestamp_verifier", &self.timestamp_verifier)
            .field("template", &self.template)
            .finish_non_exhaustive()
    }
}

impl HmacSha256Verifier {
    pub fn new(
        key: &str,
        signature_header: String,
        signature_extractor: HeaderExtractor,
        signature_encoding: SignatureEncoding,
        timestamp_verifier: Option<TimestampVerifier>,
        template: &str,
    ) -> anyhow::Result<Self> {
        if signature_header.trim().is_empty() {
            anyhow::bail!("Signature header name is required but was empty");
        }

        let key = key.trim();
        if key.is_empty() {
            anyhow::bail!("Secret key is required but was empty");
        }

        let template: SigningStringTemplate = template.try_into()?;

        if timestamp_verifier.is_none()
            && template
                .0
                .iter()
                .any(|s| matches!(s, TemplateSegment::Timestamp))
        {
            anyhow::bail!(
                "{{TIMESTAMP}} placeholder was defined in hashed message format \
                but no timestamp parsing details were included"
            )
        }

        let mac: Hmac<Sha256> = Hmac::new_from_slice(key.as_bytes())?;

        Ok(Self {
            mac,
            signature_header,
            signature_extractor,
            signature_encoding,
            timestamp_verifier,
            template,
        })
    }
}

impl WebhookSignatureVerifier for HmacSha256Verifier {
    fn verify_request(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes> {
        let raw = headers
            .get(&self.signature_header)
            .with_context(|| {
                format!(
                    "A signature value was expected at header {} but was not found",
                    self.signature_header,
                )
            })?
            .to_str()
            .with_context(|| {
                format!(
                    "Signature header '{}' is not valid UTF-8",
                    self.signature_header
                )
            })?;

        let extracted = (self.signature_extractor)(raw)?;
        let sig_bytes = self.signature_encoding.decode(&extracted)?;

        let ts_str = self
            .timestamp_verifier
            .as_ref()
            .map(|tv| tv.value(headers))
            .transpose()?;
        let signed = self
            .template
            .render(&body, ts_str.as_deref().map(str::as_bytes))
            .with_context(|| "Failed to build the hash value")?;

        let mut mac = self.mac.clone();
        mac.update(&signed);
        mac.verify_slice(&sig_bytes).context(
            "Signature verification failed. Check that the secret key matches your webhook provider's key",
        )?;

        // Verify timestamp freshness after the cryptographic signature check.
        // This prevents unauthenticated callers from probing the server's
        // max-age window via crafted timestamp headers.
        if let Some(v) = &self.timestamp_verifier {
            v.verify(headers)?;
        }

        Ok(body)
    }
}

impl TryFrom<WebhookSignatureConfig> for Box<dyn WebhookSignatureVerifier> {
    type Error = anyhow::Error;

    fn try_from(config: WebhookSignatureConfig) -> Result<Self> {
        match config {
            WebhookSignatureConfig::None {} => Ok(Box::new(NoopVerifier {})),

            WebhookSignatureConfig::Zoom {
                public_key,
                max_signature_age,
            } => {
                let timestamp_verifier = TimestampVerifier::new(
                    "x-zm-request-timestamp".to_string(),
                    max_signature_age,
                )?;

                Ok(Box::new(HmacSha256Verifier::new(
                    &public_key,
                    "x-zm-signature".to_string(),
                    extract_prefixed("v0="),
                    SignatureEncoding::Hex,
                    Some(timestamp_verifier),
                    "v0:{TIMESTAMP}:{PAYLOAD}",
                )?))
            }

            WebhookSignatureConfig::Knock {
                public_key,
                max_signature_age,
            } => {
                let timestamp_verifier = TimestampVerifier::with_options(
                    "x-knock-signature".to_string(),
                    max_signature_age,
                    extract_regex(r"(?:^|,)t=([^,]+)"),
                    TimestampUnit::Milliseconds,
                )?;

                Ok(Box::new(HmacSha256Verifier::new(
                    &public_key,
                    "x-knock-signature".to_string(),
                    extract_regex(r"(?:^|,)s=([^,]+)"),
                    SignatureEncoding::Base64,
                    Some(timestamp_verifier),
                    "{TIMESTAMP}.{PAYLOAD}",
                )?))
            }

            WebhookSignatureConfig::Twilio {
                public_key,
                max_signature_age,
            } => {
                let timestamp_verifier = TimestampVerifier::new(
                    "X-Twilio-Email-Event-Webhook-Timestamp".to_string(),
                    max_signature_age,
                )?;

                Ok(Box::new(EcdsaVerifier::new(
                    &public_key,
                    "X-Twilio-Email-Event-Webhook-Signature".to_string(),
                    Some(timestamp_verifier),
                    "{TIMESTAMP}{PAYLOAD}",
                )?))
            }

            WebhookSignatureConfig::Custom {
                algorithm,
                public_key,
                signature_header,
                signature_encoding,
                timestamp_header,
                max_signature_age,
                template,
            } => match algorithm {
                SignatureAlgorithm::Ecdsa => {
                    let tv = timestamp_header
                        .map(|h| TimestampVerifier::new(h, max_signature_age))
                        .transpose()?;
                    Ok(Box::new(EcdsaVerifier::new(
                        &public_key,
                        signature_header,
                        tv,
                        &template,
                    )?))
                }
                SignatureAlgorithm::HmacSha256 => {
                    let tv = timestamp_header
                        .map(|h| TimestampVerifier::new(h, max_signature_age))
                        .transpose()?;
                    Ok(Box::new(HmacSha256Verifier::new(
                        &public_key,
                        signature_header,
                        extract_identity(),
                        signature_encoding,
                        tv,
                        &template,
                    )?))
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use insta::assert_snapshot;
    use p256::pkcs8::EncodePublicKey;

    const TEST_PRIVATE_KEY_B64: &str = "eis1jT5PECEyQ1RldoeYqa7L3O3+DxEiM0RVZneImao=";

    fn test_signing_key() -> p256::ecdsa::SigningKey {
        let bytes = BASE64_STANDARD
            .decode(TEST_PRIVATE_KEY_B64)
            .expect("invalid test key base64");
        p256::ecdsa::SigningKey::from_slice(&bytes).expect("failed to create signing key")
    }

    fn test_verifying_key() -> VerifyingKey {
        *test_signing_key().verifying_key()
    }

    /// Helper to create a signed payload for testing
    fn sign_payload(payload: &[u8]) -> String {
        use p256::ecdsa::signature::Signer;

        let signature: Signature = test_signing_key().sign(payload);
        BASE64_STANDARD.encode(signature.to_der())
    }

    fn test_verifying_key_pem() -> String {
        use p256::pkcs8::LineEnding;
        test_verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("failed to encode public key as PEM")
    }

    fn make_ecdsa_verifier(with_timestamp: bool) -> Box<dyn WebhookSignatureVerifier> {
        let (tv, template) = if with_timestamp {
            (
                Some(
                    TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age())
                        .unwrap(),
                ),
                "{TIMESTAMP}{PAYLOAD}",
            )
        } else {
            (None, "{PAYLOAD}")
        };
        Box::new(
            EcdsaVerifier::new(
                &test_verifying_key_pem(),
                "X-Signature".to_string(),
                tv,
                template,
            )
            .unwrap(),
        )
    }

    fn make_ecdsa_verifier_with_age(
        max_age_seconds: Option<u64>,
    ) -> Box<dyn WebhookSignatureVerifier> {
        let tv =
            max_age_seconds.map(|age| TimestampVerifier::new("X-Timestamp".into(), age).unwrap());
        let template = if tv.is_some() {
            "{TIMESTAMP}{PAYLOAD}"
        } else {
            "{PAYLOAD}"
        };
        Box::new(
            EcdsaVerifier::new(
                &test_verifying_key_pem(),
                "X-Signature".to_string(),
                tv,
                template,
            )
            .unwrap(),
        )
    }

    fn test_verifying_key_b64() -> String {
        let der = test_verifying_key()
            .to_public_key_der()
            .expect("failed to encode public key as DER");
        BASE64_STANDARD.encode(der.as_bytes())
    }

    fn make_ecdsa_verifier_b64(with_timestamp: bool) -> Box<dyn WebhookSignatureVerifier> {
        let (tv, template) = if with_timestamp {
            (
                Some(
                    TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age())
                        .unwrap(),
                ),
                "{TIMESTAMP}{PAYLOAD}",
            )
        } else {
            (None, "{PAYLOAD}")
        };
        Box::new(
            EcdsaVerifier::new(
                &test_verifying_key_b64(),
                "X-Signature".to_string(),
                tv,
                template,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_ecdsa_signature_valid() {
        let now = Utc::now().timestamp().to_string();
        let body = b"test body content";

        let signature = {
            let mut payload = Vec::new();
            payload.extend(now.as_bytes());
            payload.extend(body);
            sign_payload(&payload)
        };

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", now.parse().unwrap());
            val
        };

        let verifier = make_ecdsa_verifier(true);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), body);
    }

    #[test]
    fn test_ecdsa_signature_valid_no_timestamp() {
        let body = b"test body content";
        let signature = sign_payload(body);

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());

            val
        };

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), body);
    }

    #[test]
    fn test_ecdsa_signature_invalid() {
        let body = b"test body content";
        let wrong_body = b"different content";

        let signature = sign_payload(wrong_body);

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());

            val
        };

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature verification failed. Check that the public key matches your webhook provider's key"
        )
    }

    #[test]
    fn test_ecdsa_signature_missing_header() {
        let body = b"test body content";
        let headers = axum::http::HeaderMap::new();

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"A signature value was expected at header X-Signature but was not found"
        )
    }

    #[test]
    fn test_ecdsa_signature_missing_timestamp() {
        let body = b"test body content";
        let signature = sign_payload(body);

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());

            val
        };

        let verifier = make_ecdsa_verifier(true);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err().to_string(),
            @"A timestamp was expected at header X-Timestamp but was not found"
        )
    }

    #[test]
    fn test_ecdsa_signature_invalid_base64() {
        let body = b"test body content";

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", "not-valid-base64!!!".parse().unwrap());

            val
        };

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Failed to decode base64 signature from header 'X-Signature'"
        )
    }

    #[test]
    fn test_ecdsa_signature_invalid_der() {
        let body = b"test body content";

        let headers = {
            let invalid_der =
                BASE64_STANDARD.encode([0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01]);

            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", invalid_der.parse().unwrap());

            val
        };

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature verification failed. Check that the public key matches your webhook provider's key"
        )
    }

    #[test]
    fn test_twilio_config_conversion() {
        let pem_key = test_verifying_key_pem();
        let config = WebhookSignatureConfig::Twilio {
            public_key: pem_key,
            max_signature_age: default_max_signature_age(),
        };
        let verifier: Result<Box<dyn WebhookSignatureVerifier>, _> = config.try_into();
        assert!(verifier.is_ok());
    }

    #[test]
    fn test_custom_config_conversion() {
        let pem_key = test_verifying_key_pem();
        let config = WebhookSignatureConfig::Custom {
            algorithm: SignatureAlgorithm::Ecdsa,
            public_key: pem_key,
            signature_header: "X-Custom-Sig".to_string(),
            signature_encoding: SignatureEncoding::default(),
            timestamp_header: Some("X-Custom-Ts".to_string()),
            max_signature_age: default_max_signature_age(),
            template: "{TIMESTAMP}{PAYLOAD}".to_string(),
        };
        let verifier: Result<Box<dyn WebhookSignatureVerifier>, _> = config.try_into();
        assert!(verifier.is_ok());
    }

    #[test]
    fn test_twilio_config_json_deserialization() {
        let pem_key = test_verifying_key_pem();
        let json = format!(
            r#"{{"provider": "twilio", "publicKey": {}}}"#,
            serde_json::to_string(&pem_key).unwrap()
        );
        let config: WebhookSignatureConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(config, WebhookSignatureConfig::Twilio { .. }));
    }

    #[test]
    fn test_custom_config_json_deserialization() {
        let pem_key = test_verifying_key_pem();
        let json = format!(
            r#"{{
                "provider": "custom",
                "algorithm": "ecdsa",
                "publicKey": {},
                "signatureHeader": "X-Sig",
                "timestampHeader": "X-Ts",
                "signingStringTemplate": "{{TIMESTAMP}}.{{PAYLOAD}}"
            }}"#,
            serde_json::to_string(&pem_key).unwrap()
        );
        let config: WebhookSignatureConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(config, WebhookSignatureConfig::Custom { .. }));
    }

    #[test]
    fn test_ecdsa_signature_custom_max_age_accepts_recent() {
        let now = Utc::now().timestamp().to_string();
        let body = b"test body content";

        let signature = {
            let mut payload = Vec::new();
            payload.extend(now.as_bytes());
            payload.extend(body);
            sign_payload(&payload)
        };

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", now.parse().unwrap());
            val
        };

        let verifier = make_ecdsa_verifier_with_age(Some(3600));
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
    }

    #[test]
    fn test_ecdsa_signature_custom_max_age_rejects_old() {
        let old_timestamp = Utc
            .with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp()
            .to_string();
        let body = b"test body content";

        let signature = {
            let mut payload = Vec::new();
            payload.extend(old_timestamp.as_bytes());
            payload.extend(body);
            sign_payload(&payload)
        };

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", old_timestamp.parse().unwrap());
            val
        };

        let verifier = make_ecdsa_verifier_with_age(Some(1));
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature expired. Signed at: 2000-01-01 00:00:00 UTC"
        )
    }

    #[test]
    fn test_custom_config_json_deserialization_with_max_age() {
        let pem_key = test_verifying_key_pem();
        let json = format!(
            r#"{{
                "provider": "custom",
                "algorithm": "ecdsa",
                "publicKey": {},
                "signatureHeader": "X-Sig",
                "maxSignatureAge": 3600,
                "signingStringTemplate": "{{PAYLOAD}}"
            }}"#,
            serde_json::to_string(&pem_key).unwrap()
        );
        let config: WebhookSignatureConfig = serde_json::from_str(&json).unwrap();
        match config {
            WebhookSignatureConfig::Custom {
                max_signature_age, ..
            } => {
                assert_eq!(max_signature_age, 3600);
            }
            _ => panic!("expected Custom variant"),
        }
    }

    #[test]
    fn test_ecdsa_raw_b64_key_parses() {
        let result = EcdsaVerifier::new(
            &test_verifying_key_b64(),
            "X-Signature".to_string(),
            None,
            "{PAYLOAD}",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ecdsa_signature_valid_b64_key() {
        let body = b"test body content";
        let signature = sign_payload(body);

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_ecdsa_verifier_b64(false);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));

        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), body);
    }

    #[test]
    fn test_ecdsa_raw_b64_key_invalid_base64() {
        let result = EcdsaVerifier::new(
            "not-valid-base64",
            "X-Signature".to_string(),
            None,
            "{PAYLOAD}",
        );
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Public key is not in PEM format and could not be decoded as base64. Accepted formats: PEM ('-----BEGIN PUBLIC KEY-----') or base64-encoded SPKI DER"
        );
    }

    #[test]
    fn test_ecdsa_raw_b64_key_invalid_der() {
        let garbage_der = BASE64_STANDARD.encode(b"this is not valid DER");
        let result = EcdsaVerifier::new(&garbage_der, "X-Signature".to_string(), None, "{PAYLOAD}");
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Failed to parse base64-decoded bytes as a P-256 ECDSA public key in SPKI DER format"
        );
    }

    #[test]
    fn test_template_rejects_missing_payload() {
        let result = SigningStringTemplate::try_from("{TIMESTAMP}-only");
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @r#"Hash message format must include at least {PAYLOAD}. Supported placeholders: {PAYLOAD}, {TIMESTAMP}. Ex: "{TIMESTAMP}:{PAYLOAD}""#
        );
    }

    #[test]
    fn test_template_rejects_empty() {
        let result = SigningStringTemplate::try_from("");
        assert!(result.is_err());
    }

    #[test]
    fn test_template_rejects_literal_only() {
        let result = SigningStringTemplate::try_from("no placeholders here");
        assert!(result.is_err());
    }

    #[test]
    fn test_template_accepts_payload_only() {
        let template = SigningStringTemplate::try_from("{PAYLOAD}").unwrap();
        let built = template.render(b"hello", None).unwrap();
        assert_eq!(built, b"hello");
    }

    #[test]
    fn test_template_interleaves_literals() {
        let template = SigningStringTemplate::try_from("v1:{TIMESTAMP}.{PAYLOAD}").unwrap();
        let built = template.render(b"body", Some(b"12345")).unwrap();
        assert_eq!(built, b"v1:12345.body");
    }

    #[test]
    fn test_template_payload_before_timestamp() {
        let template = SigningStringTemplate::try_from("{PAYLOAD}|{TIMESTAMP}").unwrap();
        let built = template.render(b"body", Some(b"99")).unwrap();
        assert_eq!(built, b"body|99");
    }

    #[test]
    fn test_template_build_fails_when_timestamp_missing() {
        let template = SigningStringTemplate::try_from("{TIMESTAMP}{PAYLOAD}").unwrap();
        let result = template.render(b"body", None);
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"template requires timestamp but none provided"
        );
    }

    #[test]
    fn test_ecdsa_rejects_timestamp_placeholder_without_verifier() {
        let result = EcdsaVerifier::new(
            &test_verifying_key_pem(),
            "X-Signature".to_string(),
            None,
            "{TIMESTAMP}{PAYLOAD}",
        );
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"{TIMESTAMP} placeholder was defined in hashed message format but no timestamp parsing details were included"
        );
    }

    #[test]
    fn test_ecdsa_rejects_template_without_payload() {
        let tv = TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age()).unwrap();
        let result = EcdsaVerifier::new(
            &test_verifying_key_pem(),
            "X-Signature".to_string(),
            Some(tv),
            "{TIMESTAMP}-no-payload",
        );
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @r#"Hash message format must include at least {PAYLOAD}. Supported placeholders: {PAYLOAD}, {TIMESTAMP}. Ex: "{TIMESTAMP}:{PAYLOAD}""#
        );
    }

    #[test]
    fn test_ecdsa_verifies_with_custom_template() {
        let body = b"webhook payload";
        let timestamp = Utc::now().timestamp().to_string();

        let signature = {
            let mut payload = Vec::new();
            payload.extend_from_slice(b"v1:");
            payload.extend_from_slice(timestamp.as_bytes());
            payload.extend_from_slice(b".");
            payload.extend_from_slice(body);
            sign_payload(&payload)
        };

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", timestamp.parse().unwrap());
            val
        };

        let tv = TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age()).unwrap();
        let verifier = EcdsaVerifier::new(
            &test_verifying_key_pem(),
            "X-Signature".to_string(),
            Some(tv),
            "v1:{TIMESTAMP}.{PAYLOAD}",
        )
        .unwrap();

        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
    }

    const HMAC_SECRET: &[u8] = b"super-secret-key";

    fn hmac_sign(payload: &[u8]) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(HMAC_SECRET).unwrap();
        mac.update(payload);
        mac.finalize().into_bytes().to_vec()
    }

    fn make_hmac_verifier(
        with_timestamp: bool,
        extractor: HeaderExtractor,
        encoding: SignatureEncoding,
    ) -> Box<dyn WebhookSignatureVerifier> {
        let (tv, template) = if with_timestamp {
            (
                Some(
                    TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age())
                        .unwrap(),
                ),
                "{TIMESTAMP}{PAYLOAD}",
            )
        } else {
            (None, "{PAYLOAD}")
        };
        Box::new(
            HmacSha256Verifier::new(
                str::from_utf8(HMAC_SECRET).unwrap(),
                "X-Signature".to_string(),
                extractor,
                encoding,
                tv,
                template,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_hmac_signature_valid() {
        let now = Utc::now().timestamp().to_string();
        let body = b"test body content";

        let mut signed = Vec::new();
        signed.extend(now.as_bytes());
        signed.extend(body);
        let signature = hex::encode(hmac_sign(&signed));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", now.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(true, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), body);
    }

    #[test]
    fn test_hmac_signature_valid_no_timestamp() {
        let body = b"test body content";
        let signature = hex::encode(hmac_sign(body));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), body);
    }

    #[test]
    fn test_hmac_signature_valid_base64_encoding() {
        let body = b"test body content";
        let signature = BASE64_STANDARD.encode(hmac_sign(body));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Base64);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
    }

    #[test]
    fn test_hmac_signature_valid_with_prefix() {
        let body = b"test body content";
        let signature = format!("v0={}", hex::encode(hmac_sign(body)));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_prefixed("v0="), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
    }

    #[test]
    fn test_hmac_signature_prefix_missing() {
        let body = b"test body content";
        let signature = hex::encode(hmac_sign(body));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_prefixed("v0="), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Header value did not start with expected prefix 'v0='"
        );
    }

    #[test]
    fn test_hmac_signature_invalid() {
        let body = b"test body content";
        let wrong_body = b"different content";
        let signature = hex::encode(hmac_sign(wrong_body));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature verification failed. Check that the secret key matches your webhook provider's key"
        );
    }

    #[test]
    fn test_hmac_signature_missing_header() {
        let body = b"test body content";
        let headers = axum::http::HeaderMap::new();

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"A signature value was expected at header X-Signature but was not found"
        );
    }

    #[test]
    fn test_hmac_signature_missing_timestamp() {
        let body = b"test body content";
        let signature = hex::encode(hmac_sign(body));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(true, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err().to_string(),
            @"A timestamp was expected at header X-Timestamp but was not found"
        );
    }

    #[test]
    fn test_hmac_signature_invalid_hex() {
        let body = b"test body content";

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", "zz-not-hex".parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Failed to hex-decode signature"
        );
    }

    #[test]
    fn test_hmac_signature_invalid_base64() {
        let body = b"test body content";

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", "not-valid-base64!!!".parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(false, extract_identity(), SignatureEncoding::Base64);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Failed to base64-decode signature"
        );
    }

    #[test]
    fn test_hmac_signature_expired_timestamp() {
        let old_ts = Utc
            .with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp()
            .to_string();
        let body = b"test body content";

        let mut signed = Vec::new();
        signed.extend(old_ts.as_bytes());
        signed.extend(body);
        let signature = hex::encode(hmac_sign(&signed));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", old_ts.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(true, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature expired. Signed at: 2000-01-01 00:00:00 UTC"
        );
    }

    #[test]
    fn test_hmac_signature_future_timestamp() {
        let future_ts = Utc
            .with_ymd_and_hms(2099, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp()
            .to_string();
        let body = b"test body content";

        let mut signed = Vec::new();
        signed.extend(future_ts.as_bytes());
        signed.extend(body);
        let signature = hex::encode(hmac_sign(&signed));

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", future_ts.parse().unwrap());
            val
        };

        let verifier = make_hmac_verifier(true, extract_identity(), SignatureEncoding::Hex);
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature is in the future. Signed at: 2099-01-01 00:00:00 UTC"
        );
    }

    #[test]
    fn test_hmac_rejects_timestamp_placeholder_without_verifier() {
        let result = HmacSha256Verifier::new(
            str::from_utf8(HMAC_SECRET).unwrap(),
            "X-Signature".to_string(),
            extract_identity(),
            SignatureEncoding::Hex,
            None,
            "{TIMESTAMP}{PAYLOAD}",
        );
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"{TIMESTAMP} placeholder was defined in hashed message format but no timestamp parsing details were included"
        );
    }

    #[test]
    fn test_hmac_rejects_template_without_payload() {
        let tv = TimestampVerifier::new("X-Timestamp".into(), default_max_signature_age()).unwrap();
        let result = HmacSha256Verifier::new(
            str::from_utf8(HMAC_SECRET).unwrap(),
            "X-Signature".to_string(),
            extract_identity(),
            SignatureEncoding::Hex,
            Some(tv),
            "{TIMESTAMP}-no-payload",
        );
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @r#"Hash message format must include at least {PAYLOAD}. Supported placeholders: {PAYLOAD}, {TIMESTAMP}. Ex: "{TIMESTAMP}:{PAYLOAD}""#
        );
    }

    #[test]
    fn test_zoom_config_conversion_and_verifies() {
        let now = Utc::now().timestamp().to_string();
        let body = b"zoom event body";

        let signed = format!("v0:{}:{}", now, str::from_utf8(body).unwrap());
        let signature = format!("v0={}", hex::encode(hmac_sign(signed.as_bytes())));

        let config = WebhookSignatureConfig::Zoom {
            public_key: str::from_utf8(HMAC_SECRET).unwrap().to_string(),
            max_signature_age: default_max_signature_age(),
        };
        let verifier: Box<dyn WebhookSignatureVerifier> = config.try_into().unwrap();

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("x-zm-signature", signature.parse().unwrap());
            val.insert("x-zm-request-timestamp", now.parse().unwrap());
            val
        };
        let result = verifier.verify_request(&headers, Bytes::from_static(body));
        assert!(result.is_ok());
    }

    fn knock_signed_payload(ts: &str, body: &[u8]) -> String {
        format!("{}.{}", ts, str::from_utf8(body).unwrap())
    }

    #[test]
    fn test_knock_config_conversion_and_verifies() {
        let ts = Utc::now().timestamp_millis().to_string();
        let body = br#"{"event":"x"}"#;
        let sig = BASE64_STANDARD.encode(hmac_sign(knock_signed_payload(&ts, body).as_bytes()));
        let composite = format!("t={},s={}", ts, sig);

        let config = WebhookSignatureConfig::Knock {
            public_key: str::from_utf8(HMAC_SECRET).unwrap().to_string(),
            max_signature_age: default_max_signature_age(),
        };
        let verifier: Box<dyn WebhookSignatureVerifier> = config.try_into().unwrap();

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("x-knock-signature", composite.parse().unwrap());
            val
        };
        let result = verifier.verify_request(&headers, Bytes::copy_from_slice(body));
        assert!(result.is_ok());
    }

    #[test]
    fn test_knock_signature_expired_timestamp() {
        let ts = Utc
            .with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis()
            .to_string();
        let body = br#"{"event":"x"}"#;
        let sig = BASE64_STANDARD.encode(hmac_sign(knock_signed_payload(&ts, body).as_bytes()));
        let composite = format!("t={},s={}", ts, sig);

        let config = WebhookSignatureConfig::Knock {
            public_key: str::from_utf8(HMAC_SECRET).unwrap().to_string(),
            max_signature_age: default_max_signature_age(),
        };
        let verifier: Box<dyn WebhookSignatureVerifier> = config.try_into().unwrap();

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("x-knock-signature", composite.parse().unwrap());
            val
        };
        let result = verifier.verify_request(&headers, Bytes::copy_from_slice(body));
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Signature expired. Signed at: 2000-01-01 00:00:00 UTC"
        );
    }

    #[test]
    fn test_extract_regex_no_match() {
        let extractor = extract_regex(r"(?:^|,)t=([^,]+)");
        let result = (extractor)("s=abc,other=stuff");
        assert!(result.is_err());
        assert_snapshot!(
            result.unwrap_err(),
            @"Regex did not match header value"
        );
    }

    #[test]
    fn test_custom_hmac_config_json_deserialization_base64() {
        let json = r#"{
            "provider": "custom",
            "algorithm": "hmac_sha256",
            "publicKey": "shhh",
            "signatureHeader": "X-Sig",
            "signatureEncoding": "base64",
            "signingStringTemplate": "{PAYLOAD}"
        }"#;
        let config: WebhookSignatureConfig = serde_json::from_str(json).unwrap();
        match config {
            WebhookSignatureConfig::Custom {
                signature_encoding, ..
            } => {
                assert_eq!(signature_encoding, SignatureEncoding::Base64);
            }
            _ => panic!("expected Custom variant"),
        }
    }
}
