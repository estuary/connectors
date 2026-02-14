use std::str;

use anyhow::{bail, Context, Result};
use axum::http::HeaderMap;
use base64::prelude::*;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SignatureAlgorithm {
    #[serde(rename = "ecdsa")]
    Ecdsa,
}

#[derive(Debug, Clone, Copy)]
pub struct EcdsaProviderSettings<'a> {
    pub signature_header: &'a str,
    pub timestamp_header: Option<&'a str>,
}

const TWILIO_SETTINGS: EcdsaProviderSettings = EcdsaProviderSettings {
    signature_header: "X-Twilio-Email-Event-Webhook-Signature",
    timestamp_header: Some("X-Twilio-Email-Event-Webhook-Timestamp"),
};

fn default_max_signature_age() -> u64 {
    300
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum WebhookSignatureConfig {
    #[serde(rename = "none")]
    None {},

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
        #[serde(default, rename = "timestampHeader")]
        timestamp_header: Option<String>,
        #[serde(default = "default_max_signature_age", rename = "maxSignatureAge")]
        max_signature_age: u64,
    },
}

impl Default for WebhookSignatureConfig {
    fn default() -> Self {
        Self::None {}
    }
}

pub trait WebhookSignatureVerifier: Send + Sync {
    fn verify(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes>;
}

struct NoopVerifier;

impl WebhookSignatureVerifier for NoopVerifier {
    fn verify(&self, _: &HeaderMap, body: Bytes) -> Result<Bytes> {
        Ok(body)
    }
}

struct TimestampParser {
    header: String,
    max_age: TimeDelta,
}

impl TimestampParser {
    fn new(header: String, max_age_seconds: u64) -> Result<Self> {
        if header.trim().is_empty() {
            anyhow::bail!("Timestamp header name was provided but was empty");
        }
        Ok(Self {
            header,
            max_age: TimeDelta::seconds(max_age_seconds as i64),
        })
    }

    fn as_bytes<'a>(&self, header_map: &'a HeaderMap) -> Result<&'a [u8]> {
        let val = header_map
            .get(&self.header)
            .with_context(|| {
                format!(
                    "A timestamp was expected at header {} but was not found",
                    &self.header
                )
            })?
            .as_bytes();

        Ok(val)
    }

    fn verify(&self, header_map: &HeaderMap) -> Result<()> {
        let ts_bytes = self.as_bytes(header_map)?;
        let ts_str = str::from_utf8(ts_bytes).context("Timestamp header is not valid UTF-8")?;
        let ts_unix: i64 = ts_str.parse().with_context(|| {
            format!(
                "Failed to parse timestamp header as integer: \"{}\"",
                ts_str
            )
        })?;
        let ts = DateTime::from_timestamp(ts_unix, 0)
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

struct EcdsaVerifier {
    verifying_key: VerifyingKey,
    signature_header: String,
    timestamp_parser: Option<TimestampParser>,
}

impl EcdsaVerifier {
    pub fn new(
        public_key: &str,
        signature_header: String,
        timestamp_parser: Option<TimestampParser>,
    ) -> anyhow::Result<Self> {
        if signature_header.trim().is_empty() {
            anyhow::bail!("Signature header name is required but was empty");
        }

        let verifying_key = public_key
            .parse::<VerifyingKey>()
            .context("parsing ECDSA public key from PEM")?;

        Ok(Self {
            verifying_key,
            signature_header,
            timestamp_parser,
        })
    }
}

impl WebhookSignatureVerifier for EcdsaVerifier {
    fn verify(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes> {
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

        let payload = {
            let mut value = Vec::new();

            if let Some(v) = &self.timestamp_parser {
                value.extend(v.as_bytes(headers)?);
            }
            value.extend(&body);

            value
        };

        self.verifying_key.verify(&payload, &signature).context(
            "Signature verification failed. Check that the public key matches your webhook provider's key",
        )?;

        // Verify timestamp freshness after the cryptographic signature check.
        // This prevents unauthenticated callers from probing the server's
        // max-age window via crafted timestamp headers.
        if let Some(v) = &self.timestamp_parser {
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

            WebhookSignatureConfig::Twilio {
                public_key,
                max_signature_age,
            } => {
                let timestamp_parser = TimestampParser::new(
                    // .unwrap() call is operating on a constant
                    TWILIO_SETTINGS.timestamp_header.unwrap().to_string(),
                    max_signature_age,
                )?;

                Ok(Box::new(EcdsaVerifier::new(
                    &public_key,
                    TWILIO_SETTINGS.signature_header.to_string(),
                    Some(timestamp_parser),
                )?))
            }

            WebhookSignatureConfig::Custom {
                algorithm,
                public_key,
                signature_header,
                timestamp_header,
                max_signature_age,
            } => match algorithm {
                SignatureAlgorithm::Ecdsa => {
                    let tv = timestamp_header
                        .map(|h| TimestampParser::new(h, max_signature_age))
                        .transpose()?;
                    Ok(Box::new(EcdsaVerifier::new(
                        &public_key,
                        signature_header,
                        tv,
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
        let tv = if with_timestamp {
            Some(TimestampParser::new("X-Timestamp".into(), default_max_signature_age()).unwrap())
        } else {
            None
        };
        Box::new(
            EcdsaVerifier::new(&test_verifying_key_pem(), "X-Signature".to_string(), tv).unwrap(),
        )
    }

    fn make_ecdsa_verifier_with_age(
        max_age_seconds: Option<u64>,
    ) -> Box<dyn WebhookSignatureVerifier> {
        let tv =
            max_age_seconds.map(|age| TimestampParser::new("X-Timestamp".into(), age).unwrap());
        Box::new(
            EcdsaVerifier::new(&test_verifying_key_pem(), "X-Signature".to_string(), tv).unwrap(),
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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
        let result = verifier.verify(&headers, Bytes::from_static(body));

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
            timestamp_header: Some("X-Custom-Ts".to_string()),
            max_signature_age: default_max_signature_age(),
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
                "timestampHeader": "X-Ts"
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
        let result = verifier.verify(&headers, Bytes::from_static(body));
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
        let result = verifier.verify(&headers, Bytes::from_static(body));
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
                "maxSignatureAge": 3600
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
}
