use anyhow::Context;
use axum::http::HeaderMap;
use base64::prelude::*;
use bytes::Bytes;
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum WebhookSignatureConfig {
    #[serde(rename = "none")]
    None {},

    #[serde(rename = "twilio")]
    Twilio {
        #[serde(rename = "publicKey")]
        public_key: String,
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
    },
}

impl Default for WebhookSignatureConfig {
    fn default() -> Self {
        Self::None {}
    }
}

pub trait WebhookSignatureVerifier: Send + Sync {
    fn verify(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes, anyhow::Error>;
}

struct NoopVerifier;

impl WebhookSignatureVerifier for NoopVerifier {
    fn verify(&self, _: &HeaderMap, body: Bytes) -> Result<Bytes, anyhow::Error> {
        Ok(body)
    }
}

struct EcdsaVerifier {
    verifying_key: VerifyingKey,
    signature_header: String,
    timestamp_header: Option<String>,
}

impl EcdsaVerifier {
    pub fn new(
        public_key: &str,
        signature_header: String,
        timestamp_header: Option<String>,
    ) -> anyhow::Result<Self> {
        if signature_header.trim().is_empty() {
            anyhow::bail!("Signature header name is required but was empty");
        }
        if let Some(ref ts) = timestamp_header {
            if ts.trim().is_empty() {
                anyhow::bail!("Timestamp header name was provided but was empty");
            }
        }

        let verifying_key = public_key
            .parse::<VerifyingKey>()
            .context("parsing ECDSA public key from PEM")?;

        Ok(Self {
            verifying_key,
            signature_header,
            timestamp_header,
        })
    }
}

impl WebhookSignatureVerifier for EcdsaVerifier {
    fn verify(&self, headers: &HeaderMap, body: Bytes) -> Result<Bytes, anyhow::Error> {
        let signature = {
            let b64 = headers.get(&self.signature_header).context(format!(
                "A signature value was expected at header {} but was not found",
                self.signature_header,
            ))?;
            let bytes = BASE64_STANDARD.decode(b64).context(format!(
                "Failed to decode base64 signature from header '{}'",
                self.signature_header
            ))?;
            Signature::from_der(&bytes).context(format!(
                "Failed to parse DER-encoded ECDSA signature from header '{}'",
                self.signature_header
            ))?
        };

        let payload = {
            let mut value = Vec::new();

            if let Some(key) = &self.timestamp_header {
                let timestamp = headers.get(key).context(format!(
                    "A timestamp was expected at header {} but was not found",
                    key,
                ))?;

                value.extend(timestamp.as_bytes());
            }

            value.extend(&body);

            value
        };

        self.verifying_key.verify(&payload, &signature).context(
            "Signature verification failed. Check that the public key matches your webhook provider's key",
        )?;

        Ok(body)
    }
}

impl TryFrom<WebhookSignatureConfig> for Box<dyn WebhookSignatureVerifier> {
    type Error = anyhow::Error;

    fn try_from(config: WebhookSignatureConfig) -> Result<Self, Self::Error> {
        match config {
            WebhookSignatureConfig::None {} => Ok(Box::new(NoopVerifier {})),
            WebhookSignatureConfig::Twilio { public_key } => Ok(Box::new(EcdsaVerifier::new(
                &public_key,
                TWILIO_SETTINGS.signature_header.to_string(),
                TWILIO_SETTINGS.timestamp_header.map(String::from),
            )?)),
            WebhookSignatureConfig::Custom {
                algorithm,
                public_key,
                signature_header,
                timestamp_header,
            } => match algorithm {
                SignatureAlgorithm::Ecdsa => Ok(Box::new(EcdsaVerifier::new(
                    &public_key,
                    signature_header,
                    timestamp_header,
                )?)),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
        Box::new(
            EcdsaVerifier::new(
                &test_verifying_key_pem(),
                "X-Signature".to_string(),
                if with_timestamp {
                    Some("X-Timestamp".to_string())
                } else {
                    None
                },
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_ecdsa_signature_valid() {
        let body = b"test body content";
        let timestamp = b"1234567890";

        let signature = {
            let mut payload = Vec::new();
            payload.extend(timestamp);
            payload.extend(body);

            sign_payload(&payload)
        };

        let headers = {
            let mut val = axum::http::HeaderMap::new();
            val.insert("X-Signature", signature.parse().unwrap());
            val.insert("X-Timestamp", "1234567890".parse().unwrap());

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
        assert!(result
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains("signature"));
    }

    #[test]
    fn test_ecdsa_signature_missing_header() {
        let body = b"test body content";
        let headers = axum::http::HeaderMap::new();

        let verifier = make_ecdsa_verifier(false);
        let result = verifier.verify(&headers, Bytes::from_static(body));

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("X-Signature"));
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
        assert!(result.unwrap_err().to_string().contains("X-Timestamp"));
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

        let err = result.unwrap_err().to_string().to_lowercase();

        assert!(err.contains("invalid") || err.contains("base64"));
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
    }

    #[test]
    fn test_twilio_config_conversion() {
        let pem_key = test_verifying_key_pem();
        let config = WebhookSignatureConfig::Twilio {
            public_key: pem_key,
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
}
