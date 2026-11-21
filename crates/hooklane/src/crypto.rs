//! Small cryptography helpers: API keys, endpoint signing secrets, and
//! encryption of those secrets at rest.
//!
//! * **API keys** are 32 random bytes shown to the tenant exactly once. We
//!   store only their SHA-256 hash, so a database leak does not leak usable
//!   keys. A fast hash is fine here (unlike passwords) because the key is
//!   high-entropy random data: there is no dictionary to guess from.
//! * **Endpoint signing secrets** must be readable by the worker (it needs the
//!   raw bytes to compute HMACs), so they cannot be hashed. Instead they are
//!   encrypted with AES-256-GCM using a key that lives only in the environment
//!   (`SECRET_ENCRYPTION_KEY`). A database dump alone is then not enough to
//!   forge webhooks.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rand::RngExt;
use sha2::{Digest, Sha256};

/// Errors from this module. Kept deliberately vague: crypto errors should not
/// leak details.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("SECRET_ENCRYPTION_KEY must be base64 of exactly 32 bytes")]
    BadKey,
    #[error("could not encrypt secret")]
    Encrypt,
    #[error("could not decrypt secret (wrong SECRET_ENCRYPTION_KEY or corrupt data)")]
    Decrypt,
    #[error("signing secret is malformed")]
    BadSecret,
}

const API_KEY_PREFIX: &str = "hl_";
const SECRET_PREFIX: &str = "whsec_";
const NONCE_LEN: usize = 12;

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    rand::rng().fill(&mut bytes[..]);
    bytes
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

/// A freshly generated API key: the full key (shown once) and what we store.
pub struct NewApiKey {
    pub key: String,
    pub hash: Vec<u8>,
    pub prefix: String,
}

pub fn generate_api_key() -> NewApiKey {
    let key = format!(
        "{API_KEY_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(random_bytes::<32>())
    );
    NewApiKey {
        hash: hash_api_key(&key),
        prefix: key.chars().take(10).collect(),
        key,
    }
}

pub fn hash_api_key(key: &str) -> Vec<u8> {
    Sha256::digest(key.as_bytes()).to_vec()
}

// ---------------------------------------------------------------------------
// Endpoint signing secrets (Standard Webhooks format: "whsec_<base64>")
// ---------------------------------------------------------------------------

/// 32 random bytes. The string form is `whsec_` + base64 of these bytes.
pub fn generate_signing_secret() -> Vec<u8> {
    random_bytes::<32>().to_vec()
}

pub fn format_signing_secret(raw: &[u8]) -> String {
    format!("{SECRET_PREFIX}{}", STANDARD.encode(raw))
}

pub fn parse_signing_secret(s: &str) -> Result<Vec<u8>, CryptoError> {
    let b64 = s.strip_prefix(SECRET_PREFIX).unwrap_or(s);
    STANDARD.decode(b64).map_err(|_| CryptoError::BadSecret)
}

// ---------------------------------------------------------------------------
// Encryption at rest
// ---------------------------------------------------------------------------

/// Holds the AES-256-GCM cipher built from `SECRET_ENCRYPTION_KEY`.
#[derive(Clone)]
pub struct SecretBox {
    cipher: Aes256Gcm,
}

impl SecretBox {
    pub fn from_base64_key(key_b64: &str) -> Result<SecretBox, CryptoError> {
        let key = STANDARD
            .decode(key_b64.trim())
            .map_err(|_| CryptoError::BadKey)?;
        if key.len() != 32 {
            return Err(CryptoError::BadKey);
        }
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| CryptoError::BadKey)?;
        Ok(SecretBox { cipher })
    }

    /// Output layout: 12-byte random nonce followed by the ciphertext+tag.
    /// A fresh random nonce per encryption is essential: reusing a nonce with
    /// the same key breaks GCM's security completely.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let nonce_bytes = random_bytes::<NONCE_LEN>();
        let nonce = Nonce::try_from(&nonce_bytes[..]).map_err(|_| CryptoError::Encrypt)?;
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| CryptoError::Encrypt)?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if data.len() < NONCE_LEN {
            return Err(CryptoError::Decrypt);
        }
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let nonce = Nonce::try_from(nonce_bytes).map_err(|_| CryptoError::Decrypt)?;
        self.cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| CryptoError::Decrypt)
    }
}

/// Generate a new random encryption key, base64-encoded. Used by `make up` to
/// fill in `.env` on first run.
pub fn generate_encryption_key() -> String {
    STANDARD.encode(random_bytes::<32>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption_round_trip() {
        let sb = SecretBox::from_base64_key(&generate_encryption_key()).unwrap();
        let secret = generate_signing_secret();
        let enc = sb.encrypt(&secret).unwrap();
        assert_ne!(enc[NONCE_LEN..], secret[..]);
        assert_eq!(sb.decrypt(&enc).unwrap(), secret);
    }

    #[test]
    fn same_plaintext_encrypts_differently_each_time() {
        let sb = SecretBox::from_base64_key(&generate_encryption_key()).unwrap();
        assert_ne!(sb.encrypt(b"abc").unwrap(), sb.encrypt(b"abc").unwrap());
    }

    #[test]
    fn wrong_key_fails_to_decrypt() {
        let a = SecretBox::from_base64_key(&generate_encryption_key()).unwrap();
        let b = SecretBox::from_base64_key(&generate_encryption_key()).unwrap();
        assert!(b.decrypt(&a.encrypt(b"secret").unwrap()).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let sb = SecretBox::from_base64_key(&generate_encryption_key()).unwrap();
        let mut enc = sb.encrypt(b"secret").unwrap();
        let last = enc.len() - 1;
        enc[last] ^= 1;
        assert!(sb.decrypt(&enc).is_err());
    }

    #[test]
    fn bad_key_rejected() {
        assert!(SecretBox::from_base64_key("too-short").is_err());
        assert!(SecretBox::from_base64_key(&STANDARD.encode([0u8; 16])).is_err());
    }

    #[test]
    fn api_key_hash_is_stable_and_prefix_is_short() {
        let k = generate_api_key();
        assert!(k.key.starts_with("hl_"));
        assert_eq!(k.hash, hash_api_key(&k.key));
        assert_eq!(k.prefix.len(), 10);
    }

    #[test]
    fn secret_format_round_trip() {
        let raw = generate_signing_secret();
        let s = format_signing_secret(&raw);
        assert!(s.starts_with("whsec_"));
        assert_eq!(parse_signing_secret(&s).unwrap(), raw);
    }
}
