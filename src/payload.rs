use crate::{Error, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecId {
    MessagePack,
    Json,
    Protobuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionId {
    None,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFingerprint(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionMetadata {
    pub key_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadRef {
    Inline {
        codec: CodecId,
        schema_fingerprint: SchemaFingerprint,
        compression: CompressionId,
        encryption: Option<EncryptionMetadata>,
        bytes: Vec<u8>,
    },
    Blob {
        codec: CodecId,
        schema_fingerprint: SchemaFingerprint,
        compression: CompressionId,
        encryption: Option<EncryptionMetadata>,
        digest: String,
        size: u64,
        uri: String,
    },
}

impl PayloadRef {
    pub fn inline_messagepack<T>(value: &T) -> Result<Self>
    where
        T: Serialize + ?Sized,
    {
        let bytes = rmp_serde::to_vec_named(value)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        Ok(Self::Inline {
            codec: CodecId::MessagePack,
            schema_fingerprint: SchemaFingerprint(type_fingerprint::<T>()),
            compression: CompressionId::None,
            encryption: None,
            bytes,
        })
    }

    pub fn decode_messagepack<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match self {
            PayloadRef::Inline {
                codec: CodecId::MessagePack,
                compression: CompressionId::None,
                encryption: None,
                bytes,
                ..
            } => rmp_serde::from_slice(bytes).map_err(|err| Error::PayloadDecode(err.to_string())),
            PayloadRef::Inline { codec, .. } => Err(Error::PayloadDecode(format!(
                "unsupported inline codec for MessagePack decode: {codec:?}"
            ))),
            PayloadRef::Blob { .. } => Err(Error::PayloadDecode(
                "blob payload hydration is not implemented in phase 0001".to_owned(),
            )),
        }
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            PayloadRef::Inline { bytes, .. } => bytes.len(),
            PayloadRef::Blob { size, .. } => (*size).try_into().unwrap_or(usize::MAX),
        }
    }
}

pub fn encode_payload<T>(value: &T) -> Result<PayloadRef>
where
    T: Serialize + ?Sized,
{
    PayloadRef::inline_messagepack(value)
}

pub fn decode_payload<T>(payload: &PayloadRef) -> Result<T>
where
    T: DeserializeOwned,
{
    payload.decode_messagepack()
}

pub fn type_fingerprint<T: ?Sized>() -> String {
    let mut hasher = Sha256::new();
    hasher.update(std::any::type_name::<T>().as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub fn payload_digest(payload: &PayloadRef) -> String {
    let mut hasher = Sha256::new();
    match payload {
        PayloadRef::Inline { bytes, .. } => hasher.update(bytes),
        PayloadRef::Blob { digest, .. } => hasher.update(digest.as_bytes()),
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}
