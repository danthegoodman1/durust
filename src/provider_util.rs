//! Pure, storage-agnostic helpers shared by the SQLite, Postgres, and in-memory
//! providers. These carry no transaction or connection state, so keeping a single
//! copy prevents the providers' retry, timeout, codec, and metadata encodings from
//! silently diverging.

use crate::{
    ActivityId, ActivityTask, CodecId, CompressionId, EncryptionMetadata, Error, Result,
    TimestampMs,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn codec_to_str(codec: CodecId) -> &'static str {
    match codec {
        CodecId::MessagePack => "messagepack",
        CodecId::Json => "json",
    }
}

pub(crate) fn codec_from_str(value: &str) -> Result<CodecId> {
    match value {
        "messagepack" => Ok(CodecId::MessagePack),
        "json" => Ok(CodecId::Json),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload codec `{other}`"
        ))),
    }
}

pub(crate) fn compression_to_str(compression: CompressionId) -> &'static str {
    match compression {
        CompressionId::None => "none",
    }
}

pub(crate) fn compression_from_str(value: &str) -> Result<CompressionId> {
    match value {
        "none" => Ok(CompressionId::None),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload compression `{other}`"
        ))),
    }
}

pub(crate) fn encode_encryption_metadata(
    encryption: &Option<EncryptionMetadata>,
) -> Result<Option<Vec<u8>>> {
    encryption
        .as_ref()
        .map(|metadata| {
            rmp_serde::to_vec_named(metadata).map_err(|err| Error::PayloadEncode(err.to_string()))
        })
        .transpose()
}

pub(crate) fn decode_encryption_metadata(blob: Option<Vec<u8>>) -> Result<Option<EncryptionMetadata>> {
    blob.map(|blob| {
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .transpose()
}

pub(crate) fn timeout_message(activity_id: &ActivityId, attempt: u32, heartbeat: bool) -> String {
    if heartbeat {
        format!(
            "activity `{}` missed heartbeat on attempt {}",
            activity_id.0,
            attempt.max(1)
        )
    } else {
        format!(
            "activity `{}` timed out on attempt {}",
            activity_id.0,
            attempt.max(1)
        )
    }
}

pub(crate) fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
}

pub(crate) fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

pub(crate) fn unix_epoch_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

pub(crate) fn ready_at_ms_for_delay(delay: Duration) -> i64 {
    if delay.is_zero() {
        0
    } else {
        unix_epoch_millis().saturating_add(duration_millis_i64(delay))
    }
}

pub(crate) fn activity_timeout_at_ms(timeout: Option<Duration>) -> Option<i64> {
    activity_timeout_at_ms_from(TimestampMs(unix_epoch_millis()), timeout)
}

pub(crate) fn activity_timeout_at_ms_from(now: TimestampMs, timeout: Option<Duration>) -> Option<i64> {
    timeout.map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption_metadata_round_trips_through_the_provider_codec() {
        // The encryption column is spec'd forward-compatibility metadata. Exercise the
        // non-null branch end to end so the `Some` path the providers persist is covered,
        // not just the `None` default the rest of the suite uses.
        let metadata = EncryptionMetadata {
            key_id: "kms://durust/test-key/1".to_owned(),
        };
        let encoded = encode_encryption_metadata(&Some(metadata.clone()))
            .expect("encryption metadata should encode")
            .expect("Some metadata should produce a stored blob");
        let decoded =
            decode_encryption_metadata(Some(encoded)).expect("encryption metadata should decode");
        assert_eq!(decoded, Some(metadata));

        assert_eq!(encode_encryption_metadata(&None).unwrap(), None);
        assert_eq!(decode_encryption_metadata(None).unwrap(), None);
    }

    #[test]
    fn codec_and_compression_strings_round_trip() {
        for codec in [CodecId::MessagePack, CodecId::Json] {
            assert_eq!(codec_from_str(codec_to_str(codec)).unwrap(), codec);
        }
        // The removed protobuf codec must now be rejected rather than silently decoded.
        assert!(codec_from_str("protobuf").is_err());
        assert_eq!(
            compression_from_str(compression_to_str(CompressionId::None)).unwrap(),
            CompressionId::None
        );
    }
}
