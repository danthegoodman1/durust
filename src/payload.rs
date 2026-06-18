use crate::{
    ActivityMapInputManifest, ActivityMapInputPage, ActivityMapResultManifest,
    ActivityMapResultPage, ActivityMapScheduled, ActivityScheduled, ActivityTask,
    ChildStartOutboxMessage, ChildWorkflowCompleted, ChildWorkflowFailed,
    ChildWorkflowMapCompleted, ChildWorkflowMapFailed, ChildWorkflowMapScheduled,
    ChildWorkflowStartRequested, DurableFailure, Error, HistoryEventData, Result, SideEffectMarker,
    SignalConsumed,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

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

pub const DEFAULT_INLINE_THRESHOLD_BYTES: usize = 8 * 1024;
pub const MAX_SIDE_EFFECT_PAYLOAD_BYTES: usize = DEFAULT_INLINE_THRESHOLD_BYTES;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadStorageConfig {
    pub codec: CodecId,
    pub inline_threshold_bytes: usize,
    pub blob_store: Option<BlobStoreConfig>,
}

impl Default for PayloadStorageConfig {
    fn default() -> Self {
        Self {
            codec: CodecId::MessagePack,
            inline_threshold_bytes: DEFAULT_INLINE_THRESHOLD_BYTES,
            blob_store: None,
        }
    }
}

impl PayloadStorageConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn codec(mut self, codec: CodecId) -> Self {
        self.codec = codec;
        self
    }

    pub fn inline_threshold_bytes(mut self, threshold: usize) -> Self {
        self.inline_threshold_bytes = threshold;
        self
    }

    pub fn blob_store(mut self, blob_store: BlobStoreConfig) -> Self {
        self.blob_store = Some(blob_store);
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobStoreConfig {
    LocalDirectory { root: PathBuf, prefix: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadBlob {
    pub codec: CodecId,
    pub schema_fingerprint: SchemaFingerprint,
    pub compression: CompressionId,
    pub encryption: Option<EncryptionMetadata>,
    pub bytes: Vec<u8>,
}

impl PayloadRef {
    pub fn codec(&self) -> CodecId {
        match self {
            PayloadRef::Inline { codec, .. } | PayloadRef::Blob { codec, .. } => *codec,
        }
    }

    pub fn inline_messagepack<T>(value: &T) -> Result<Self>
    where
        T: Serialize + ?Sized,
    {
        let bytes =
            rmp_serde::to_vec_named(value).map_err(|err| Error::PayloadEncode(err.to_string()))?;
        Ok(Self::Inline {
            codec: CodecId::MessagePack,
            schema_fingerprint: SchemaFingerprint(type_fingerprint::<T>()),
            compression: CompressionId::None,
            encryption: None,
            bytes,
        })
    }

    pub fn inline_json<T>(value: &T) -> Result<Self>
    where
        T: Serialize + ?Sized,
    {
        let bytes =
            serde_json::to_vec(value).map_err(|err| Error::PayloadEncode(err.to_string()))?;
        Ok(Self::Inline {
            codec: CodecId::Json,
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
                "blob payload must be hydrated by the durability provider before decode".to_owned(),
            )),
        }
    }

    pub fn decode_json<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match self {
            PayloadRef::Inline {
                codec: CodecId::Json,
                compression: CompressionId::None,
                encryption: None,
                bytes,
                ..
            } => serde_json::from_slice(bytes).map_err(|err| Error::PayloadDecode(err.to_string())),
            PayloadRef::Inline { codec, .. } => Err(Error::PayloadDecode(format!(
                "unsupported inline codec for JSON decode: {codec:?}"
            ))),
            PayloadRef::Blob { .. } => Err(Error::PayloadDecode(
                "blob payload must be hydrated by the durability provider before decode".to_owned(),
            )),
        }
    }

    pub fn to_blob_ref(&self, uri: String) -> Result<Self> {
        match self {
            PayloadRef::Inline {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                bytes,
            } => Ok(PayloadRef::Blob {
                codec: *codec,
                schema_fingerprint: schema_fingerprint.clone(),
                compression: *compression,
                encryption: encryption.clone(),
                digest: digest_bytes(bytes),
                size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                uri,
            }),
            PayloadRef::Blob { .. } => Ok(self.clone()),
        }
    }

    pub fn blob_digest(&self) -> Option<&str> {
        match self {
            PayloadRef::Blob { digest, .. } => Some(digest.as_str()),
            PayloadRef::Inline { .. } => None,
        }
    }

    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            PayloadRef::Inline { bytes, .. } => Some(bytes),
            PayloadRef::Blob { .. } => None,
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

pub fn encode_payload_with_codec<T>(value: &T, codec: CodecId) -> Result<PayloadRef>
where
    T: Serialize + ?Sized,
{
    match codec {
        CodecId::MessagePack => PayloadRef::inline_messagepack(value),
        CodecId::Json => PayloadRef::inline_json(value),
        CodecId::Protobuf => Err(Error::PayloadEncode(
            "protobuf payload codec is not enabled".to_owned(),
        )),
    }
}

pub fn decode_payload<T>(payload: &PayloadRef) -> Result<T>
where
    T: DeserializeOwned,
{
    match payload.codec() {
        CodecId::MessagePack => payload.decode_messagepack(),
        CodecId::Json => payload.decode_json(),
        CodecId::Protobuf => Err(Error::PayloadDecode(
            "protobuf payload codec is not enabled".to_owned(),
        )),
    }
}

pub fn validate_inline_side_effect_payload(payload: &PayloadRef) -> Result<()> {
    match payload {
        PayloadRef::Inline { bytes, .. } if bytes.len() <= MAX_SIDE_EFFECT_PAYLOAD_BYTES => Ok(()),
        PayloadRef::Inline { bytes, .. } => Err(Error::PayloadEncode(format!(
            "side effect payload is {} bytes, exceeding the {} byte inline limit",
            bytes.len(),
            MAX_SIDE_EFFECT_PAYLOAD_BYTES
        ))),
        PayloadRef::Blob { .. } => Err(Error::PayloadDecode(
            "side effect payloads must be stored inline".to_owned(),
        )),
    }
}

pub(crate) fn validate_side_effect_marker(marker: &SideEffectMarker) -> Result<()> {
    if marker.key.is_empty() {
        return Err(Error::PayloadEncode(
            "side effect key must not be empty".to_owned(),
        ));
    }
    validate_inline_side_effect_payload(&marker.value)
}

pub fn type_fingerprint<T: ?Sized>() -> String {
    type_name_fingerprint(std::any::type_name::<T>())
}

pub fn type_name_fingerprint(type_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(type_name.as_bytes());
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

pub fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(crate) fn map_failure_payloads<F>(
    mut failure: DurableFailure,
    map_payload: &mut F,
) -> Result<DurableFailure>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    if let Some(details) = failure.details.take() {
        failure.details = Some(map_payload(details)?);
    }
    Ok(failure)
}

pub(crate) fn map_activity_task_payloads<F>(
    mut task: ActivityTask,
    map_payload: &mut F,
) -> Result<ActivityTask>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    task.input = map_payload(task.input)?;
    Ok(task)
}

pub(crate) fn map_activity_map_input_manifest_ref<FLoad, FLeaf, FFinish>(
    payload: PayloadRef,
    load_container: &mut FLoad,
    map_leaf: &mut FLeaf,
    finish_container: &mut FFinish,
) -> Result<PayloadRef>
where
    FLoad: FnMut(PayloadRef) -> Result<PayloadRef>,
    FLeaf: FnMut(PayloadRef) -> Result<PayloadRef>,
    FFinish: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    let root = load_container(payload)?;
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = load_container(page)?;
            let page_codec = page.codec();
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            page.items = page
                .items
                .into_iter()
                .map(&mut *map_leaf)
                .collect::<Result<Vec<_>>>()?;
            finish_container(crate::encode_payload_with_codec(&page, page_codec)?)
        })
        .collect::<Result<Vec<_>>>()?;
    finish_container(crate::encode_payload_with_codec(&manifest, root.codec())?)
}

pub(crate) fn map_activity_map_result_manifest_ref<FLoad, FLeaf, FFinish>(
    payload: PayloadRef,
    load_container: &mut FLoad,
    map_leaf: &mut FLeaf,
    finish_container: &mut FFinish,
) -> Result<PayloadRef>
where
    FLoad: FnMut(PayloadRef) -> Result<PayloadRef>,
    FLeaf: FnMut(PayloadRef) -> Result<PayloadRef>,
    FFinish: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    let root = load_container(payload)?;
    let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = load_container(page)?;
            let page_codec = page.codec();
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            page.results = page
                .results
                .into_iter()
                .map(&mut *map_leaf)
                .collect::<Result<Vec<_>>>()?;
            finish_container(crate::encode_payload_with_codec(&page, page_codec)?)
        })
        .collect::<Result<Vec<_>>>()?;
    finish_container(crate::encode_payload_with_codec(&manifest, root.codec())?)
}

pub(crate) fn map_child_start_payloads<F>(
    mut message: ChildStartOutboxMessage,
    map_payload: &mut F,
) -> Result<ChildStartOutboxMessage>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    message.input = map_payload(message.input)?;
    Ok(message)
}

pub(crate) fn map_history_event_payloads<F>(
    data: HistoryEventData,
    map_payload: &mut F,
) -> Result<HistoryEventData>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    Ok(match data {
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        } => HistoryEventData::WorkflowStarted {
            workflow_type,
            input: map_payload(input)?,
        },
        HistoryEventData::WorkflowCompleted { result } => HistoryEventData::WorkflowCompleted {
            result: map_payload(result)?,
        },
        HistoryEventData::WorkflowFailed { failure } => HistoryEventData::WorkflowFailed {
            failure: map_failure_payloads(failure, map_payload)?,
        },
        HistoryEventData::WorkflowCancelled { reason } => {
            HistoryEventData::WorkflowCancelled { reason }
        }
        HistoryEventData::WorkflowContinuedAsNew { input } => {
            HistoryEventData::WorkflowContinuedAsNew {
                input: map_payload(input)?,
            }
        }
        HistoryEventData::WorkflowTaskStarted => HistoryEventData::WorkflowTaskStarted,
        HistoryEventData::ActivityScheduled(scheduled) => HistoryEventData::ActivityScheduled(
            map_activity_scheduled_payloads(scheduled, map_payload)?,
        ),
        HistoryEventData::ActivityMapScheduled(scheduled) => {
            HistoryEventData::ActivityMapScheduled(map_activity_map_scheduled_payloads(
                scheduled,
                map_payload,
            )?)
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            completed.result_manifest = map_payload(completed.result_manifest)?;
            HistoryEventData::ActivityMapCompleted(completed)
        }
        HistoryEventData::ActivityMapFailed(mut failed) => {
            failed.failure = map_failure_payloads(failed.failure, map_payload)?;
            HistoryEventData::ActivityMapFailed(failed)
        }
        HistoryEventData::ActivityCompleted(mut completed) => {
            completed.result = map_payload(completed.result)?;
            HistoryEventData::ActivityCompleted(completed)
        }
        HistoryEventData::ActivityFailed(mut failed) => {
            failed.failure = map_failure_payloads(failed.failure, map_payload)?;
            HistoryEventData::ActivityFailed(failed)
        }
        HistoryEventData::ActivityTimedOut(timed_out) => {
            HistoryEventData::ActivityTimedOut(timed_out)
        }
        HistoryEventData::ChildWorkflowStartRequested(requested) => {
            HistoryEventData::ChildWorkflowStartRequested(map_child_start_requested_payloads(
                requested,
                map_payload,
            )?)
        }
        HistoryEventData::ChildWorkflowStarted(started) => {
            HistoryEventData::ChildWorkflowStarted(started)
        }
        HistoryEventData::ChildWorkflowCompleted(completed) => {
            HistoryEventData::ChildWorkflowCompleted(map_child_completed_payloads(
                completed,
                map_payload,
            )?)
        }
        HistoryEventData::ChildWorkflowFailed(failed) => {
            HistoryEventData::ChildWorkflowFailed(map_child_failed_payloads(failed, map_payload)?)
        }
        HistoryEventData::ChildWorkflowCancelled(cancelled) => {
            HistoryEventData::ChildWorkflowCancelled(cancelled)
        }
        HistoryEventData::ChildWorkflowMapScheduled(scheduled) => {
            HistoryEventData::ChildWorkflowMapScheduled(map_child_workflow_map_scheduled_payloads(
                scheduled,
                map_payload,
            )?)
        }
        HistoryEventData::ChildWorkflowMapCompleted(completed) => {
            HistoryEventData::ChildWorkflowMapCompleted(map_child_workflow_map_completed_payloads(
                completed,
                map_payload,
            )?)
        }
        HistoryEventData::ChildWorkflowMapFailed(failed) => {
            HistoryEventData::ChildWorkflowMapFailed(map_child_workflow_map_failed_payloads(
                failed,
                map_payload,
            )?)
        }
        HistoryEventData::TimerStarted(timer) => HistoryEventData::TimerStarted(timer),
        HistoryEventData::TimerFired(timer) => HistoryEventData::TimerFired(timer),
        HistoryEventData::SignalConsumed(signal) => {
            HistoryEventData::SignalConsumed(map_signal_payloads(signal, map_payload)?)
        }
        HistoryEventData::SelectWinner(winner) => HistoryEventData::SelectWinner(winner),
        HistoryEventData::VersionMarker(marker) => HistoryEventData::VersionMarker(marker),
        HistoryEventData::DeprecatedPatchMarker(marker) => {
            HistoryEventData::DeprecatedPatchMarker(marker)
        }
        HistoryEventData::SideEffectMarker(marker) => {
            validate_side_effect_marker(&marker)?;
            HistoryEventData::SideEffectMarker(marker)
        }
    })
}

fn map_activity_scheduled_payloads<F>(
    mut scheduled: ActivityScheduled,
    map_payload: &mut F,
) -> Result<ActivityScheduled>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    scheduled.input = map_payload(scheduled.input)?;
    Ok(scheduled)
}

fn map_activity_map_scheduled_payloads<F>(
    mut scheduled: ActivityMapScheduled,
    map_payload: &mut F,
) -> Result<ActivityMapScheduled>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    scheduled.input_manifest = map_payload(scheduled.input_manifest)?;
    Ok(scheduled)
}

fn map_child_start_requested_payloads<F>(
    mut requested: ChildWorkflowStartRequested,
    map_payload: &mut F,
) -> Result<ChildWorkflowStartRequested>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    requested.input = map_payload(requested.input)?;
    Ok(requested)
}

fn map_child_completed_payloads<F>(
    mut completed: ChildWorkflowCompleted,
    map_payload: &mut F,
) -> Result<ChildWorkflowCompleted>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    completed.result = map_payload(completed.result)?;
    Ok(completed)
}

fn map_child_failed_payloads<F>(
    mut failed: ChildWorkflowFailed,
    map_payload: &mut F,
) -> Result<ChildWorkflowFailed>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    failed.failure = map_failure_payloads(failed.failure, map_payload)?;
    Ok(failed)
}

fn map_child_workflow_map_scheduled_payloads<F>(
    mut scheduled: ChildWorkflowMapScheduled,
    map_payload: &mut F,
) -> Result<ChildWorkflowMapScheduled>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    scheduled.input_manifest = map_payload(scheduled.input_manifest)?;
    Ok(scheduled)
}

fn map_child_workflow_map_completed_payloads<F>(
    mut completed: ChildWorkflowMapCompleted,
    map_payload: &mut F,
) -> Result<ChildWorkflowMapCompleted>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    completed.result_manifest = map_payload(completed.result_manifest)?;
    Ok(completed)
}

fn map_child_workflow_map_failed_payloads<F>(
    mut failed: ChildWorkflowMapFailed,
    map_payload: &mut F,
) -> Result<ChildWorkflowMapFailed>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    failed.failure = map_failure_payloads(failed.failure, map_payload)?;
    Ok(failed)
}

fn map_signal_payloads<F>(mut signal: SignalConsumed, map_payload: &mut F) -> Result<SignalConsumed>
where
    F: FnMut(PayloadRef) -> Result<PayloadRef>,
{
    signal.payload = map_payload(signal.payload)?;
    Ok(signal)
}
