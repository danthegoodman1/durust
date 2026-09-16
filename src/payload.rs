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
use std::future::Future;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecId {
    MessagePack,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionId {
    None,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFingerprint(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionMetadata {
    pub key_id: String,
}

/// A payload reference, serialized in the shape both runtimes store inside
/// payloads that embed other payloads (map manifests and pages):
/// `{ kind: "Inline" | "Blob", codec, schemaFingerprint, compression,
/// encryption, ... }`, with inline bytes as a byte string.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum PayloadRef {
    Inline {
        codec: CodecId,
        schema_fingerprint: SchemaFingerprint,
        compression: CompressionId,
        encryption: Option<EncryptionMetadata>,
        #[serde(with = "serde_bytes")]
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
    }
}

pub fn decode_payload<T>(payload: &PayloadRef) -> Result<T>
where
    T: DeserializeOwned,
{
    match payload.codec() {
        CodecId::MessagePack => payload.decode_messagepack(),
        CodecId::Json => payload.decode_json(),
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
    cached_type_name_fingerprint(std::any::type_name::<T>()).to_owned()
}

/// Fingerprints are computed once per type. `type_name` returns one static
/// string per type within a binary, so its address is the key, and the
/// fingerprint is leaked once so a hit borrows it: the per-type set is
/// bounded by the program's types, and every encode used to pay a SHA-256,
/// a hex encode, and two allocations for a value that never changes.
fn cached_type_name_fingerprint(type_name: &'static str) -> &'static str {
    use std::collections::HashMap;
    use std::sync::RwLock;
    static CACHE: RwLock<Option<HashMap<usize, &'static str>>> = RwLock::new(None);
    let key = type_name.as_ptr() as usize;
    if let Some(cached) = CACHE
        .read()
        .expect("fingerprint cache poisoned")
        .as_ref()
        .and_then(|cache| cache.get(&key).copied())
    {
        return cached;
    }
    let fingerprint: &'static str = Box::leak(type_name_fingerprint(type_name).into_boxed_str());
    CACHE
        .write()
        .expect("fingerprint cache poisoned")
        .get_or_insert_with(HashMap::new)
        .entry(key)
        .or_insert(fingerprint)
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
            // A page the loader could not inline belongs to another layer
            // (foreign scheme); it passes through opaquely and its items are
            // that layer's responsibility.
            if matches!(page, PayloadRef::Blob { .. }) {
                return Ok(page);
            }
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

/// Async payload rewrite operations for one storage direction (normalize or
/// hydrate) of a single provider. Implementations supply the storage-specific
/// leaf behavior; the [`rewrite_history_event_payloads`] visitor owns the shared
/// history-event traversal so the providers no longer hand-maintain identical
/// match arms. Returned futures are `Send` so they compose into the providers'
/// boxed backend futures.
pub(crate) trait PayloadRewrite {
    fn payload(&mut self, payload: PayloadRef) -> impl Future<Output = Result<PayloadRef>> + Send;

    fn activity_map_input_manifest(
        &mut self,
        manifest: PayloadRef,
    ) -> impl Future<Output = Result<PayloadRef>> + Send;

    fn activity_map_result_manifest(
        &mut self,
        manifest: PayloadRef,
    ) -> impl Future<Output = Result<PayloadRef>> + Send;

    fn child_workflow_map_result_manifest(
        &mut self,
        manifest: PayloadRef,
    ) -> impl Future<Output = Result<PayloadRef>> + Send;
}

async fn rewrite_failure<R: PayloadRewrite>(
    rewriter: &mut R,
    mut failure: DurableFailure,
) -> Result<DurableFailure> {
    if let Some(details) = failure.details.take() {
        failure.details = Some(rewriter.payload(details).await?);
    }
    Ok(failure)
}

/// Walks a history event applying the rewriter to each payload-bearing field.
/// Side-effect markers are validated in place (defense-in-depth against an
/// oversized inline payload); events without payloads pass through unchanged.
pub(crate) async fn rewrite_history_event_payloads<R: PayloadRewrite>(
    rewriter: &mut R,
    data: HistoryEventData,
) -> Result<HistoryEventData> {
    Ok(match data {
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        } => HistoryEventData::WorkflowStarted {
            workflow_type,
            input: rewriter.payload(input).await?,
        },
        HistoryEventData::WorkflowCompleted { result } => HistoryEventData::WorkflowCompleted {
            result: rewriter.payload(result).await?,
        },
        HistoryEventData::WorkflowFailed { failure } => HistoryEventData::WorkflowFailed {
            failure: rewrite_failure(rewriter, failure).await?,
        },
        HistoryEventData::WorkflowContinuedAsNew { input } => {
            HistoryEventData::WorkflowContinuedAsNew {
                input: rewriter.payload(input).await?,
            }
        }
        HistoryEventData::ActivityScheduled(mut scheduled) => {
            scheduled.input = rewriter.payload(scheduled.input).await?;
            HistoryEventData::ActivityScheduled(scheduled)
        }
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            scheduled.input_manifest = rewriter
                .activity_map_input_manifest(scheduled.input_manifest)
                .await?;
            HistoryEventData::ActivityMapScheduled(scheduled)
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            completed.result_manifest = rewriter
                .activity_map_result_manifest(completed.result_manifest)
                .await?;
            HistoryEventData::ActivityMapCompleted(completed)
        }
        HistoryEventData::ActivityMapFailed(mut failed) => {
            failed.failure = rewrite_failure(rewriter, failed.failure).await?;
            HistoryEventData::ActivityMapFailed(failed)
        }
        HistoryEventData::ChildWorkflowMapScheduled(mut scheduled) => {
            scheduled.input_manifest = rewriter
                .activity_map_input_manifest(scheduled.input_manifest)
                .await?;
            HistoryEventData::ChildWorkflowMapScheduled(scheduled)
        }
        HistoryEventData::ChildWorkflowMapCompleted(mut completed) => {
            completed.result_manifest = rewriter
                .child_workflow_map_result_manifest(completed.result_manifest)
                .await?;
            HistoryEventData::ChildWorkflowMapCompleted(completed)
        }
        HistoryEventData::ChildWorkflowMapFailed(mut failed) => {
            failed.failure = rewrite_failure(rewriter, failed.failure).await?;
            HistoryEventData::ChildWorkflowMapFailed(failed)
        }
        HistoryEventData::ActivityCompleted(mut completed) => {
            completed.result = rewriter.payload(completed.result).await?;
            HistoryEventData::ActivityCompleted(completed)
        }
        HistoryEventData::ActivityFailed(mut failed) => {
            failed.failure = rewrite_failure(rewriter, failed.failure).await?;
            HistoryEventData::ActivityFailed(failed)
        }
        HistoryEventData::ChildWorkflowStartRequested(mut requested) => {
            requested.input = rewriter.payload(requested.input).await?;
            HistoryEventData::ChildWorkflowStartRequested(requested)
        }
        HistoryEventData::ChildWorkflowCompleted(mut completed) => {
            completed.result = rewriter.payload(completed.result).await?;
            HistoryEventData::ChildWorkflowCompleted(completed)
        }
        HistoryEventData::ChildWorkflowFailed(mut failed) => {
            failed.failure = rewrite_failure(rewriter, failed.failure).await?;
            HistoryEventData::ChildWorkflowFailed(failed)
        }
        HistoryEventData::SignalConsumed(mut signal) => {
            signal.payload = rewriter.payload(signal.payload).await?;
            HistoryEventData::SignalConsumed(signal)
        }
        HistoryEventData::SideEffectMarker(marker) => {
            validate_side_effect_marker(&marker)?;
            HistoryEventData::SideEffectMarker(marker)
        }
        // The payload-free variants, listed rather than swept up by a
        // `other => other` catch-all. The catch-all was the asymmetry: its
        // synchronous twin `map_history_event_payloads` is exhaustive, so a new
        // payload-bearing variant fails that build immediately, while here it
        // matched `other` and passed through **unnormalized** on the Postgres
        // and `payload_backend` paths — a silent storage bug rather than a
        // compile error. Spelled out, the compiler now stops both.
        //
        // The arms below are pass-through, exactly as the catch-all was; the
        // set of variants this function rewrites is unchanged. A new variant
        // belongs in this list only once it is known to carry no payload.
        data @ (HistoryEventData::WorkflowCancelled { reason: _ }
        | HistoryEventData::WorkflowTaskStarted
        | HistoryEventData::ActivityTimedOut(_)
        | HistoryEventData::ChildWorkflowStarted(_)
        | HistoryEventData::ChildWorkflowCancelled(_)
        | HistoryEventData::TimerStarted(_)
        | HistoryEventData::TimerFired(_)
        | HistoryEventData::SelectWinner(_)
        | HistoryEventData::VersionMarker(_)
        | HistoryEventData::DeprecatedPatchMarker(_)) => data,
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

// ---- one payload traversal ---------------------------------------------------
//
// Every collector over stored payloads (roots for the decorator's garbage
// collector, reachability for a provider's own blob table, the decorator's
// walk over its external store) is built from the two pieces below: the one
// exhaustive match over a history event's payload fields, and one walk over a
// manifest and its pages that lets the caller load each container from
// whichever store holds it.

/// What a manifest's bytes describe, which decides how its pages decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestKind {
    ActivityMapInput,
    ActivityMapResult,
    ChildWorkflowMapResult,
}

impl ManifestKind {
    pub(crate) fn root(self, payload: PayloadRef) -> crate::PayloadRootRef {
        match self {
            Self::ActivityMapInput => crate::PayloadRootRef::ActivityMapInputManifest(payload),
            Self::ActivityMapResult => crate::PayloadRootRef::ActivityMapResultManifest(payload),
            Self::ChildWorkflowMapResult => {
                crate::PayloadRootRef::ChildWorkflowMapResultManifest(payload)
            }
        }
    }
}

impl crate::PayloadRootRef {
    /// The manifest kind of a container root, or `None` for a plain payload.
    pub(crate) fn manifest_kind(&self) -> Option<ManifestKind> {
        match self {
            Self::Payload(_) => None,
            Self::ActivityMapInputManifest(_) => Some(ManifestKind::ActivityMapInput),
            Self::ActivityMapResultManifest(_) => Some(ManifestKind::ActivityMapResult),
            Self::ChildWorkflowMapResultManifest(_) => Some(ManifestKind::ChildWorkflowMapResult),
        }
    }
}

/// A payload-bearing field of a history event, tagged by what its bytes hold.
#[derive(Clone, Copy, Debug)]
pub(crate) enum PayloadSlot<'a> {
    Payload(&'a PayloadRef),
    Manifest(ManifestKind, &'a PayloadRef),
}

/// Calls `visit` with every payload slot of `data`, in field order, after
/// validating a `SideEffectMarker`'s inline bound. Events without payloads
/// yield nothing.
pub(crate) fn history_event_payload_slots<'a>(
    data: &'a HistoryEventData,
    visit: &mut dyn FnMut(PayloadSlot<'a>),
) -> Result<()> {
    let mut payload = |payload: &'a PayloadRef| visit(PayloadSlot::Payload(payload));
    match data {
        HistoryEventData::WorkflowStarted { input, .. }
        | HistoryEventData::WorkflowContinuedAsNew { input } => payload(input),
        HistoryEventData::WorkflowCompleted { result } => payload(result),
        HistoryEventData::WorkflowFailed { failure } => {
            if let Some(details) = failure_payload(failure) {
                payload(details);
            }
        }
        HistoryEventData::ActivityScheduled(scheduled) => payload(&scheduled.input),
        HistoryEventData::ActivityMapScheduled(scheduled) => visit(PayloadSlot::Manifest(
            ManifestKind::ActivityMapInput,
            &scheduled.input_manifest,
        )),
        HistoryEventData::ActivityMapCompleted(completed) => visit(PayloadSlot::Manifest(
            ManifestKind::ActivityMapResult,
            &completed.result_manifest,
        )),
        HistoryEventData::ActivityMapFailed(failed) => {
            if let Some(details) = failure_payload(&failed.failure) {
                payload(details);
            }
        }
        HistoryEventData::ActivityCompleted(completed) => payload(&completed.result),
        HistoryEventData::ActivityFailed(failed) => {
            if let Some(details) = failure_payload(&failed.failure) {
                payload(details);
            }
        }
        HistoryEventData::ChildWorkflowStartRequested(requested) => payload(&requested.input),
        HistoryEventData::ChildWorkflowCompleted(completed) => payload(&completed.result),
        HistoryEventData::ChildWorkflowFailed(failed) => {
            if let Some(details) = failure_payload(&failed.failure) {
                payload(details);
            }
        }
        HistoryEventData::ChildWorkflowMapScheduled(scheduled) => visit(PayloadSlot::Manifest(
            ManifestKind::ActivityMapInput,
            &scheduled.input_manifest,
        )),
        HistoryEventData::ChildWorkflowMapCompleted(completed) => visit(PayloadSlot::Manifest(
            ManifestKind::ChildWorkflowMapResult,
            &completed.result_manifest,
        )),
        HistoryEventData::ChildWorkflowMapFailed(failed) => {
            if let Some(details) = failure_payload(&failed.failure) {
                payload(details);
            }
        }
        HistoryEventData::SignalConsumed(signal) => payload(&signal.payload),
        HistoryEventData::SideEffectMarker(marker) => validate_side_effect_marker(marker)?,
        HistoryEventData::WorkflowCancelled { .. }
        | HistoryEventData::WorkflowTaskStarted
        | HistoryEventData::ActivityTimedOut(_)
        | HistoryEventData::ChildWorkflowStarted(_)
        | HistoryEventData::ChildWorkflowCancelled(_)
        | HistoryEventData::TimerStarted(_)
        | HistoryEventData::TimerFired(_)
        | HistoryEventData::SelectWinner(_)
        | HistoryEventData::VersionMarker(_)
        | HistoryEventData::DeprecatedPatchMarker(_) => {}
    }
    Ok(())
}

pub(crate) fn failure_payload(failure: &DurableFailure) -> Option<&PayloadRef> {
    failure.details.as_ref()
}

pub(crate) fn child_workflow_map_outcome_payload(
    outcome: &crate::ChildWorkflowMapItemOutcome,
) -> Option<&PayloadRef> {
    match outcome {
        crate::ChildWorkflowMapItemOutcome::Succeeded { result } => Some(result),
        crate::ChildWorkflowMapItemOutcome::Failed { failure } => failure_payload(failure),
        crate::ChildWorkflowMapItemOutcome::Cancelled { .. } => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManifestLevel {
    Manifest,
    Page,
}

/// A walk over a manifest, its pages, and the refs inside them, driven by the
/// caller: `next_container` names the container whose bytes are needed, the
/// caller loads them from whichever store holds them and answers with
/// `provide` (or `skip` when the store is not one it reads), and `into_refs`
/// returns every ref the walk met, containers included, in manifest order.
pub(crate) struct ManifestWalk {
    kind: ManifestKind,
    pending: std::collections::VecDeque<(ManifestLevel, PayloadRef)>,
    current: Option<ManifestLevel>,
    refs: Vec<PayloadRef>,
}

impl ManifestWalk {
    pub(crate) fn new(kind: ManifestKind, manifest: PayloadRef) -> Self {
        Self {
            kind,
            pending: std::collections::VecDeque::from([(
                ManifestLevel::Manifest,
                manifest.clone(),
            )]),
            current: None,
            refs: vec![manifest],
        }
    }

    /// A walk that starts below an already decoded activity-map input
    /// manifest, at its pages.
    pub(crate) fn from_input_pages(pages: impl IntoIterator<Item = PayloadRef>) -> Self {
        let pages: Vec<PayloadRef> = pages.into_iter().collect();
        Self {
            kind: ManifestKind::ActivityMapInput,
            pending: pages
                .iter()
                .cloned()
                .map(|page| (ManifestLevel::Page, page))
                .collect(),
            current: None,
            refs: pages,
        }
    }

    /// The next container whose bytes the walk needs, or `None` when done.
    pub(crate) fn next_container(&mut self) -> Option<PayloadRef> {
        let (level, container) = self.pending.pop_front()?;
        self.current = Some(level);
        Some(container)
    }

    /// Names the container `next_container` returned, for error messages.
    pub(crate) fn context(&self) -> &'static str {
        match (self.kind, self.current) {
            (ManifestKind::ActivityMapInput, Some(ManifestLevel::Page)) => {
                "activity map input manifest page"
            }
            (ManifestKind::ActivityMapInput, _) => "activity map input manifest root",
            (ManifestKind::ActivityMapResult, Some(ManifestLevel::Page)) => {
                "activity map result manifest page"
            }
            (ManifestKind::ActivityMapResult, _) => "activity map result manifest root",
            (ManifestKind::ChildWorkflowMapResult, Some(ManifestLevel::Page)) => {
                "child workflow map result manifest page"
            }
            (ManifestKind::ChildWorkflowMapResult, _) => "child workflow map result manifest root",
        }
    }

    /// Answers `next_container` with the container's inline bytes: a manifest
    /// queues its pages, a page adds the refs it holds.
    pub(crate) fn provide(&mut self, hydrated: &PayloadRef) -> Result<()> {
        let level = self.current.take().unwrap_or(ManifestLevel::Manifest);
        match (self.kind, level) {
            (ManifestKind::ActivityMapInput, ManifestLevel::Manifest) => {
                let manifest: ActivityMapInputManifest = decode_payload(hydrated)?;
                self.queue_pages(manifest.pages);
            }
            (ManifestKind::ActivityMapInput, ManifestLevel::Page) => {
                let page: ActivityMapInputPage = decode_payload(hydrated)?;
                self.refs.extend(page.items);
            }
            (ManifestKind::ActivityMapResult, ManifestLevel::Manifest) => {
                let manifest: ActivityMapResultManifest = decode_payload(hydrated)?;
                self.queue_pages(manifest.pages);
            }
            (ManifestKind::ActivityMapResult, ManifestLevel::Page) => {
                let page: ActivityMapResultPage = decode_payload(hydrated)?;
                self.refs.extend(page.results);
            }
            (ManifestKind::ChildWorkflowMapResult, ManifestLevel::Manifest) => {
                let manifest: crate::ChildWorkflowMapResultManifest = decode_payload(hydrated)?;
                self.queue_pages(manifest.pages);
            }
            (ManifestKind::ChildWorkflowMapResult, ManifestLevel::Page) => {
                let page: crate::ChildWorkflowMapResultPage = decode_payload(hydrated)?;
                self.refs.extend(
                    page.outcomes
                        .iter()
                        .filter_map(child_workflow_map_outcome_payload)
                        .cloned(),
                );
            }
        }
        Ok(())
    }

    /// Answers `next_container` by leaving the container unopened.
    pub(crate) fn skip(&mut self) {
        self.current = None;
    }

    fn queue_pages(&mut self, pages: Vec<PayloadRef>) {
        for page in pages {
            self.refs.push(page.clone());
            self.pending.push_back((ManifestLevel::Page, page));
        }
    }

    pub(crate) fn into_refs(self) -> Vec<PayloadRef> {
        self.refs
    }
}

/// Every ref a manifest reaches, loading containers through `load`, which
/// answers `None` for a container held in a store this caller does not read.
pub(crate) fn manifest_refs(
    kind: ManifestKind,
    manifest: &PayloadRef,
    load: &mut dyn FnMut(&PayloadRef) -> Result<Option<PayloadRef>>,
) -> Result<Vec<PayloadRef>> {
    let mut walk = ManifestWalk::new(kind, manifest.clone());
    drive_manifest_walk(&mut walk, load)?;
    Ok(walk.into_refs())
}

/// Every ref below an already decoded activity-map input manifest.
pub(crate) fn input_manifest_page_refs(
    manifest: &ActivityMapInputManifest,
    load: &mut dyn FnMut(&PayloadRef) -> Result<Option<PayloadRef>>,
) -> Result<Vec<PayloadRef>> {
    let mut walk = ManifestWalk::from_input_pages(manifest.pages.iter().cloned());
    drive_manifest_walk(&mut walk, load)?;
    Ok(walk.into_refs())
}

fn drive_manifest_walk(
    walk: &mut ManifestWalk,
    load: &mut dyn FnMut(&PayloadRef) -> Result<Option<PayloadRef>>,
) -> Result<()> {
    while let Some(container) = walk.next_container() {
        match load(&container)? {
            Some(hydrated) => walk.provide(&hydrated)?,
            None => walk.skip(),
        }
    }
    Ok(())
}

/// The payload roots of one history event: plain refs as they are, and each
/// manifest through `hydrate_manifest`, which returns the manifest with its
/// bytes in place when this store holds them and the ref unchanged otherwise.
pub(crate) fn history_event_payload_roots(
    data: &HistoryEventData,
    roots: &mut Vec<crate::PayloadRootRef>,
    hydrate_manifest: &mut dyn FnMut(ManifestKind, &PayloadRef) -> Result<PayloadRef>,
) -> Result<()> {
    let mut slots = Vec::new();
    history_event_payload_slots(data, &mut |slot| slots.push(slot))?;
    for slot in slots {
        match slot {
            PayloadSlot::Payload(payload) => {
                roots.push(crate::PayloadRootRef::Payload(payload.clone()));
            }
            PayloadSlot::Manifest(kind, manifest) => {
                roots.push(kind.root(hydrate_manifest(kind, manifest)?));
            }
        }
    }
    Ok(())
}

/// Every ref one history event reaches: plain refs directly, and each
/// manifest with everything inside it, containers loaded through `load`.
pub(crate) fn history_event_payload_refs(
    data: &HistoryEventData,
    load: &mut dyn FnMut(&PayloadRef) -> Result<Option<PayloadRef>>,
    visit: &mut dyn FnMut(&PayloadRef) -> Result<()>,
) -> Result<()> {
    let mut slots = Vec::new();
    history_event_payload_slots(data, &mut |slot| slots.push(slot))?;
    for slot in slots {
        match slot {
            PayloadSlot::Payload(payload) => visit(payload)?,
            PayloadSlot::Manifest(kind, manifest) => {
                for payload in manifest_refs(kind, manifest, load)? {
                    visit(&payload)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActivityMapCompleted, ChildWorkflowMapCompleted, RunId, command_id};
    use futures::executor::block_on;

    // Returns a distinct tagged payload from each rewriter method so a test can
    // assert which method a given history-event field was routed through.
    struct SentinelRewriter;

    impl PayloadRewrite for SentinelRewriter {
        async fn payload(&mut self, _payload: PayloadRef) -> Result<PayloadRef> {
            crate::encode_payload(&"payload".to_owned())
        }
        async fn activity_map_input_manifest(
            &mut self,
            _manifest: PayloadRef,
        ) -> Result<PayloadRef> {
            crate::encode_payload(&"input-manifest".to_owned())
        }
        async fn activity_map_result_manifest(
            &mut self,
            _manifest: PayloadRef,
        ) -> Result<PayloadRef> {
            crate::encode_payload(&"activity-result-manifest".to_owned())
        }
        async fn child_workflow_map_result_manifest(
            &mut self,
            _manifest: PayloadRef,
        ) -> Result<PayloadRef> {
            crate::encode_payload(&"child-result-manifest".to_owned())
        }
    }

    fn tag(payload: &PayloadRef) -> String {
        crate::decode_payload::<String>(payload).expect("sentinel payload should decode")
    }

    // Guards the shared visitor's per-field routing: plain payloads, failure
    // details, and the two distinct result-manifest kinds must each reach the
    // matching rewriter method (a transcription slip would land on the wrong one).
    #[test]
    fn rewrite_history_event_routes_each_field_to_its_rewriter() {
        let cid = command_id(&RunId::new("run/test"), 1);
        let orig = crate::encode_payload(&"orig".to_owned()).unwrap();

        let event = HistoryEventData::WorkflowCompleted {
            result: orig.clone(),
        };
        let HistoryEventData::WorkflowCompleted { result } =
            block_on(rewrite_history_event_payloads(&mut SentinelRewriter, event)).unwrap()
        else {
            panic!("workflow completed variant changed");
        };
        assert_eq!(tag(&result), "payload");

        let event = HistoryEventData::WorkflowFailed {
            failure: DurableFailure::new("e", "m")
                .with_details(&"orig".to_owned())
                .unwrap(),
        };
        let HistoryEventData::WorkflowFailed { failure } =
            block_on(rewrite_history_event_payloads(&mut SentinelRewriter, event)).unwrap()
        else {
            panic!("workflow failed variant changed");
        };
        assert_eq!(tag(&failure.details.unwrap()), "payload");

        let event = HistoryEventData::ActivityMapCompleted(ActivityMapCompleted {
            command_id: cid.clone(),
            result_manifest: orig.clone(),
            item_count: 0,
            success_count: 0,
            failure_count: 0,
        });
        let HistoryEventData::ActivityMapCompleted(completed) =
            block_on(rewrite_history_event_payloads(&mut SentinelRewriter, event)).unwrap()
        else {
            panic!("activity map completed variant changed");
        };
        assert_eq!(tag(&completed.result_manifest), "activity-result-manifest");

        let event = HistoryEventData::ChildWorkflowMapCompleted(ChildWorkflowMapCompleted {
            command_id: cid,
            result_manifest: orig,
            item_count: 0,
            success_count: 0,
            failure_count: 0,
            cancellation_count: 0,
        });
        let HistoryEventData::ChildWorkflowMapCompleted(completed) =
            block_on(rewrite_history_event_payloads(&mut SentinelRewriter, event)).unwrap()
        else {
            panic!("child workflow map completed variant changed");
        };
        assert_eq!(tag(&completed.result_manifest), "child-result-manifest");
    }

    // The visitor validates side-effect markers on every storage rewrite (the
    // behavior memory/SQLite always had and Postgres now shares).
    #[test]
    fn rewrite_history_event_validates_side_effect_markers() {
        let cid = command_id(&RunId::new("run/test"), 1);
        let valid = HistoryEventData::SideEffectMarker(SideEffectMarker {
            command_id: cid.clone(),
            key: "k".to_owned(),
            value: crate::encode_payload(&"v".to_owned()).unwrap(),
        });
        assert!(block_on(rewrite_history_event_payloads(&mut SentinelRewriter, valid)).is_ok());

        let invalid = HistoryEventData::SideEffectMarker(SideEffectMarker {
            command_id: cid,
            key: String::new(),
            value: crate::encode_payload(&"v".to_owned()).unwrap(),
        });
        assert!(
            block_on(rewrite_history_event_payloads(
                &mut SentinelRewriter,
                invalid
            ))
            .is_err()
        );
    }
}
