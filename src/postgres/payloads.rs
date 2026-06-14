use super::*;
use tokio_postgres::Transaction;

impl PostgresBackend {
    pub(super) async fn payload_roots_inner(&self) -> Result<PayloadRootsOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let roots = self.collect_payload_roots_tx(&tx, &schema).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(PayloadRootsOutcome { roots })
    }

    pub(super) async fn gc_payload_blobs_inner(
        &self,
        req: PayloadGarbageCollectionRequest,
    ) -> Result<PayloadGarbageCollectionOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let mut reachable = BTreeSet::new();
        self.collect_reachable_payload_blobs_tx(&tx, &schema, &mut reachable)
            .await?;
        let rows = tx
            .query(
                &format!("select digest from {schema}.payload_blobs order by digest asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        let all_digests = rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<BTreeSet<_>>();
        let scanned_blobs = all_digests.len();
        let retained_blobs = all_digests
            .iter()
            .filter(|digest| reachable.contains(*digest))
            .count();
        let garbage = all_digests
            .into_iter()
            .filter(|digest| !reachable.contains(digest))
            .collect::<Vec<_>>();
        let deleted_blobs = garbage.len();
        if !req.dry_run {
            for digest in garbage {
                tx.execute(
                    &format!("delete from {schema}.payload_blobs where digest = $1"),
                    &[&digest],
                )
                .await
                .map_err(postgres_error)?;
            }
        }
        tx.commit().await.map_err(postgres_error)?;
        Ok(PayloadGarbageCollectionOutcome {
            scanned_blobs,
            retained_blobs,
            deleted_blobs,
        })
    }

    pub(super) async fn collect_payload_roots_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
    ) -> Result<Vec<PayloadRootRef>> {
        let mut roots = Vec::new();
        let rows = tx
            .query(
                &format!(
                    "select data from {schema}.history_events order by run_id asc, event_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_history_event_payload_roots_tx(tx, &data, &mut roots)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_tasks order by activity_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(task.input));
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_maps order by map_command_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::ActivityMapInputManifest(
                self.activity_map_input_root_for_roots_tx(tx, task.input_manifest)
                    .await?,
            ));
        }

        let rows = tx
            .query(
                &format!(
                    "select result
                     from {schema}.activity_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let result: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(result));
        }

        let rows = tx
            .query(
                &format!("select payload from {schema}.signals order by signal_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(payload));
        }

        let rows = tx
            .query(
                &format!(
                    "select payload
                     from {schema}.query_projections
                     order by namespace asc, workflow_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(payload));
        }

        Ok(roots)
    }

    pub(super) async fn collect_reachable_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        let rows = tx
            .query(
                &format!(
                    "select data from {schema}.history_events order by run_id asc, event_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_history_event_payload_blobs_tx(tx, &data, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_tasks order by activity_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &task.input, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_maps order by map_command_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_activity_map_input_manifest_ref_tx(tx, &task.input_manifest, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!(
                    "select result
                     from {schema}.activity_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let result: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &result, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select payload from {schema}.signals order by signal_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &payload, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!(
                    "select payload
                     from {schema}.query_projections
                     order by namespace asc, workflow_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &payload, reachable)
                .await?;
        }

        Ok(())
    }

    pub(super) async fn collect_history_event_payload_roots_tx(
        &self,
        tx: &Transaction<'_>,
        data: &HistoryEventData,
        roots: &mut Vec<PayloadRootRef>,
    ) -> Result<()> {
        match data {
            HistoryEventData::WorkflowStarted { input, .. }
            | HistoryEventData::WorkflowContinuedAsNew { input } => {
                roots.push(PayloadRootRef::Payload(input.clone()));
            }
            HistoryEventData::WorkflowCompleted { result } => {
                roots.push(PayloadRootRef::Payload(result.clone()));
            }
            HistoryEventData::WorkflowFailed { failure } => {
                collect_failure_payload_roots(failure, roots);
            }
            HistoryEventData::ActivityScheduled(scheduled) => {
                roots.push(PayloadRootRef::Payload(scheduled.input.clone()));
            }
            HistoryEventData::ActivityMapScheduled(scheduled) => {
                roots.push(PayloadRootRef::ActivityMapInputManifest(
                    self.activity_map_input_root_for_roots_tx(tx, scheduled.input_manifest.clone())
                        .await?,
                ));
            }
            HistoryEventData::ActivityMapCompleted(completed) => {
                roots.push(PayloadRootRef::ActivityMapResultManifest(
                    self.activity_map_result_root_for_roots_tx(
                        tx,
                        completed.result_manifest.clone(),
                    )
                    .await?,
                ));
            }
            HistoryEventData::ActivityMapFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::ActivityCompleted(completed) => {
                roots.push(PayloadRootRef::Payload(completed.result.clone()));
            }
            HistoryEventData::ActivityFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::ChildWorkflowStartRequested(requested) => {
                roots.push(PayloadRootRef::Payload(requested.input.clone()));
            }
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                roots.push(PayloadRootRef::Payload(completed.result.clone()));
            }
            HistoryEventData::ChildWorkflowFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::SignalConsumed(signal) => {
                roots.push(PayloadRootRef::Payload(signal.payload.clone()));
            }
            HistoryEventData::SideEffectMarker(marker) => {
                crate::payload::validate_side_effect_marker(marker)?;
            }
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

    pub(super) async fn collect_history_event_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        data: &HistoryEventData,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        match data {
            HistoryEventData::WorkflowStarted { input, .. }
            | HistoryEventData::WorkflowContinuedAsNew { input } => {
                self.collect_payload_blob_ref_tx(tx, input, reachable).await
            }
            HistoryEventData::WorkflowCompleted { result } => {
                self.collect_payload_blob_ref_tx(tx, result, reachable)
                    .await
            }
            HistoryEventData::WorkflowFailed { failure } => {
                self.collect_failure_payload_blobs_tx(tx, failure, reachable)
                    .await
            }
            HistoryEventData::ActivityScheduled(scheduled) => {
                self.collect_payload_blob_ref_tx(tx, &scheduled.input, reachable)
                    .await
            }
            HistoryEventData::ActivityMapScheduled(scheduled) => {
                self.collect_activity_map_input_manifest_ref_tx(
                    tx,
                    &scheduled.input_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ActivityMapCompleted(completed) => {
                self.collect_activity_map_result_manifest_ref_tx(
                    tx,
                    &completed.result_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ActivityMapFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::ActivityCompleted(completed) => {
                self.collect_payload_blob_ref_tx(tx, &completed.result, reachable)
                    .await
            }
            HistoryEventData::ActivityFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowStartRequested(requested) => {
                self.collect_payload_blob_ref_tx(tx, &requested.input, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                self.collect_payload_blob_ref_tx(tx, &completed.result, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::SignalConsumed(signal) => {
                self.collect_payload_blob_ref_tx(tx, &signal.payload, reachable)
                    .await
            }
            HistoryEventData::SideEffectMarker(marker) => {
                crate::payload::validate_side_effect_marker(marker)
            }
            HistoryEventData::WorkflowCancelled { .. }
            | HistoryEventData::WorkflowTaskStarted
            | HistoryEventData::ActivityTimedOut(_)
            | HistoryEventData::ChildWorkflowStarted(_)
            | HistoryEventData::ChildWorkflowCancelled(_)
            | HistoryEventData::TimerStarted(_)
            | HistoryEventData::TimerFired(_)
            | HistoryEventData::SelectWinner(_)
            | HistoryEventData::VersionMarker(_)
            | HistoryEventData::DeprecatedPatchMarker(_) => Ok(()),
        }
    }

    pub(super) async fn collect_failure_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        failure: &DurableFailure,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        if let Some(details) = &failure.details {
            self.collect_payload_blob_ref_tx(tx, details, reachable)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn collect_payload_blob_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        let PayloadRef::Blob { digest, uri, .. } = payload else {
            return Ok(());
        };
        if uri.starts_with("postgres://payload/") {
            self.load_payload_blob_tx(tx, payload).await?;
        } else if !is_opaque_external_payload_ref(payload) {
            self.load_payload_blob_tx(tx, payload).await?;
        }
        reachable.insert(digest.clone());
        Ok(())
    }

    pub(super) async fn activity_map_input_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_activity_map_input_manifest_from_storage_tx(tx, payload)
            .await
    }

    pub(super) async fn activity_map_result_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_activity_map_result_manifest_from_storage_tx(tx, payload)
            .await
    }

    pub(super) async fn collect_activity_map_input_manifest_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        self.collect_payload_blob_ref_tx(tx, payload, reachable)
            .await?;
        if is_opaque_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_opaque_external_payload_ref(&page) {
                continue;
            }
            let page_payload = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page: ActivityMapInputPage = crate::decode_payload(&page_payload)?;
            for item in page.items {
                self.collect_payload_blob_ref_tx(tx, &item, reachable)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn collect_activity_map_result_manifest_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        self.collect_payload_blob_ref_tx(tx, payload, reachable)
            .await?;
        if is_opaque_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapResultManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_opaque_external_payload_ref(&page) {
                continue;
            }
            let page_payload = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page: ActivityMapResultPage = crate::decode_payload(&page_payload)?;
            for result in page.results {
                self.collect_payload_blob_ref_tx(tx, &result, reachable)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn normalize_payload_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        match payload {
            PayloadRef::Inline {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                bytes,
            } if bytes.len() > self.payload_config.inline_threshold_bytes => {
                let digest = digest_bytes(&bytes);
                let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let encryption_blob = encode_encryption_metadata(&encryption)?;
                let schema = self.schema_sql();
                tx.execute(
                    &format!(
                        "insert into {schema}.payload_blobs
                         (digest, codec, schema_fingerprint, compression, encryption, size, bytes)
                         values ($1, $2, $3, $4, $5, $6, $7)
                         on conflict(digest) do nothing"
                    ),
                    &[
                        &digest,
                        &codec_to_str(codec),
                        &schema_fingerprint.0,
                        &compression_to_str(compression),
                        &encryption_blob,
                        &i64::try_from(size).unwrap_or(i64::MAX),
                        &bytes,
                    ],
                )
                .await
                .map_err(postgres_error)?;
                Ok(PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    digest: digest.clone(),
                    size,
                    uri: format!("postgres://payload/{digest}"),
                })
            }
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } => {
                if !is_opaque_external_payload_ref(&payload) {
                    self.load_payload_blob_tx(tx, &payload).await?;
                }
                Ok(payload)
            }
        }
    }

    pub(super) async fn hydrate_payload_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        match payload {
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } if is_opaque_external_payload_ref(&payload) => {
                Ok(payload)
            }
            payload @ PayloadRef::Blob { .. } => {
                let PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    ..
                } = &payload
                else {
                    unreachable!();
                };
                let blob = self.load_payload_blob(&payload).await?;
                Ok(PayloadRef::Inline {
                    codec: *codec,
                    schema_fingerprint: schema_fingerprint.clone(),
                    compression: *compression,
                    encryption: encryption.clone(),
                    bytes: blob.bytes,
                })
            }
        }
    }

    pub(super) async fn hydrate_payload_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        match payload {
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } if is_opaque_external_payload_ref(&payload) => {
                Ok(payload)
            }
            payload @ PayloadRef::Blob { .. } => {
                let PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    ..
                } = &payload
                else {
                    unreachable!();
                };
                let blob = self.load_payload_blob_tx(tx, &payload).await?;
                Ok(PayloadRef::Inline {
                    codec: *codec,
                    schema_fingerprint: schema_fingerprint.clone(),
                    compression: *compression,
                    encryption: encryption.clone(),
                    bytes: blob.bytes,
                })
            }
        }
    }

    pub(super) async fn normalize_activity_map_input_manifest_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.normalize_payload_for_storage_tx(tx, item).await?);
            }
            page.items = items;
            let page = crate::encode_payload_with_codec(&page, self.payload_config.codec)?;
            pages.push(self.normalize_payload_for_storage_tx(tx, page).await?);
        }
        manifest.pages = pages;
        let root = crate::encode_payload_with_codec(&manifest, self.payload_config.codec)?;
        self.normalize_payload_for_storage_tx(tx, root).await
    }

    pub(super) async fn normalize_activity_map_result_manifest_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.normalize_payload_for_storage_tx(tx, result).await?);
            }
            page.results = results;
            let page = crate::encode_payload_with_codec(&page, self.payload_config.codec)?;
            pages.push(self.normalize_payload_for_storage_tx(tx, page).await?);
        }
        manifest.pages = pages;
        let root = crate::encode_payload_with_codec(&manifest, self.payload_config.codec)?;
        self.normalize_payload_for_storage_tx(tx, root).await
    }

    pub(super) async fn hydrate_activity_map_input_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage(page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.hydrate_payload_from_storage(item).await?);
            }
            page.items = items;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    pub(super) async fn hydrate_activity_map_result_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage(page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.hydrate_payload_from_storage(result).await?);
            }
            page.results = results;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    pub(super) async fn hydrate_activity_map_input_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.hydrate_payload_from_storage_tx(tx, item).await?);
            }
            page.items = items;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    pub(super) async fn hydrate_activity_map_result_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.hydrate_payload_from_storage_tx(tx, result).await?);
            }
            page.results = results;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    pub(super) async fn normalize_history_event_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        data: HistoryEventData,
    ) -> Result<HistoryEventData> {
        match data {
            HistoryEventData::WorkflowStarted {
                workflow_type,
                input,
            } => Ok(HistoryEventData::WorkflowStarted {
                workflow_type,
                input: self.normalize_payload_for_storage_tx(tx, input).await?,
            }),
            HistoryEventData::WorkflowCompleted { result } => {
                Ok(HistoryEventData::WorkflowCompleted {
                    result: self.normalize_payload_for_storage_tx(tx, result).await?,
                })
            }
            HistoryEventData::WorkflowFailed { failure } => Ok(HistoryEventData::WorkflowFailed {
                failure: self.normalize_failure_for_storage_tx(tx, failure).await?,
            }),
            HistoryEventData::WorkflowContinuedAsNew { input } => {
                Ok(HistoryEventData::WorkflowContinuedAsNew {
                    input: self.normalize_payload_for_storage_tx(tx, input).await?,
                })
            }
            HistoryEventData::ActivityScheduled(mut scheduled) => {
                scheduled.input = self
                    .normalize_payload_for_storage_tx(tx, scheduled.input)
                    .await?;
                Ok(HistoryEventData::ActivityScheduled(scheduled))
            }
            HistoryEventData::ActivityMapScheduled(mut scheduled) => {
                scheduled.input_manifest = self
                    .normalize_activity_map_input_manifest_for_storage_tx(
                        tx,
                        scheduled.input_manifest,
                    )
                    .await?;
                Ok(HistoryEventData::ActivityMapScheduled(scheduled))
            }
            HistoryEventData::ActivityMapCompleted(mut completed) => {
                completed.result_manifest = self
                    .normalize_activity_map_result_manifest_for_storage_tx(
                        tx,
                        completed.result_manifest,
                    )
                    .await?;
                Ok(HistoryEventData::ActivityMapCompleted(completed))
            }
            HistoryEventData::ActivityMapFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ActivityMapFailed(failed))
            }
            HistoryEventData::ActivityCompleted(mut completed) => {
                completed.result = self
                    .normalize_payload_for_storage_tx(tx, completed.result)
                    .await?;
                Ok(HistoryEventData::ActivityCompleted(completed))
            }
            HistoryEventData::ActivityFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ActivityFailed(failed))
            }
            HistoryEventData::ChildWorkflowStartRequested(mut requested) => {
                requested.input = self
                    .normalize_payload_for_storage_tx(tx, requested.input)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowStartRequested(requested))
            }
            HistoryEventData::ChildWorkflowCompleted(mut completed) => {
                completed.result = self
                    .normalize_payload_for_storage_tx(tx, completed.result)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowCompleted(completed))
            }
            HistoryEventData::ChildWorkflowFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowFailed(failed))
            }
            HistoryEventData::SignalConsumed(mut signal) => {
                signal.payload = self
                    .normalize_payload_for_storage_tx(tx, signal.payload)
                    .await?;
                Ok(HistoryEventData::SignalConsumed(signal))
            }
            other => Ok(other),
        }
    }

    pub(super) async fn normalize_activity_task_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: crate::ActivityTask,
    ) -> Result<crate::ActivityTask> {
        task.input = self
            .normalize_payload_for_storage_tx(tx, task.input)
            .await?;
        Ok(task)
    }

    pub(super) async fn normalize_activity_map_task_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: ActivityMapTask,
    ) -> Result<ActivityMapTask> {
        task.input_manifest = self
            .normalize_activity_map_input_manifest_for_storage_tx(tx, task.input_manifest)
            .await?;
        Ok(task)
    }

    pub(super) async fn normalize_child_start_message_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut message: ChildStartOutboxMessage,
    ) -> Result<ChildStartOutboxMessage> {
        message.input = self
            .normalize_payload_for_storage_tx(tx, message.input)
            .await?;
        Ok(message)
    }

    pub(super) async fn hydrate_activity_task_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: ActivityTask,
    ) -> Result<ActivityTask> {
        task.input = self.hydrate_payload_from_storage_tx(tx, task.input).await?;
        Ok(task)
    }

    pub(super) async fn normalize_failure_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut failure: DurableFailure,
    ) -> Result<DurableFailure> {
        if let Some(details) = failure.details.take() {
            failure.details = Some(self.normalize_payload_for_storage_tx(tx, details).await?);
        }
        Ok(failure)
    }

    pub(super) async fn hydrate_history_event_from_storage(
        &self,
        data: HistoryEventData,
    ) -> Result<HistoryEventData> {
        match data {
            HistoryEventData::WorkflowStarted {
                workflow_type,
                input,
            } => Ok(HistoryEventData::WorkflowStarted {
                workflow_type,
                input: self.hydrate_payload_from_storage(input).await?,
            }),
            HistoryEventData::WorkflowCompleted { result } => {
                Ok(HistoryEventData::WorkflowCompleted {
                    result: self.hydrate_payload_from_storage(result).await?,
                })
            }
            HistoryEventData::WorkflowFailed { failure } => Ok(HistoryEventData::WorkflowFailed {
                failure: self.hydrate_failure_from_storage(failure).await?,
            }),
            HistoryEventData::WorkflowContinuedAsNew { input } => {
                Ok(HistoryEventData::WorkflowContinuedAsNew {
                    input: self.hydrate_payload_from_storage(input).await?,
                })
            }
            HistoryEventData::ActivityScheduled(mut scheduled) => {
                scheduled.input = self.hydrate_payload_from_storage(scheduled.input).await?;
                Ok(HistoryEventData::ActivityScheduled(scheduled))
            }
            HistoryEventData::ActivityMapScheduled(mut scheduled) => {
                scheduled.input_manifest = self
                    .hydrate_activity_map_input_manifest_from_storage(scheduled.input_manifest)
                    .await?;
                Ok(HistoryEventData::ActivityMapScheduled(scheduled))
            }
            HistoryEventData::ActivityMapCompleted(mut completed) => {
                completed.result_manifest = self
                    .hydrate_activity_map_result_manifest_from_storage(completed.result_manifest)
                    .await?;
                Ok(HistoryEventData::ActivityMapCompleted(completed))
            }
            HistoryEventData::ActivityMapFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ActivityMapFailed(failed))
            }
            HistoryEventData::ActivityCompleted(mut completed) => {
                completed.result = self.hydrate_payload_from_storage(completed.result).await?;
                Ok(HistoryEventData::ActivityCompleted(completed))
            }
            HistoryEventData::ActivityFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ActivityFailed(failed))
            }
            HistoryEventData::ChildWorkflowStartRequested(mut requested) => {
                requested.input = self.hydrate_payload_from_storage(requested.input).await?;
                Ok(HistoryEventData::ChildWorkflowStartRequested(requested))
            }
            HistoryEventData::ChildWorkflowCompleted(mut completed) => {
                completed.result = self.hydrate_payload_from_storage(completed.result).await?;
                Ok(HistoryEventData::ChildWorkflowCompleted(completed))
            }
            HistoryEventData::ChildWorkflowFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ChildWorkflowFailed(failed))
            }
            HistoryEventData::SignalConsumed(mut signal) => {
                signal.payload = self.hydrate_payload_from_storage(signal.payload).await?;
                Ok(HistoryEventData::SignalConsumed(signal))
            }
            other => Ok(other),
        }
    }

    pub(super) async fn hydrate_failure_from_storage(
        &self,
        mut failure: DurableFailure,
    ) -> Result<DurableFailure> {
        if let Some(details) = failure.details.take() {
            failure.details = Some(self.hydrate_payload_from_storage(details).await?);
        }
        Ok(failure)
    }

    pub(super) async fn load_payload_blob_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
    ) -> Result<PayloadBlob> {
        let PayloadRef::Blob {
            codec: ref_codec,
            schema_fingerprint: ref_schema_fingerprint,
            compression: ref_compression,
            encryption: ref_encryption,
            digest,
            size,
            uri: _,
        } = payload
        else {
            return Err(Error::PayloadDecode(
                "inline payload does not reference blob storage".to_owned(),
            ));
        };
        let schema = self.schema_sql();
        let row = tx
            .query_opt(
                &format!(
                    "select codec, schema_fingerprint, compression, encryption, size, bytes
                     from {schema}.payload_blobs
                     where digest = $1"
                ),
                &[digest],
            )
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))?;
        decode_payload_blob_row(
            payload,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            *ref_codec,
            ref_schema_fingerprint,
            *ref_compression,
            ref_encryption,
            digest,
            *size,
        )
    }

    pub(super) async fn load_payload_blob(&self, payload: &PayloadRef) -> Result<PayloadBlob> {
        let PayloadRef::Blob {
            codec: ref_codec,
            schema_fingerprint: ref_schema_fingerprint,
            compression: ref_compression,
            encryption: ref_encryption,
            digest,
            size,
            uri: _,
        } = payload
        else {
            return Err(Error::PayloadDecode(
                "inline payload does not reference blob storage".to_owned(),
            ));
        };
        let schema = self.schema_sql();
        let client = self.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "select codec, schema_fingerprint, compression, encryption, size, bytes
                     from {schema}.payload_blobs
                     where digest = $1"
                ),
                &[digest],
            )
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))?;
        decode_payload_blob_row(
            payload,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            *ref_codec,
            ref_schema_fingerprint,
            *ref_compression,
            ref_encryption,
            digest,
            *size,
        )
    }
}
