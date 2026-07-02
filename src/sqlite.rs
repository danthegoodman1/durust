use crate::provider_util::{
    ActivityFailureDecision, TerminalCleanup, activity_claim_lease_timeout_at_ms,
    activity_failure_decision, activity_timeout_at_ms, activity_timeout_at_ms_from,
    activity_timeout_decision, child_terminal_event_data_and_reason,
    child_terminal_map_item_outcome, claim_lease_until_ms, codec_from_str, codec_to_str,
    commit_has_workflow_visible_mutations, compression_from_str, compression_to_str,
    decode_encryption_metadata, encode_encryption_metadata, event_type_from_str, event_type_to_str,
    marker_kind_from_str, marker_kind_to_str, parent_close_policy_to_str, payload_gc_cutoff_ms,
    post_commit_ready_reason, ready_at_ms_for_delay, reason_from_str, reason_to_str,
    timed_out_by_heartbeat, timeout_message, unix_epoch_millis, wait_kind_to_str,
};
use crate::{
    ActivityFailed, ActivityId, ActivityMapInputManifest, ActivityMapInputPage, ActivityMapItem,
    ActivityMapResultManifest, ActivityMapResultPage, ActivityMapTask, ActivityTask,
    ActivityTaskClaim, BlobStoreConfig, CancelWorkflowOutcome, CancelWorkflowRequest,
    ChildStartOutboxMessage, ChildWorkflowMapFailureMode, ChildWorkflowMapItem,
    ChildWorkflowMapItemOutcome, ChildWorkflowMapTask, ClaimActivityOptions,
    ClaimWorkflowTaskOptions, ClaimedActivityTask, ClaimedWorkflowTask, CommandId, CommandSeq,
    CommitOutcome, CompleteActivityOutcome, CompleteActivityRequest,
    DispatchChildWorkflowStartsOutcome, DispatchChildWorkflowStartsRequest, DurableBackend, Error,
    EventId, FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, ParentClosePolicy, PayloadBlob, PayloadRef,
    PayloadRootRef, PayloadRootsOutcome, PayloadStorageConfig, ReadSignalInboxRequest, Result,
    RunId, SignalInboxRecord, SignalWorkflowOutcome, SignalWorkflowRequest, StartWorkflowOutcome,
    StartWorkflowRequest, TimeoutDueActivitiesOutcome, TimeoutDueActivitiesRequest, TimestampMs,
    WaitKind, WorkerId, WorkflowChangeMarkerKind, WorkflowChangeVersionRecord,
    WorkflowChangeVersionStatus, WorkflowChangeVersionsOutcome, WorkflowChangeVersionsRequest,
    WorkflowId, WorkflowTaskClaim, WorkflowTaskCommit, WorkflowTaskReason, WorkflowType,
    activity_map_input_at, digest_bytes, encode_activity_map_result_manifest_with_codec,
    encode_child_workflow_map_result_manifest_with_codec, event_payload_len, is_terminal,
};
use futures::future::{BoxFuture, ready};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct SqliteBackend {
    path: PathBuf,
    payload_config: PayloadStorageConfig,
    conn: Arc<Mutex<Connection>>,
}

impl fmt::Debug for SqliteBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteBackend")
            .field("path", &self.path)
            .field("payload_config", &self.payload_config)
            .finish_non_exhaustive()
    }
}

impl SqliteBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = open_sqlite_connection(&path)?;
        configure_journal_mode(&conn)?;
        init_schema(&conn)?;
        Ok(Self {
            path,
            payload_config: PayloadStorageConfig::default(),
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn open_with_payload_storage(
        path: impl AsRef<Path>,
        payload_config: PayloadStorageConfig,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = open_sqlite_connection(&path)?;
        configure_journal_mode(&conn)?;
        init_schema(&conn)?;
        Ok(Self {
            path,
            payload_config,
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn payload_blob_count(&self) -> Result<usize> {
        let conn = self.connection()?;
        let sqlite_blobs = conn
            .query_row("select count(*) from payload_blobs", [], |row| {
                row.get::<_, usize>(0)
            })
            .map_err(sqlite_error)?;
        let external_blobs = external_blob_listings(&self.payload_config)?.len();
        Ok(sqlite_blobs + external_blobs)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| Error::Backend("sqlite connection mutex poisoned".to_owned()))
    }
}

fn open_sqlite_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .map_err(sqlite_error)?;
    configure_connection_defaults(&conn)?;
    Ok(conn)
}

impl DurableBackend for SqliteBackend {
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.payload_config.clone()
    }

    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            if let Some(run_id) = tx
                .query_row(
                    "select run_id from workflow_instances where namespace = ?1 and workflow_id = ?2",
                    params![req.namespace.0, req.workflow_id.0],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sqlite_error)?
            {
                tx.commit().map_err(sqlite_error)?;
                return Ok(StartWorkflowOutcome::AlreadyStarted {
                    run_id: RunId::new(run_id),
                });
            }

            let input = normalize_payload_for_storage(&tx, &self.payload_config, req.input)?;
            let run_id = RunId::new(format!("run-{}", next_counter(&tx, "run")?));
            let start = HistoryEventData::WorkflowStarted {
                workflow_type: req.workflow_type.clone(),
                input,
            };
            tx.execute(
                "insert into workflow_instances
                 (namespace, workflow_id, run_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy)
                 values (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, 0, null, 0, null, null, null)",
                params![
                    req.namespace.0,
                    req.workflow_id.0,
                    run_id.0,
                    req.workflow_type.name,
                    req.workflow_type.version,
                    req.task_queue.0,
                    reason_to_str(&WorkflowTaskReason::WorkflowStarted)
                ],
            )
            .map_err(sqlite_error)?;
            insert_history_event(&tx, &run_id, EventId(1), start)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(StartWorkflowOutcome::Started { run_id })
        })();
        Box::pin(ready(result))
    }

    fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> BoxFuture<'static, Result<CancelWorkflowOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((run_id, tail, terminal)) = tx
                .query_row(
                    "select run_id, current_event_id, terminal
                     from workflow_instances
                     where namespace = ?1 and workflow_id = ?2",
                    params![req.namespace.0, req.workflow_id.0],
                    |row| {
                        Ok((
                            RunId::new(row.get::<_, String>(0)?),
                            row.get::<_, u64>(1)?,
                            row.get::<_, bool>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::Backend(format!(
                    "workflow `{}` was not found",
                    req.workflow_id.0
                )));
            };
            if terminal {
                tx.commit().map_err(sqlite_error)?;
                return Ok(CancelWorkflowOutcome::AlreadyTerminal { run_id });
            }

            let event_id = EventId(tail).next();
            let terminal_event = HistoryEventData::WorkflowCancelled { reason: req.reason };
            insert_history_event(&tx, &run_id, event_id, terminal_event.clone())?;
            cleanup_run_operational_state(&tx, &run_id, TerminalCleanup::Closed)?;
            tx.execute(
                "update workflow_instances
                 set current_event_id = ?1,
                     workflow_claim_token = null,
                     terminal = 1,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = ?2",
                params![event_id.0, run_id.0],
            )
            .map_err(sqlite_error)?;
            handle_terminal_run(&tx, &self.payload_config, &run_id, &terminal_event)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(CancelWorkflowOutcome::Cancelled { run_id, event_id })
        })();
        Box::pin(ready(result))
    }

    fn current_time(&self) -> BoxFuture<'static, Result<TimestampMs>> {
        Box::pin(ready(Ok(TimestampMs(unix_epoch_millis()))))
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedWorkflowTask>>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let now_ms = unix_epoch_millis();
            // A held claim blocks reclaiming only while its lease is unexpired.
            // A held claim with a null lease (only possible for rows claimed
            // before the lease column existed) stays unclaimable, failing safe.
            let mut stmt = tx
                .prepare(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason
                     from workflow_instances
                     where namespace = ?1
                       and task_queue = ?2
                       and ready_reason is not null
                       and ready_at_ms <= ?3
                       and (workflow_claim_token is null or claim_lease_until_ms <= ?3)
                       and terminal = 0
                     order by rowid asc",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(
                    params![opts.namespace.0, opts.task_queue.0, now_ms],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, u32>(3)?,
                            row.get::<_, u64>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .map_err(sqlite_error)?;

            let mut selected = None;
            for row in rows {
                let (run_id, workflow_id, name, version, tail, reason) =
                    row.map_err(sqlite_error)?;
                let workflow_type = WorkflowType::new(name, version);
                if opts
                    .registered_workflow_types
                    .iter()
                    .any(|registered| registered == &workflow_type)
                {
                    selected = Some((
                        RunId::new(run_id),
                        WorkflowId::new(workflow_id),
                        workflow_type,
                        EventId(tail),
                        reason_from_str(&reason)?,
                    ));
                    break;
                }
            }
            drop(stmt);

            let Some((run_id, workflow_id, workflow_type, tail, reason)) = selected else {
                tx.commit().map_err(sqlite_error)?;
                return Ok(None);
            };
            // The ready reason and visibility stay on the row while claimed so
            // a reclaim after lease expiry hands out the same task a fresh
            // claim would; commit, conflict, and release overwrite them.
            let token = next_counter(&tx, "claim")?;
            tx.execute(
                "update workflow_instances
                 set workflow_claim_token = ?1, claim_lease_until_ms = ?2
                 where run_id = ?3",
                params![
                    token,
                    claim_lease_until_ms(TimestampMs(now_ms), opts.lease_duration),
                    run_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(Some(ClaimedWorkflowTask {
                run_id: run_id.clone(),
                workflow_id,
                workflow_type,
                claim: WorkflowTaskClaim {
                    run_id,
                    worker_id,
                    token,
                },
                replay_target_event_id: tail,
                reason,
                prefetched_history: Vec::new(),
            }))
        })();
        Box::pin(ready(result))
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let result = (|| {
            let conn = self.connection()?;
            let max_events = req.max_events.max(1);
            let max_bytes = req.max_bytes.max(1);
            let mut stmt = conn
                .prepare(
                    "select event_id, event_type, data
                     from history_events
                     where run_id = ?1 and event_id > ?2 and event_id <= ?3
                     order by event_id asc",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(
                    params![req.run_id.0, req.after_event_id.0, req.up_to_event_id.0],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .map_err(sqlite_error)?;

            let mut events = Vec::new();
            let mut bytes = 0usize;
            for row in rows {
                let (event_id, event_type, data) = row.map_err(sqlite_error)?;
                let data: HistoryEventData = rmp_serde::from_slice(&data)
                    .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                let event_bytes = event_payload_len(&data).max(1);
                if !events.is_empty()
                    && (events.len() >= max_events || bytes + event_bytes > max_bytes)
                {
                    break;
                }
                let data = hydrate_history_event_from_storage(&conn, &self.payload_config, data)?;
                bytes += event_bytes;
                events.push(HistoryEvent {
                    event_id: EventId(event_id),
                    event_type: event_type_from_str(&event_type)?,
                    data,
                });
                if events.len() >= max_events {
                    break;
                }
            }

            let last_event_id = events
                .last()
                .map(|event| event.event_id)
                .unwrap_or(req.after_event_id);
            let has_more = conn
                .query_row(
                    "select 1 from history_events
                     where run_id = ?1 and event_id > ?2 and event_id <= ?3
                     limit 1",
                    params![req.run_id.0, last_event_id.0, req.up_to_event_id.0],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sqlite_error)?
                .is_some();

            Ok(HistoryChunk {
                events,
                last_event_id,
                has_more,
            })
        })();
        Box::pin(ready(result))
    }

    fn stream_history_for_replay(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let result = (|| {
            let conn = self.connection()?;
            let max_events = req.max_events.max(1);
            let max_bytes = req.max_bytes.max(1);
            let mut stmt = conn
                .prepare(
                    "select event_id, event_type, data
                     from history_events
                     where run_id = ?1 and event_id > ?2 and event_id <= ?3
                     order by event_id asc",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(
                    params![req.run_id.0, req.after_event_id.0, req.up_to_event_id.0],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .map_err(sqlite_error)?;

            let mut events = Vec::new();
            let mut bytes = 0usize;
            for row in rows {
                let (event_id, event_type, data) = row.map_err(sqlite_error)?;
                let data: HistoryEventData = rmp_serde::from_slice(&data)
                    .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                let event_bytes = event_payload_len(&data).max(1);
                if !events.is_empty()
                    && (events.len() >= max_events || bytes + event_bytes > max_bytes)
                {
                    break;
                }
                bytes += event_bytes;
                events.push(HistoryEvent {
                    event_id: EventId(event_id),
                    event_type: event_type_from_str(&event_type)?,
                    data,
                });
                if events.len() >= max_events {
                    break;
                }
            }

            let last_event_id = events
                .last()
                .map(|event| event.event_id)
                .unwrap_or(req.after_event_id);
            let has_more = conn
                .query_row(
                    "select 1 from history_events
                     where run_id = ?1 and event_id > ?2 and event_id <= ?3
                     limit 1",
                    params![req.run_id.0, last_event_id.0, req.up_to_event_id.0],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sqlite_error)?
                .is_some();

            Ok(HistoryChunk {
                events,
                last_event_id,
                has_more,
            })
        })();
        Box::pin(ready(result))
    }

    fn hydrate_payload(&self, payload: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        let result = (|| {
            let conn = self.connection()?;
            hydrate_payload_from_storage(&conn, &self.payload_config, payload)
        })();
        Box::pin(ready(result))
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let result = (|| {
            let conn = self.connection()?;
            hydrate_activity_map_result_manifest_from_storage(&conn, &self.payload_config, payload)
        })();
        Box::pin(ready(result))
    }

    fn hydrate_child_workflow_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let result = (|| {
            let conn = self.connection()?;
            hydrate_child_workflow_map_result_manifest_from_storage(
                &conn,
                &self.payload_config,
                payload,
            )
        })();
        Box::pin(ready(result))
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((current_tail, claim_token, terminal, namespace, workflow_id)) = tx
                .query_row(
                    "select current_event_id, workflow_claim_token, terminal, namespace, workflow_id
                     from workflow_instances where run_id = ?1",
                    params![claim.run_id.0],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, Option<u64>>(1)?,
                            row.get::<_, bool>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::RunNotFound(claim.run_id));
            };
            if claim_token != Some(claim.token) {
                return Err(Error::StaleLease);
            }
            if EventId(current_tail) != batch.expected_tail_event_id {
                tx.execute(
                    "update workflow_instances
                     set workflow_claim_token = null, ready_reason = ?1, ready_at_ms = 0
                     where run_id = ?2",
                    params![
                        reason_to_str(&WorkflowTaskReason::CacheEvicted),
                        claim.run_id.0
                    ],
                )
                .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)?;
                return Ok(CommitOutcome::Conflict);
            }
            if terminal && commit_has_workflow_visible_mutations(&batch) {
                return Err(Error::TerminalWorkflow);
            }

            let config = self.payload_config.clone();
            let append_events =
                normalize_history_events_for_storage(&tx, &config, batch.append_events)?;
            let schedule_activities =
                normalize_activity_tasks_for_storage(&tx, &config, batch.schedule_activities)?;
            let schedule_activity_maps = batch
                .schedule_activity_maps
                .into_iter()
                .map(|task| normalize_activity_map_task_for_storage(&tx, &config, task))
                .collect::<Result<Vec<_>>>()?;
            let schedule_child_workflow_maps = batch
                .schedule_child_workflow_maps
                .into_iter()
                .map(|task| normalize_child_workflow_map_task_for_storage(&tx, &config, task))
                .collect::<Result<Vec<_>>>()?;
            let start_child_workflows = batch
                .start_child_workflows
                .into_iter()
                .map(|message| normalize_child_start_message_for_storage(&tx, &config, message))
                .collect::<Result<Vec<_>>>()?;
            let query_projection = batch
                .query_projection
                .map(|payload| normalize_payload_for_storage(&tx, &config, payload))
                .transpose()?;

            let mut next_event_id = EventId(current_tail);
            let mut became_terminal = false;
            let mut terminal_event = None;
            for event in append_events {
                next_event_id = next_event_id.next();
                became_terminal |= is_terminal(&event.data);
                if is_terminal(&event.data) {
                    terminal_event = Some(event.data.clone());
                }
                insert_history_event(&tx, &claim.run_id, next_event_id, event.data)?;
            }
            for task in schedule_activities {
                let timeout_at_ms = activity_timeout_at_ms(task.start_to_close_timeout);
                let task_blob = rmp_serde::to_vec_named(&task)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?;
                tx.execute(
                    "insert into activity_tasks
                     (activity_id, namespace, run_id, activity_name, task_queue, task,
                      claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
                     values (?1, ?2, ?3, ?4, ?5, ?6, null, 0, ?7, null)",
                    params![
                        task.activity_id.0,
                        namespace.as_str(),
                        task.run_id.0,
                        task.activity_name.0,
                        task.task_queue.0,
                        task_blob,
                        timeout_at_ms
                    ],
                )
                .map_err(sqlite_error)?;
            }
            for map_task in schedule_activity_maps {
                insert_activity_map(&tx, &config, namespace.as_str(), &map_task)?;
                materialize_activity_map_items(&tx, &config, &map_task.map_command_id)?;
            }
            for map_task in schedule_child_workflow_maps {
                insert_child_workflow_map(&tx, &config, namespace.as_str(), &map_task)?;
                materialize_child_workflow_map_items(&tx, &config, &map_task.map_command_id)?;
            }
            for message in start_child_workflows {
                insert_child_outbox(&tx, namespace.as_str(), &message)?;
            }
            for wait in batch.upsert_waits {
                tx.execute(
                    "insert into active_waits
                     (wait_id, run_id, command_seq, kind, wait_key, ready_at_ms)
                     values (?1, ?2, ?3, ?4, ?5, ?6)
                     on conflict(wait_id) do update set
                        run_id = excluded.run_id,
                        command_seq = excluded.command_seq,
                        kind = excluded.kind,
                        wait_key = excluded.wait_key,
                        ready_at_ms = excluded.ready_at_ms",
                    params![
                        wait.wait_id.0,
                        wait.run_id.0,
                        wait.command_id.seq.0,
                        wait_kind_to_str(&wait.kind),
                        wait.key,
                        wait.ready_at.map(|ready_at| ready_at.0),
                    ],
                )
                .map_err(sqlite_error)?;
            }
            for signal_id in batch.consume_signals {
                tx.execute(
                    "update signals set consumed = 1 where signal_id = ?1",
                    params![signal_id.0],
                )
                .map_err(sqlite_error)?;
            }
            for wait_id in batch.delete_waits {
                tx.execute(
                    "delete from active_waits where wait_id = ?1",
                    params![wait_id.0],
                )
                .map_err(sqlite_error)?;
            }
            for command_id in batch.cancel_commands {
                cancel_command_operational_state(&tx, &command_id)?;
            }
            if let Some(payload) = query_projection {
                let payload_blob = rmp_serde::to_vec_named(&payload)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?;
                tx.execute(
                    "insert into query_projections
                     (namespace, workflow_id, run_id, event_id, payload)
                     values (?1, ?2, ?3, ?4, ?5)
                     on conflict(namespace, workflow_id) do update set
                        run_id = excluded.run_id,
                        event_id = excluded.event_id,
                        payload = excluded.payload",
                    params![
                        namespace.as_str(),
                        workflow_id.as_str(),
                        claim.run_id.0,
                        next_event_id.0,
                        payload_blob
                    ],
                )
                .map_err(sqlite_error)?;
            }
            let terminal_after_commit = became_terminal || terminal;
            if terminal_after_commit {
                let cleanup = terminal_event
                    .as_ref()
                    .map(TerminalCleanup::for_terminal_event)
                    .unwrap_or(TerminalCleanup::Closed);
                cleanup_run_operational_state(&tx, &claim.run_id, cleanup)?;
                if let Some(event @ HistoryEventData::WorkflowContinuedAsNew { .. }) =
                    terminal_event.clone()
                {
                    continue_run_as_new(&tx, &claim.run_id, event)?;
                    tx.commit().map_err(sqlite_error)?;
                    return Ok(CommitOutcome::Committed {
                        new_tail_event_id: next_event_id,
                    });
                }
                if let Some(event) = terminal_event {
                    handle_terminal_run(&tx, &config, &claim.run_id, &event)?;
                }
            }
            // SQLite applies child starts through the outbox, so no
            // same-commit child reason exists; only the signal recheck can
            // re-mark the run.
            let signal_ready = !terminal_after_commit && signal_wait_ready(&tx, &claim.run_id)?;
            let ready_reason = post_commit_ready_reason(terminal_after_commit, None, signal_ready)
                .as_ref()
                .map(reason_to_str);
            tx.execute(
                "update workflow_instances
                 set current_event_id = ?1,
                     workflow_claim_token = null,
                     terminal = ?2,
                     ready_reason = ?3,
                     ready_at_ms = 0
                 where run_id = ?4",
                params![
                    next_event_id.0,
                    terminal_after_commit,
                    ready_reason,
                    claim.run_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(CommitOutcome::Committed {
                new_tail_event_id: next_event_id,
            })
        })();
        Box::pin(ready(result))
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((claim_token, terminal)) = tx
                .query_row(
                    "select workflow_claim_token, terminal from workflow_instances where run_id = ?1",
                    params![claim.run_id.0],
                    |row| Ok((row.get::<_, Option<u64>>(0)?, row.get::<_, bool>(1)?)),
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::RunNotFound(claim.run_id));
            };
            if claim_token != Some(claim.token) {
                return Err(Error::StaleLease);
            }
            let ready_reason = (!terminal).then(|| reason_to_str(&release.reason));
            let ready_at_ms = if terminal {
                0
            } else {
                ready_at_ms_for_delay(release.delay)
            };
            tx.execute(
                "update workflow_instances
                 set workflow_claim_token = null, ready_reason = ?1, ready_at_ms = ?2
                 where run_id = ?3",
                params![ready_reason, ready_at_ms, claim.run_id.0],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)
        })();
        Box::pin(ready(result))
    }

    fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> BoxFuture<'static, Result<SignalWorkflowOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let duplicate = tx
                .query_row(
                    "select 1 from signals where signal_id = ?1 limit 1",
                    params![req.signal_id.0],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sqlite_error)?
                .is_some();
            if duplicate {
                tx.commit().map_err(sqlite_error)?;
                return Ok(SignalWorkflowOutcome::Duplicate);
            }

            let Some((run_id, terminal)) = tx
                .query_row(
                    "select run_id, terminal
                     from workflow_instances
                     where namespace = ?1 and workflow_id = ?2",
                    params![req.namespace.0, req.workflow_id.0],
                    |row| Ok((RunId::new(row.get::<_, String>(0)?), row.get::<_, bool>(1)?)),
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::Backend(format!(
                    "workflow `{}` was not found",
                    req.workflow_id.0
                )));
            };
            if terminal {
                return Err(Error::TerminalWorkflow);
            }

            let received_sequence = next_counter(&tx, "signal")?;
            let payload_ref =
                normalize_payload_for_storage(&tx, &self.payload_config, req.payload)?;
            let payload = rmp_serde::to_vec_named(&payload_ref)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?;
            tx.execute(
                "insert into signals
                 (signal_id, namespace, run_id, signal_name, payload, received_sequence, consumed)
                 values (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                params![
                    req.signal_id.0,
                    req.namespace.0,
                    run_id.0,
                    req.signal_name.0,
                    payload,
                    received_sequence,
                ],
            )
            .map_err(sqlite_error)?;

            if signal_wait_ready(&tx, &run_id)? {
                tx.execute(
                    "update workflow_instances
                     set ready_reason = ?1, ready_at_ms = 0
                     where run_id = ?2 and terminal = 0",
                    params![reason_to_str(&WorkflowTaskReason::SignalReceived), run_id.0],
                )
                .map_err(sqlite_error)?;
            }

            tx.commit().map_err(sqlite_error)?;
            Ok(SignalWorkflowOutcome::Accepted)
        })();
        Box::pin(ready(result))
    }

    fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> BoxFuture<'static, Result<Option<SignalInboxRecord>>> {
        let result = (|| {
            let conn = self.connection()?;
            let record = conn
                .query_row(
                    "select signal_id, signal_name, payload
                     from signals
                     where run_id = ?1 and signal_name = ?2 and consumed = 0
                     order by received_sequence asc
                     limit 1",
                    params![req.run_id.0, req.signal_name.0],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?;
            record
                .map(|(signal_id, signal_name, payload)| {
                    let payload: PayloadRef = rmp_serde::from_slice(&payload)
                        .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                    let payload =
                        hydrate_payload_from_storage(&conn, &self.payload_config, payload)?;
                    Ok(SignalInboxRecord {
                        signal_id: crate::SignalId::new(signal_id),
                        signal_name: crate::SignalName::new(signal_name),
                        payload,
                    })
                })
                .transpose()
        })();
        Box::pin(ready(result))
    }

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let mut stmt = tx
                .prepare(
                    "select w.wait_id, w.run_id, w.command_seq
                     from active_waits w
                     join workflow_instances i on i.run_id = w.run_id
                     where i.namespace = ?1
                       and w.kind = ?2
                       and w.ready_at_ms is not null
                       and w.ready_at_ms <= ?3
                     order by w.ready_at_ms asc, w.wait_id asc
                     limit ?4",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(
                    params![
                        req.namespace.0,
                        wait_kind_to_str(&WaitKind::Timer),
                        req.now.0,
                        i64::try_from(req.limit.max(1)).unwrap_or(i64::MAX)
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, u64>(2)?,
                        ))
                    },
                )
                .map_err(sqlite_error)?;
            let due = rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(sqlite_error)?;
            drop(stmt);

            let mut fired = 0usize;
            for (wait_id, run_id, command_seq) in due {
                let run_id = RunId::new(run_id);
                let Some((tail, terminal)) = tx
                    .query_row(
                        "select current_event_id, terminal
                         from workflow_instances
                         where run_id = ?1",
                        params![run_id.0],
                        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
                    )
                    .optional()
                    .map_err(sqlite_error)?
                else {
                    tx.execute(
                        "delete from active_waits where wait_id = ?1",
                        params![wait_id],
                    )
                    .map_err(sqlite_error)?;
                    continue;
                };
                if terminal {
                    tx.execute(
                        "delete from active_waits where wait_id = ?1",
                        params![wait_id],
                    )
                    .map_err(sqlite_error)?;
                    continue;
                }

                let event_id = EventId(tail).next();
                insert_history_event(
                    &tx,
                    &run_id,
                    event_id,
                    HistoryEventData::TimerFired(crate::TimerFired {
                        command_id: CommandId {
                            run_id: run_id.clone(),
                            seq: CommandSeq(command_seq),
                        },
                        fired_at: req.now,
                    }),
                )?;
                tx.execute(
                    "update workflow_instances
                     set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
                     where run_id = ?3",
                    params![
                        event_id.0,
                        reason_to_str(&WorkflowTaskReason::TimerFired),
                        run_id.0
                    ],
                )
                .map_err(sqlite_error)?;
                tx.execute(
                    "delete from active_waits where wait_id = ?1",
                    params![wait_id],
                )
                .map_err(sqlite_error)?;
                fired += 1;
            }

            tx.commit().map_err(sqlite_error)?;
            Ok(FireDueTimersOutcome { fired })
        })();
        Box::pin(ready(result))
    }

    fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, Result<TimeoutDueActivitiesOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let mut stmt = tx
                .prepare(
                    "select a.activity_id
                     from activity_tasks a
                     join workflow_instances i on i.run_id = a.run_id
                     where a.namespace = ?1
                       and a.completed = 0
                       and (
                         (a.timeout_at_ms is not null and a.timeout_at_ms <= ?2)
                         or
                         (a.heartbeat_deadline_at_ms is not null and a.heartbeat_deadline_at_ms <= ?2)
                       )
                       and i.terminal = 0
                     order by min(
                         coalesce(a.timeout_at_ms, 9223372036854775807),
                         coalesce(a.heartbeat_deadline_at_ms, 9223372036854775807)
                       ) asc,
                       a.activity_id asc
                     limit ?3",
                )
                .map_err(sqlite_error)?;
            let due = stmt
                .query_map(
                    params![
                        req.namespace.0,
                        req.now.0,
                        i64::try_from(req.limit.max(1)).unwrap_or(i64::MAX)
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sqlite_error)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(sqlite_error)?;
            drop(stmt);

            let mut timed_out = 0usize;
            for activity_id in due {
                if timeout_activity(&tx, ActivityId(activity_id), req.now)? {
                    timed_out += 1;
                }
            }

            tx.commit().map_err(sqlite_error)?;
            Ok(TimeoutDueActivitiesOutcome { timed_out })
        })();
        Box::pin(ready(result))
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let now = TimestampMs(unix_epoch_millis());
            let mut stmt = tx
                .prepare(
                    "select a.activity_id, a.activity_name, a.task
                     from activity_tasks a
                     join workflow_instances i on i.run_id = a.run_id
                     where a.namespace = ?1
                       and a.task_queue = ?2
                       and a.completed = 0
                       and a.claim_token is null
                       and (a.timeout_at_ms is null or a.timeout_at_ms > ?3)
                       and i.terminal = 0
                     order by a.rowid asc",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(params![opts.namespace.0, opts.task_queue.0, now.0], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(sqlite_error)?;
            let mut selected = None;
            for row in rows {
                let (activity_id, activity_name, task_blob) = row.map_err(sqlite_error)?;
                if opts
                    .registered_activity_names
                    .iter()
                    .any(|registered| registered.0 == activity_name)
                {
                    let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                        .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                    let task = hydrate_activity_task_from_storage(&tx, &self.payload_config, task)?;
                    if let Some(map_item) = &task.map_item {
                        if activity_map_is_completed(&tx, &map_item.map_command_id)? {
                            tx.execute(
                                "update activity_tasks
                                 set completed = 1,
                                     heartbeat_deadline_at_ms = null
                                 where activity_id = ?1",
                                params![activity_id],
                            )
                            .map_err(sqlite_error)?;
                            continue;
                        }
                    }
                    selected = Some((ActivityId(activity_id), task));
                    break;
                }
            }
            drop(stmt);

            let Some((activity_id, task)) = selected else {
                tx.commit().map_err(sqlite_error)?;
                return Ok(None);
            };
            let token = next_counter(&tx, "claim")?;
            // The lease-derived deadline is only produced for tasks without
            // explicit timeouts, whose stored timeout_at_ms is null; coalesce
            // keeps explicit deadlines authoritative.
            tx.execute(
                "update activity_tasks
                 set claim_token = ?1,
                     heartbeat_deadline_at_ms = ?2,
                     timeout_at_ms = coalesce(?3, timeout_at_ms)
                 where activity_id = ?4",
                params![
                    token,
                    activity_timeout_at_ms_from(now, task.heartbeat_timeout),
                    activity_claim_lease_timeout_at_ms(
                        now,
                        task.start_to_close_timeout,
                        task.heartbeat_timeout,
                        opts.lease_duration,
                    ),
                    activity_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(Some(ClaimedActivityTask {
                task,
                claim: ActivityTaskClaim {
                    activity_id,
                    worker_id,
                    token,
                },
            }))
        })();
        Box::pin(ready(result))
    }

    fn heartbeat_activity(
        &self,
        req: crate::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((task_blob, claim_token, completed)) = tx
                .query_row(
                    "select task, claim_token, completed
                     from activity_tasks
                     where activity_id = ?1",
                    params![req.claim.activity_id.0],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Option<u64>>(1)?,
                            row.get::<_, bool>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                // Activity rows exist until their run's terminal cleanup
                // deletes them, so a missing row is a completed activity.
                tx.commit().map_err(sqlite_error)?;
                return Ok(crate::ActivityHeartbeatOutcome::AlreadyCompleted);
            };
            if completed {
                tx.commit().map_err(sqlite_error)?;
                return Ok(crate::ActivityHeartbeatOutcome::AlreadyCompleted);
            }
            if claim_token != Some(req.claim.token) {
                return Err(Error::StaleLease);
            }

            let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            tx.execute(
                "update activity_tasks
                 set heartbeat_deadline_at_ms = ?1
                 where activity_id = ?2",
                params![
                    activity_timeout_at_ms(task.heartbeat_timeout),
                    req.claim.activity_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(crate::ActivityHeartbeatOutcome::Recorded)
        })();
        Box::pin(ready(result))
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((task_blob, claim_token, completed)) = tx
                .query_row(
                    "select task, claim_token, completed from activity_tasks where activity_id = ?1",
                    params![req.claim.activity_id.0],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Option<u64>>(1)?,
                            row.get::<_, bool>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                // Missing row means the run's terminal cleanup deleted it.
                tx.commit().map_err(sqlite_error)?;
                return Ok(CompleteActivityOutcome::AlreadyCompleted);
            };
            if completed {
                tx.commit().map_err(sqlite_error)?;
                return Ok(CompleteActivityOutcome::AlreadyCompleted);
            }
            if claim_token != Some(req.claim.token) {
                return Err(Error::StaleLease);
            }
            let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            let result = normalize_payload_for_storage(&tx, &self.payload_config, req.result)?;
            if let Some(map_item) = task.map_item.clone() {
                let outcome = complete_map_item(
                    &tx,
                    &self.payload_config,
                    task,
                    map_item,
                    result,
                    req.claim.activity_id.clone(),
                )?;
                tx.commit().map_err(sqlite_error)?;
                return Ok(outcome);
            }
            let Some((tail, terminal)) = tx
                .query_row(
                    "select current_event_id, terminal from workflow_instances where run_id = ?1",
                    params![task.run_id.0],
                    |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::RunNotFound(task.run_id));
            };
            if terminal {
                return Err(Error::TerminalWorkflow);
            }
            let event_id = EventId(tail).next();
            insert_history_event(
                &tx,
                &task.run_id,
                event_id,
                HistoryEventData::ActivityCompleted(crate::ActivityCompleted {
                    command_id: task.command_id,
                    result,
                }),
            )?;
            tx.execute(
                "update workflow_instances
                 set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
                 where run_id = ?3",
                params![
                    event_id.0,
                    reason_to_str(&WorkflowTaskReason::ActivityCompleted),
                    task.run_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.execute(
                "update activity_tasks
                 set completed = 1,
                     heartbeat_deadline_at_ms = null
                 where activity_id = ?1",
                params![req.claim.activity_id.0],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(CompleteActivityOutcome::Completed { event_id })
        })();
        Box::pin(ready(result))
    }

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let Some((task_blob, claim_token, completed)) = tx
                .query_row(
                    "select task, claim_token, completed from activity_tasks where activity_id = ?1",
                    params![req.claim.activity_id.0],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Option<u64>>(1)?,
                            row.get::<_, bool>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                // Missing row means the run's terminal cleanup deleted it.
                tx.commit().map_err(sqlite_error)?;
                return Ok(FailActivityOutcome::AlreadyCompleted);
            };
            if completed {
                tx.commit().map_err(sqlite_error)?;
                return Ok(FailActivityOutcome::AlreadyCompleted);
            }
            if claim_token != Some(req.claim.token) {
                return Err(Error::StaleLease);
            }
            let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            if let ActivityFailureDecision::Retry { next_attempt } =
                activity_failure_decision(&task, req.failure.non_retryable)
            {
                let mut retry_task = task.clone();
                retry_task.attempt = next_attempt;
                let retry_blob = rmp_serde::to_vec_named(&retry_task)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?;
                tx.execute(
                    "update activity_tasks
                     set task = ?1,
                         claim_token = null,
                         timeout_at_ms = ?2,
                         heartbeat_deadline_at_ms = null
                     where activity_id = ?3",
                    params![
                        retry_blob,
                        activity_timeout_at_ms(retry_task.start_to_close_timeout),
                        req.claim.activity_id.0
                    ],
                )
                .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)?;
                return Ok(FailActivityOutcome::RetryScheduled { next_attempt });
            }
            let failure = normalize_failure_for_storage(&tx, &self.payload_config, req.failure)?;
            if let Some(map_item) = task.map_item.clone() {
                let outcome =
                    fail_map_item(&tx, task, map_item, failure, req.claim.activity_id.clone())?;
                tx.commit().map_err(sqlite_error)?;
                return Ok(outcome);
            }
            let Some((tail, terminal)) = tx
                .query_row(
                    "select current_event_id, terminal from workflow_instances where run_id = ?1",
                    params![task.run_id.0],
                    |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
                )
                .optional()
                .map_err(sqlite_error)?
            else {
                return Err(Error::RunNotFound(task.run_id));
            };
            if terminal {
                return Err(Error::TerminalWorkflow);
            }
            let event_id = EventId(tail).next();
            insert_history_event(
                &tx,
                &task.run_id,
                event_id,
                HistoryEventData::ActivityFailed(ActivityFailed {
                    command_id: task.command_id,
                    failure,
                }),
            )?;
            tx.execute(
                "update workflow_instances
                 set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
                 where run_id = ?3",
                params![
                    event_id.0,
                    reason_to_str(&WorkflowTaskReason::ActivityFailed),
                    task.run_id.0
                ],
            )
            .map_err(sqlite_error)?;
            tx.execute(
                "update activity_tasks
                 set completed = 1,
                     heartbeat_deadline_at_ms = null
                 where activity_id = ?1",
                params![req.claim.activity_id.0],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(FailActivityOutcome::Failed { event_id })
        })();
        Box::pin(ready(result))
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let limit = req.limit.max(1);
            let outbox_ids = {
                let mut stmt = tx
                    .prepare(
                        "select outbox_id
                         from child_outbox
                         where namespace = ?1 and dispatched = 0
                         order by outbox_id asc
                         limit ?2",
                    )
                    .map_err(sqlite_error)?;
                let rows = stmt
                    .query_map(params![req.namespace.0, limit as i64], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(sqlite_error)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(sqlite_error)?
            };
            let mut dispatched = 0usize;
            for outbox_id in outbox_ids {
                dispatch_child_start(&tx, &self.payload_config, &outbox_id)?;
                dispatched += 1;
            }
            tx.commit().map_err(sqlite_error)?;
            Ok(DispatchChildWorkflowStartsOutcome { dispatched })
        })();
        Box::pin(ready(result))
    }

    fn query_projection(
        &self,
        req: crate::QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<crate::QueryProjectionOutcome>> {
        let result = (|| {
            let conn = self.connection()?;
            let row = conn
                .query_row(
                    "select run_id, event_id, payload
                     from query_projections
                     where namespace = ?1 and workflow_id = ?2",
                    params![req.namespace.0, req.workflow_id.0],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sqlite_error)?;
            row.map(|(run_id, event_id, payload)| {
                let payload: PayloadRef = rmp_serde::from_slice(&payload)
                    .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                let payload = hydrate_payload_from_storage(&conn, &self.payload_config, payload)?;
                Ok(crate::QueryProjectionOutcome::Found {
                    run_id: RunId::new(run_id),
                    event_id: EventId(event_id),
                    payload,
                })
            })
            .transpose()
            .map(|outcome| outcome.unwrap_or(crate::QueryProjectionOutcome::NotFound))
        })();
        Box::pin(ready(result))
    }

    fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, Result<WorkflowChangeVersionsOutcome>> {
        let result = (|| {
            let conn = self.connection()?;
            let mut stmt = conn
                .prepare(
                    "select c.namespace,
                            c.workflow_id,
                            c.workflow_name,
                            c.workflow_version,
                            c.run_id,
                            c.change_id,
                            c.version,
                            c.marker_kind,
                            c.command_seq,
                            c.first_event_id,
                            c.last_seen_at_ms,
                            i.terminal
                     from workflow_change_versions c
                     join workflow_instances i on i.run_id = c.run_id
                     where c.namespace = ?1
                       and (?2 is null or c.workflow_id = ?2)
                       and (?3 is null or c.run_id = ?3)
                       and (?4 is null or c.change_id = ?4)
                     order by c.workflow_id asc, c.run_id asc, c.change_id asc",
                )
                .map_err(sqlite_error)?;
            let workflow_id = req
                .workflow_id
                .as_ref()
                .map(|workflow_id| workflow_id.0.as_str());
            let run_id = req.run_id.as_ref().map(|run_id| run_id.0.as_str());
            let change_id = req.change_id.as_deref();
            let rows = stmt
                .query_map(
                    params![req.namespace.0, workflow_id, run_id, change_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, u32>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, i32>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, u64>(8)?,
                            row.get::<_, u64>(9)?,
                            row.get::<_, i64>(10)?,
                            row.get::<_, bool>(11)?,
                        ))
                    },
                )
                .map_err(sqlite_error)?;
            let mut records = Vec::new();
            for row in rows {
                let (
                    namespace,
                    workflow_id,
                    workflow_name,
                    workflow_version,
                    run_id,
                    change_id,
                    version,
                    marker_kind,
                    command_seq,
                    first_event_id,
                    last_seen_at,
                    terminal,
                ) = row.map_err(sqlite_error)?;
                records.push(WorkflowChangeVersionRecord {
                    namespace: crate::Namespace::new(namespace),
                    workflow_id: WorkflowId::new(workflow_id),
                    workflow_type: WorkflowType::new(workflow_name, workflow_version),
                    run_id: RunId::new(run_id),
                    change_id,
                    version,
                    marker_kind: marker_kind_from_str(&marker_kind)?,
                    command_seq: CommandSeq(command_seq),
                    first_event_id: EventId(first_event_id),
                    last_seen_at: TimestampMs(last_seen_at),
                    status: if terminal {
                        WorkflowChangeVersionStatus::Closed
                    } else {
                        WorkflowChangeVersionStatus::Open
                    },
                });
            }
            Ok(WorkflowChangeVersionsOutcome { records })
        })();
        Box::pin(ready(result))
    }

    fn payload_roots(&self) -> BoxFuture<'static, Result<PayloadRootsOutcome>> {
        let result = (|| {
            let conn = self.connection()?;
            collect_payload_roots(&conn, &self.payload_config)
                .map(|roots| PayloadRootsOutcome { roots })
        })();
        Box::pin(ready(result))
    }

    fn gc_payload_blobs(
        &self,
        req: crate::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, Result<crate::PayloadGarbageCollectionOutcome>> {
        let result = (|| {
            let mut conn = self.connection()?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let mut reachable = BTreeSet::new();
            collect_reachable_payload_blobs(&tx, &self.payload_config, &mut reachable)?;
            // Directory-store files are written before the transaction that
            // makes them reachable commits, so an unreachable-but-young blob
            // may belong to an in-flight commit. Only blobs older than the
            // grace period are garbage.
            let cutoff = payload_gc_cutoff_ms(unix_epoch_millis(), req.min_age);
            let mut all_blobs: BTreeMap<String, i64> = {
                let mut stmt = tx
                    .prepare("select digest, created_at_ms from payload_blobs order by digest asc")
                    .map_err(sqlite_error)?;
                stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sqlite_error)?
                .collect::<std::result::Result<BTreeMap<_, _>, _>>()
                .map_err(sqlite_error)?
            };
            for (digest, last_modified) in external_blob_listings(&self.payload_config)? {
                // A digest present in both stores keeps the younger timestamp
                // so the grace period stays conservative.
                let entry = all_blobs.entry(digest).or_insert(last_modified.0);
                *entry = (*entry).max(last_modified.0);
            }
            let scanned_blobs = all_blobs.len();
            let garbage = all_blobs
                .iter()
                .filter(|(digest, last_modified)| {
                    !reachable.contains(*digest) && **last_modified <= cutoff
                })
                .map(|(digest, _)| digest.clone())
                .collect::<Vec<_>>();
            let mut deleted_blobs = garbage.len();
            let mut failed_blobs = 0_usize;
            if !req.dry_run {
                deleted_blobs = 0;
                for digest in garbage {
                    tx.execute(
                        "delete from payload_blobs where digest = ?1",
                        params![digest.as_str()],
                    )
                    .map_err(sqlite_error)?;
                    // A file that cannot be deleted is recorded and retried on
                    // the next sweep instead of aborting the rest.
                    match delete_external_blob(&self.payload_config, &digest) {
                        Ok(()) => deleted_blobs += 1,
                        Err(_) => failed_blobs += 1,
                    }
                }
            }
            tx.commit().map_err(sqlite_error)?;
            Ok(crate::PayloadGarbageCollectionOutcome {
                scanned_blobs,
                retained_blobs: scanned_blobs - deleted_blobs - failed_blobs,
                deleted_blobs,
                failed_blobs,
            })
        })();
        Box::pin(ready(result))
    }
}

fn normalize_history_events_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    events: Vec<crate::NewHistoryEvent>,
) -> Result<Vec<crate::NewHistoryEvent>> {
    events
        .into_iter()
        .map(|event| {
            Ok(crate::NewHistoryEvent {
                data: normalize_history_event_for_storage(conn, config, event.data)?,
            })
        })
        .collect()
}

fn collect_reachable_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    collect_history_payload_blobs(conn, config, reachable)?;
    collect_activity_payload_blobs(conn, config, reachable)?;
    collect_activity_map_payload_blobs(conn, config, reachable)?;
    collect_child_workflow_map_payload_blobs(conn, config, reachable)?;
    collect_child_outbox_payload_blobs(conn, config, reachable)?;
    collect_signal_payload_blobs(conn, config, reachable)?;
    collect_query_projection_payload_blobs(conn, config, reachable)?;
    Ok(())
}

fn collect_payload_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
) -> Result<Vec<PayloadRootRef>> {
    let mut roots = Vec::new();
    collect_history_payload_roots(conn, config, &mut roots)?;
    collect_activity_payload_roots(conn, &mut roots)?;
    collect_activity_map_payload_roots(conn, config, &mut roots)?;
    collect_child_workflow_map_payload_roots(conn, config, &mut roots)?;
    collect_child_outbox_payload_roots(conn, &mut roots)?;
    collect_signal_payload_roots(conn, &mut roots)?;
    collect_query_projection_payload_roots(conn, &mut roots)?;
    Ok(roots)
}

fn collect_history_payload_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select data from history_events order by run_id asc, event_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let data: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_history_event_payload_roots(conn, config, &data, roots)?;
    }
    Ok(())
}

fn collect_activity_payload_roots(
    conn: &Connection,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from activity_tasks order by activity_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ActivityTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::Payload(task.input));
    }
    Ok(())
}

fn collect_activity_map_payload_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from activity_maps order by map_command_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ActivityMapTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::ActivityMapInputManifest(
            activity_map_input_root_for_roots(conn, config, &task.input_manifest)?,
        ));
    }
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "select result from activity_map_results order by map_command_id asc, item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let result: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::Payload(result));
    }
    Ok(())
}

fn collect_child_workflow_map_payload_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from child_workflow_maps order by map_command_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ChildWorkflowMapTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::ActivityMapInputManifest(
            activity_map_input_root_for_roots(conn, config, &task.input_manifest)?,
        ));
    }
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "select outcome from child_workflow_map_results
             order by map_command_id asc, item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let outcome: ChildWorkflowMapItemOutcome =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_child_workflow_map_outcome_payload_roots(&outcome, roots);
    }
    Ok(())
}

fn collect_child_workflow_map_outcome_payload_roots(
    outcome: &ChildWorkflowMapItemOutcome,
    roots: &mut Vec<PayloadRootRef>,
) {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { result } => {
            roots.push(PayloadRootRef::Payload(result.clone()));
        }
        ChildWorkflowMapItemOutcome::Failed { failure } => {
            collect_failure_payload_roots(failure, roots);
        }
        ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
    }
}

fn collect_child_outbox_payload_roots(
    conn: &Connection,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select message from child_outbox order by outbox_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let message: ChildStartOutboxMessage =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::Payload(message.input));
    }
    Ok(())
}

fn collect_signal_payload_roots(conn: &Connection, roots: &mut Vec<PayloadRootRef>) -> Result<()> {
    let mut stmt = conn
        .prepare("select payload from signals order by signal_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let payload: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::Payload(payload));
    }
    Ok(())
}

fn collect_query_projection_payload_roots(
    conn: &Connection,
    roots: &mut Vec<PayloadRootRef>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select payload from query_projections order by namespace asc, workflow_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let payload: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        roots.push(PayloadRootRef::Payload(payload));
    }
    Ok(())
}

fn collect_history_event_payload_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
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
                activity_map_input_root_for_roots(conn, config, &scheduled.input_manifest)?,
            ));
        }
        HistoryEventData::ActivityMapCompleted(completed) => {
            roots.push(PayloadRootRef::ActivityMapResultManifest(
                activity_map_result_root_for_roots(conn, config, &completed.result_manifest)?,
            ));
        }
        HistoryEventData::ActivityMapFailed(failed) => {
            collect_failure_payload_roots(&failed.failure, roots);
        }
        HistoryEventData::ChildWorkflowMapScheduled(scheduled) => {
            roots.push(PayloadRootRef::ActivityMapInputManifest(
                activity_map_input_root_for_roots(conn, config, &scheduled.input_manifest)?,
            ));
        }
        HistoryEventData::ChildWorkflowMapCompleted(completed) => {
            roots.push(PayloadRootRef::ChildWorkflowMapResultManifest(
                child_workflow_map_result_root_for_roots(conn, config, &completed.result_manifest)?,
            ));
        }
        HistoryEventData::ChildWorkflowMapFailed(failed) => {
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

fn collect_failure_payload_roots(failure: &crate::DurableFailure, roots: &mut Vec<PayloadRootRef>) {
    if let Some(details) = &failure.details {
        roots.push(PayloadRootRef::Payload(details.clone()));
    }
}

fn activity_map_input_root_for_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
) -> Result<PayloadRef> {
    if is_external_payload_ref(payload) {
        return Ok(payload.clone());
    }
    hydrate_activity_map_input_manifest_from_storage(conn, config, payload.clone())
}

fn activity_map_result_root_for_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
) -> Result<PayloadRef> {
    if is_external_payload_ref(payload) {
        return Ok(payload.clone());
    }
    hydrate_activity_map_result_manifest_from_storage(conn, config, payload.clone())
}

fn child_workflow_map_result_root_for_roots(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
) -> Result<PayloadRef> {
    if is_external_payload_ref(payload) {
        return Ok(payload.clone());
    }
    hydrate_child_workflow_map_result_manifest_from_storage(conn, config, payload.clone())
}

fn collect_history_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select data from history_events order by run_id asc, event_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let data: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_history_event_payload_blobs(conn, config, &data, reachable)?;
    }
    Ok(())
}

fn collect_activity_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from activity_tasks order by activity_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ActivityTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_payload_blob_ref(conn, config, &task.input, reachable)?;
    }
    Ok(())
}

fn collect_activity_map_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from activity_maps order by map_command_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ActivityMapTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_activity_map_input_manifest_ref(conn, config, &task.input_manifest, reachable)?;
    }
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "select result from activity_map_results order by map_command_id asc, item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let result: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_payload_blob_ref(conn, config, &result, reachable)?;
    }
    Ok(())
}

fn collect_child_workflow_map_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select task from child_workflow_maps order by map_command_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let task: ChildWorkflowMapTask =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_activity_map_input_manifest_ref(conn, config, &task.input_manifest, reachable)?;
    }
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "select outcome from child_workflow_map_results
             order by map_command_id asc, item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let outcome: ChildWorkflowMapItemOutcome =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_child_workflow_map_outcome_payload_blobs(conn, config, &outcome, reachable)?;
    }
    Ok(())
}

fn collect_child_workflow_map_outcome_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    outcome: &ChildWorkflowMapItemOutcome,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { result } => {
            collect_payload_blob_ref(conn, config, result, reachable)?;
        }
        ChildWorkflowMapItemOutcome::Failed { failure } => {
            collect_failure_payload_blobs(conn, config, failure, reachable)?;
        }
        ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
    }
    Ok(())
}

fn collect_child_outbox_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select message from child_outbox order by outbox_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let message: ChildStartOutboxMessage =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_payload_blob_ref(conn, config, &message.input, reachable)?;
    }
    Ok(())
}

fn collect_signal_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select payload from signals order by signal_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let payload: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_payload_blob_ref(conn, config, &payload, reachable)?;
    }
    Ok(())
}

fn collect_query_projection_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn
        .prepare("select payload from query_projections order by namespace asc, workflow_id asc")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let payload: PayloadRef =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        collect_payload_blob_ref(conn, config, &payload, reachable)?;
    }
    Ok(())
}

fn collect_history_event_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    data: &HistoryEventData,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    match data {
        HistoryEventData::WorkflowStarted { input, .. }
        | HistoryEventData::WorkflowContinuedAsNew { input } => {
            collect_payload_blob_ref(conn, config, input, reachable)
        }
        HistoryEventData::WorkflowCompleted { result } => {
            collect_payload_blob_ref(conn, config, result, reachable)
        }
        HistoryEventData::WorkflowFailed { failure } => {
            collect_failure_payload_blobs(conn, config, failure, reachable)
        }
        HistoryEventData::WorkflowCancelled { .. } | HistoryEventData::WorkflowTaskStarted => {
            Ok(())
        }
        HistoryEventData::ActivityScheduled(scheduled) => {
            collect_payload_blob_ref(conn, config, &scheduled.input, reachable)
        }
        HistoryEventData::ActivityMapScheduled(scheduled) => {
            collect_activity_map_input_manifest_ref(
                conn,
                config,
                &scheduled.input_manifest,
                reachable,
            )
        }
        HistoryEventData::ActivityMapCompleted(completed) => {
            collect_activity_map_result_manifest_ref(
                conn,
                config,
                &completed.result_manifest,
                reachable,
            )
        }
        HistoryEventData::ActivityMapFailed(failed) => {
            collect_failure_payload_blobs(conn, config, &failed.failure, reachable)
        }
        HistoryEventData::ChildWorkflowMapScheduled(scheduled) => {
            collect_activity_map_input_manifest_ref(
                conn,
                config,
                &scheduled.input_manifest,
                reachable,
            )
        }
        HistoryEventData::ChildWorkflowMapCompleted(completed) => {
            collect_child_workflow_map_result_manifest_ref(
                conn,
                config,
                &completed.result_manifest,
                reachable,
            )
        }
        HistoryEventData::ChildWorkflowMapFailed(failed) => {
            collect_failure_payload_blobs(conn, config, &failed.failure, reachable)
        }
        HistoryEventData::ActivityCompleted(completed) => {
            collect_payload_blob_ref(conn, config, &completed.result, reachable)
        }
        HistoryEventData::ActivityFailed(failed) => {
            collect_failure_payload_blobs(conn, config, &failed.failure, reachable)
        }
        HistoryEventData::ActivityTimedOut(_)
        | HistoryEventData::ChildWorkflowStarted(_)
        | HistoryEventData::ChildWorkflowCancelled(_)
        | HistoryEventData::TimerStarted(_)
        | HistoryEventData::TimerFired(_)
        | HistoryEventData::SelectWinner(_)
        | HistoryEventData::VersionMarker(_)
        | HistoryEventData::DeprecatedPatchMarker(_) => Ok(()),
        HistoryEventData::SideEffectMarker(marker) => {
            crate::payload::validate_side_effect_marker(marker)
        }
        HistoryEventData::ChildWorkflowStartRequested(requested) => {
            collect_payload_blob_ref(conn, config, &requested.input, reachable)
        }
        HistoryEventData::ChildWorkflowCompleted(completed) => {
            collect_payload_blob_ref(conn, config, &completed.result, reachable)
        }
        HistoryEventData::ChildWorkflowFailed(failed) => {
            collect_failure_payload_blobs(conn, config, &failed.failure, reachable)
        }
        HistoryEventData::SignalConsumed(signal) => {
            collect_payload_blob_ref(conn, config, &signal.payload, reachable)
        }
    }
}

fn collect_failure_payload_blobs(
    conn: &Connection,
    config: &PayloadStorageConfig,
    failure: &crate::DurableFailure,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    if let Some(details) = &failure.details {
        collect_payload_blob_ref(conn, config, details, reachable)?;
    }
    Ok(())
}

fn collect_payload_blob_ref(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    if let PayloadRef::Blob { digest, uri, .. } = payload {
        if is_sqlite_payload_uri(uri) {
            load_payload_blob(conn, config, payload, false)?;
        }
        reachable.insert(digest.clone());
    }
    Ok(())
}

fn collect_activity_map_input_manifest_ref(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    collect_payload_blob_ref(conn, config, payload, reachable)?;
    if is_external_payload_ref(payload) {
        return Ok(());
    }
    let manifest_payload = hydrate_payload_from_storage(conn, config, payload.clone())?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    for page in &manifest.pages {
        collect_payload_blob_ref(conn, config, page, reachable)?;
        if is_external_payload_ref(page) {
            continue;
        }
        let page_payload = hydrate_payload_from_storage(conn, config, page.clone())?;
        let page: ActivityMapInputPage = crate::decode_payload(&page_payload)?;
        for item in &page.items {
            collect_payload_blob_ref(conn, config, item, reachable)?;
        }
    }
    Ok(())
}

fn collect_activity_map_result_manifest_ref(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    collect_payload_blob_ref(conn, config, payload, reachable)?;
    if is_external_payload_ref(payload) {
        return Ok(());
    }
    let manifest_payload = hydrate_payload_from_storage(conn, config, payload.clone())?;
    let manifest: ActivityMapResultManifest = crate::decode_payload(&manifest_payload)?;
    for page in &manifest.pages {
        collect_payload_blob_ref(conn, config, page, reachable)?;
        if is_external_payload_ref(page) {
            continue;
        }
        let page_payload = hydrate_payload_from_storage(conn, config, page.clone())?;
        let page: ActivityMapResultPage = crate::decode_payload(&page_payload)?;
        for result in &page.results {
            collect_payload_blob_ref(conn, config, result, reachable)?;
        }
    }
    Ok(())
}

fn collect_child_workflow_map_result_manifest_ref(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    collect_payload_blob_ref(conn, config, payload, reachable)?;
    if is_external_payload_ref(payload) {
        return Ok(());
    }
    let manifest_payload = hydrate_payload_from_storage(conn, config, payload.clone())?;
    let manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&manifest_payload)?;
    for page in &manifest.pages {
        collect_payload_blob_ref(conn, config, page, reachable)?;
        if is_external_payload_ref(page) {
            continue;
        }
        let page_payload = hydrate_payload_from_storage(conn, config, page.clone())?;
        let page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page_payload)?;
        for outcome in &page.outcomes {
            match outcome {
                crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
                    collect_payload_blob_ref(conn, config, result, reachable)?;
                }
                crate::ChildWorkflowMapItemOutcome::Failed { failure } => {
                    collect_failure_payload_blobs(conn, config, failure, reachable)?;
                }
                crate::ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
            }
        }
    }
    Ok(())
}

fn normalize_history_event_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    data: HistoryEventData,
) -> Result<HistoryEventData> {
    match data {
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = normalize_activity_map_input_manifest_for_storage(
                    conn,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapScheduled(scheduled))
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest = normalize_activity_map_result_manifest_for_storage(
                    conn,
                    config,
                    completed.result_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapCompleted(completed))
        }
        HistoryEventData::ChildWorkflowMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = normalize_activity_map_input_manifest_for_storage(
                    conn,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapScheduled(scheduled))
        }
        HistoryEventData::ChildWorkflowMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest =
                    normalize_child_workflow_map_result_manifest_for_storage(
                        conn,
                        config,
                        completed.result_manifest,
                    )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapCompleted(completed))
        }
        data => crate::payload::map_history_event_payloads(data, &mut |payload| {
            normalize_payload_for_storage(conn, config, payload)
        }),
    }
}

fn hydrate_history_event_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    data: HistoryEventData,
) -> Result<HistoryEventData> {
    match data {
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = hydrate_activity_map_input_manifest_from_storage(
                    conn,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapScheduled(scheduled))
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest = hydrate_activity_map_result_manifest_from_storage(
                    conn,
                    config,
                    completed.result_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapCompleted(completed))
        }
        HistoryEventData::ChildWorkflowMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = hydrate_activity_map_input_manifest_from_storage(
                    conn,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapScheduled(scheduled))
        }
        HistoryEventData::ChildWorkflowMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest =
                    hydrate_child_workflow_map_result_manifest_from_storage(
                        conn,
                        config,
                        completed.result_manifest,
                    )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapCompleted(completed))
        }
        data => crate::payload::map_history_event_payloads(data, &mut |payload| {
            hydrate_payload_from_storage(conn, config, payload)
        }),
    }
}

fn normalize_activity_tasks_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    tasks: Vec<ActivityTask>,
) -> Result<Vec<ActivityTask>> {
    tasks
        .into_iter()
        .map(|task| normalize_activity_task_for_storage(conn, config, task))
        .collect()
}

fn normalize_activity_task_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    task: ActivityTask,
) -> Result<ActivityTask> {
    crate::payload::map_activity_task_payloads(task, &mut |payload| {
        normalize_payload_for_storage(conn, config, payload)
    })
}

fn hydrate_activity_task_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    task: ActivityTask,
) -> Result<ActivityTask> {
    crate::payload::map_activity_task_payloads(task, &mut |payload| {
        hydrate_payload_from_storage(conn, config, payload)
    })
}

fn normalize_activity_map_task_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    mut task: ActivityMapTask,
) -> Result<ActivityMapTask> {
    task.input_manifest =
        normalize_activity_map_input_manifest_for_storage(conn, config, task.input_manifest)?;
    Ok(task)
}

fn normalize_child_workflow_map_task_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    mut task: ChildWorkflowMapTask,
) -> Result<ChildWorkflowMapTask> {
    task.input_manifest =
        normalize_activity_map_input_manifest_for_storage(conn, config, task.input_manifest)?;
    Ok(task)
}

fn normalize_child_start_message_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    message: ChildStartOutboxMessage,
) -> Result<ChildStartOutboxMessage> {
    crate::payload::map_child_start_payloads(message, &mut |payload| {
        normalize_payload_for_storage(conn, config, payload)
    })
}

fn normalize_failure_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    failure: crate::DurableFailure,
) -> Result<crate::DurableFailure> {
    crate::payload::map_failure_payloads(failure, &mut |payload| {
        normalize_payload_for_storage(conn, config, payload)
    })
}

fn normalize_activity_map_input_manifest_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(conn, config, payload)?;
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(conn, config, page)?;
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            page.items = page
                .items
                .into_iter()
                .map(|payload| normalize_payload_for_storage(conn, config, payload))
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                conn,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        conn,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_activity_map_result_manifest_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(conn, config, payload)?;
    let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(conn, config, page)?;
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            page.results = page
                .results
                .into_iter()
                .map(|payload| normalize_payload_for_storage(conn, config, payload))
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                conn,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        conn,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_child_workflow_map_result_manifest_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(conn, config, payload)?;
    let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(conn, config, page)?;
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            page.outcomes = page
                .outcomes
                .into_iter()
                .map(|outcome| {
                    normalize_child_workflow_map_outcome_for_storage(conn, config, outcome)
                })
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                conn,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        conn,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_child_workflow_map_outcome_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    outcome: crate::ChildWorkflowMapItemOutcome,
) -> Result<crate::ChildWorkflowMapItemOutcome> {
    match outcome {
        crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            Ok(crate::ChildWorkflowMapItemOutcome::Succeeded {
                result: normalize_payload_for_storage(conn, config, result)?,
            })
        }
        crate::ChildWorkflowMapItemOutcome::Failed { failure } => {
            Ok(crate::ChildWorkflowMapItemOutcome::Failed {
                failure: normalize_failure_for_storage(conn, config, failure)?,
            })
        }
        crate::ChildWorkflowMapItemOutcome::Cancelled { reason } => {
            Ok(crate::ChildWorkflowMapItemOutcome::Cancelled { reason })
        }
    }
}

fn hydrate_activity_map_input_manifest_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let mut load_container = |payload| hydrate_payload_from_storage(conn, config, payload);
    let mut hydrate_leaf = |payload| hydrate_payload_from_storage(conn, config, payload);
    let mut finish_container = Ok;
    crate::payload::map_activity_map_input_manifest_ref(
        payload,
        &mut load_container,
        &mut hydrate_leaf,
        &mut finish_container,
    )
}

fn hydrate_activity_map_result_manifest_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let mut load_container = |payload| hydrate_payload_from_storage(conn, config, payload);
    let mut hydrate_leaf = |payload| hydrate_payload_from_storage(conn, config, payload);
    let mut finish_container = Ok;
    crate::payload::map_activity_map_result_manifest_ref(
        payload,
        &mut load_container,
        &mut hydrate_leaf,
        &mut finish_container,
    )
}

fn hydrate_child_workflow_map_result_manifest_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(conn, config, payload)?;
    let root_codec = root.codec();
    let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(conn, config, page)?;
            let page_codec = page.codec();
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            page.outcomes = page
                .outcomes
                .into_iter()
                .map(|outcome| {
                    hydrate_child_workflow_map_outcome_from_storage(conn, config, outcome)
                })
                .collect::<Result<Vec<_>>>()?;
            crate::encode_payload_with_codec(&page, page_codec)
        })
        .collect::<Result<Vec<_>>>()?;
    crate::encode_payload_with_codec(&manifest, root_codec)
}

fn hydrate_child_workflow_map_outcome_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    outcome: crate::ChildWorkflowMapItemOutcome,
) -> Result<crate::ChildWorkflowMapItemOutcome> {
    match outcome {
        crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            Ok(crate::ChildWorkflowMapItemOutcome::Succeeded {
                result: hydrate_payload_from_storage(conn, config, result)?,
            })
        }
        crate::ChildWorkflowMapItemOutcome::Failed { mut failure } => {
            if let Some(details) = failure.details.take() {
                failure.details = Some(hydrate_payload_from_storage(conn, config, details)?);
            }
            Ok(crate::ChildWorkflowMapItemOutcome::Failed { failure })
        }
        crate::ChildWorkflowMapItemOutcome::Cancelled { reason } => {
            Ok(crate::ChildWorkflowMapItemOutcome::Cancelled { reason })
        }
    }
}

fn normalize_payload_for_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    match payload {
        PayloadRef::Inline {
            codec,
            schema_fingerprint,
            compression,
            encryption,
            bytes,
        } if bytes.len() > config.inline_threshold_bytes => {
            let digest = digest_bytes(&bytes);
            let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let uri = if let Some(uri) = store_external_payload_blob(config, &digest, &bytes)? {
                uri
            } else {
                let encryption_blob = encode_encryption_metadata(&encryption)?;
                // A content-addressed reuse keeps the first row's blob but
                // restarts the GC grace period for it.
                conn.execute(
                    "insert into payload_blobs
                     (digest, codec, schema_fingerprint, compression, encryption, size, bytes,
                      created_at_ms)
                     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     on conflict(digest) do update set created_at_ms = excluded.created_at_ms",
                    params![
                        digest.as_str(),
                        codec_to_str(codec),
                        schema_fingerprint.0.as_str(),
                        compression_to_str(compression),
                        encryption_blob,
                        size,
                        bytes.as_slice(),
                        unix_epoch_millis(),
                    ],
                )
                .map_err(sqlite_error)?;
                format!("sqlite://payload/{digest}")
            };
            Ok(PayloadRef::Blob {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                digest: digest.clone(),
                size,
                uri,
            })
        }
        payload @ PayloadRef::Inline { .. } => Ok(payload),
        payload @ PayloadRef::Blob { .. } => {
            // Only refs with this provider's schemes are validated against its
            // stores; every other scheme is opaque and persists unchanged.
            if matches!(&payload, PayloadRef::Blob { uri, .. } if is_sqlite_payload_uri(uri)) {
                load_payload_blob(conn, config, &payload, true)?;
            }
            Ok(payload)
        }
    }
}

fn hydrate_payload_from_storage(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    match payload {
        payload @ PayloadRef::Inline { .. } => Ok(payload),
        payload @ PayloadRef::Blob { .. } => {
            if matches!(&payload, PayloadRef::Blob { uri, .. } if !is_sqlite_payload_uri(uri)) {
                return Ok(payload);
            }
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
            let blob = load_payload_blob(conn, config, &payload, false)?;
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

fn load_payload_blob(
    conn: &Connection,
    config: &PayloadStorageConfig,
    payload: &PayloadRef,
    require_schema_fingerprint_match: bool,
) -> Result<PayloadBlob> {
    let PayloadRef::Blob {
        codec: ref_codec,
        schema_fingerprint: ref_schema_fingerprint,
        compression: ref_compression,
        encryption: ref_encryption,
        digest,
        size,
        uri,
    } = payload
    else {
        return Err(Error::PayloadDecode(
            "inline payload does not reference blob storage".to_owned(),
        ));
    };
    if uri.starts_with("local://payload/") {
        let bytes = load_external_payload_blob(config, digest, size)?;
        return Ok(PayloadBlob {
            codec: *ref_codec,
            schema_fingerprint: ref_schema_fingerprint.clone(),
            compression: *ref_compression,
            encryption: ref_encryption.clone(),
            bytes,
        });
    }
    let row = conn
        .query_row(
            "select codec, schema_fingerprint, compression, encryption, size, bytes
             from payload_blobs
             where digest = ?1",
            params![digest.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))?;
    let (row_codec, row_schema_fingerprint, row_compression, encryption_blob, stored_size, bytes) =
        row;
    let Some(bytes) = bytes else {
        return Err(Error::PayloadDecode(format!(
            "payload blob `{digest}` is stored outside SQLite but no configured external blob store matched `{uri}`"
        )));
    };
    let actual_digest = digest_bytes(&bytes);
    if &actual_digest != digest {
        return Err(Error::PayloadDecode(format!(
            "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
        )));
    }
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != *size || stored_size != *size {
        return Err(Error::PayloadDecode(format!(
            "payload blob size mismatch: expected {size}, got {actual_size}"
        )));
    }
    let blob = PayloadBlob {
        codec: codec_from_str(&row_codec)?,
        schema_fingerprint: crate::SchemaFingerprint(row_schema_fingerprint),
        compression: compression_from_str(&row_compression)?,
        encryption: decode_encryption_metadata(encryption_blob)?,
        bytes,
    };
    if blob.codec != *ref_codec
        || (require_schema_fingerprint_match && blob.schema_fingerprint != *ref_schema_fingerprint)
        || blob.compression != *ref_compression
        || blob.encryption != *ref_encryption
    {
        return Err(Error::PayloadDecode(format!(
            "payload blob metadata mismatch for `{digest}`"
        )));
    }
    Ok(blob)
}

fn store_external_payload_blob(
    config: &PayloadStorageConfig,
    digest: &str,
    bytes: &[u8],
) -> Result<Option<String>> {
    let Some(blob_store) = &config.blob_store else {
        return Ok(None);
    };
    match blob_store {
        BlobStoreConfig::LocalDirectory { root, prefix } => {
            let dir = local_blob_dir(root, prefix);
            fs::create_dir_all(&dir).map_err(|err| {
                Error::Backend(format!(
                    "failed to create local payload blob directory `{}`: {err}",
                    dir.display()
                ))
            })?;
            let path = dir.join(digest);
            if path.exists() {
                let expected_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let metadata = fs::metadata(&path).map_err(|err| {
                    Error::Backend(format!(
                        "failed to inspect local payload blob `{}`: {err}",
                        path.display()
                    ))
                })?;
                if metadata.len() != expected_size {
                    return Err(Error::PayloadDecode(format!(
                        "payload blob size mismatch: expected {expected_size}, got {}",
                        metadata.len()
                    )));
                }
                // Refresh the mtime so the GC grace period keeps protecting a
                // blob that this in-flight commit is about to reference; the
                // digest is re-validated on every read.
                fs::File::options()
                    .write(true)
                    .open(&path)
                    .and_then(|file| file.set_modified(std::time::SystemTime::now()))
                    .map_err(|err| {
                        Error::Backend(format!(
                            "failed to refresh local payload blob `{}`: {err}",
                            path.display()
                        ))
                    })?;
                return Ok(Some(local_blob_uri(digest)));
            }
            let tmp_path = dir.join(format!("{digest}.tmp-{}", std::process::id()));
            fs::write(&tmp_path, bytes).map_err(|err| {
                Error::Backend(format!(
                    "failed to write local payload blob `{}`: {err}",
                    tmp_path.display()
                ))
            })?;
            match fs::rename(&tmp_path, &path) {
                Ok(()) => {}
                Err(_) if path.exists() => {
                    let _ = fs::remove_file(&tmp_path);
                }
                Err(err) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(Error::Backend(format!(
                        "failed to commit local payload blob `{}`: {err}",
                        path.display()
                    )));
                }
            }
            Ok(Some(local_blob_uri(digest)))
        }
    }
}

fn load_external_payload_blob(
    config: &PayloadStorageConfig,
    digest: &str,
    expected_size: &u64,
) -> Result<Vec<u8>> {
    let Some(blob_store) = &config.blob_store else {
        return Err(Error::PayloadDecode(format!(
            "missing payload blob `{digest}`"
        )));
    };
    match blob_store {
        BlobStoreConfig::LocalDirectory { root, prefix } => {
            let path = local_blob_path(root, prefix, digest);
            let bytes = fs::read(&path).map_err(|err| match err.kind() {
                ErrorKind::NotFound => {
                    Error::PayloadDecode(format!("missing payload blob `{digest}`"))
                }
                _ => Error::PayloadDecode(format!(
                    "failed to read local payload blob `{}`: {err}",
                    path.display()
                )),
            })?;
            let actual_digest = digest_bytes(&bytes);
            if actual_digest != digest {
                return Err(Error::PayloadDecode(format!(
                    "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
                )));
            }
            let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if actual_size != *expected_size {
                return Err(Error::PayloadDecode(format!(
                    "payload blob size mismatch: expected {expected_size}, got {actual_size}"
                )));
            }
            Ok(bytes)
        }
    }
}

fn external_blob_listings(config: &PayloadStorageConfig) -> Result<BTreeMap<String, TimestampMs>> {
    let Some(blob_store) = &config.blob_store else {
        return Ok(BTreeMap::new());
    };
    match blob_store {
        BlobStoreConfig::LocalDirectory { root, prefix } => {
            let dir = local_blob_dir(root, prefix);
            let mut blobs = BTreeMap::new();
            match fs::read_dir(&dir) {
                Ok(entries) => {
                    for entry in entries {
                        let entry = entry.map_err(|err| {
                            Error::Backend(format!(
                                "failed to list local payload blob directory `{}`: {err}",
                                dir.display()
                            ))
                        })?;
                        let metadata = entry.metadata().map_err(|err| {
                            Error::Backend(format!(
                                "failed to inspect local payload blob `{}`: {err}",
                                entry.path().display()
                            ))
                        })?;
                        if !metadata.is_file() {
                            continue;
                        }
                        let name = entry.file_name().to_string_lossy().into_owned();
                        if name.contains(".tmp-") {
                            continue;
                        }
                        // A file without a readable mtime counts as brand new
                        // so GC retains rather than deletes when unsure.
                        let last_modified = metadata
                            .modified()
                            .ok()
                            .and_then(|modified| {
                                modified.duration_since(std::time::UNIX_EPOCH).ok()
                            })
                            .map(|since_epoch| {
                                i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)
                            })
                            .unwrap_or_else(unix_epoch_millis);
                        blobs.insert(name, TimestampMs(last_modified));
                    }
                    Ok(blobs)
                }
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(blobs),
                Err(err) => Err(Error::Backend(format!(
                    "failed to list local payload blob directory `{}`: {err}",
                    dir.display()
                ))),
            }
        }
    }
}

fn delete_external_blob(config: &PayloadStorageConfig, digest: &str) -> Result<()> {
    let Some(blob_store) = &config.blob_store else {
        return Ok(());
    };
    match blob_store {
        BlobStoreConfig::LocalDirectory { root, prefix } => {
            let path = local_blob_path(root, prefix, digest);
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(Error::Backend(format!(
                    "failed to delete local payload blob `{}`: {err}",
                    path.display()
                ))),
            }
        }
    }
}

fn local_blob_dir(root: &Path, prefix: &str) -> PathBuf {
    if prefix.is_empty() {
        root.to_path_buf()
    } else {
        root.join(prefix)
    }
}

fn local_blob_path(root: &Path, prefix: &str, digest: &str) -> PathBuf {
    local_blob_dir(root, prefix).join(digest)
}

fn local_blob_uri(digest: &str) -> String {
    format!("local://payload/{digest}")
}

fn is_sqlite_payload_uri(uri: &str) -> bool {
    uri.starts_with("sqlite://payload/") || uri.starts_with("local://payload/")
}

// Every blob ref this provider did not mint is opaque: it belongs to whatever
// layer owns its scheme (a `PayloadBackend` blob store), so the provider never
// hydrates, validates, or garbage-collects it.
fn is_external_payload_ref(payload: &PayloadRef) -> bool {
    matches!(payload, PayloadRef::Blob { uri, .. } if !is_sqlite_payload_uri(uri))
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        create table if not exists meta (
            key text primary key,
            value integer not null
        );

        create table if not exists workflow_instances (
            namespace text not null,
            workflow_id text not null,
            run_id text primary key,
            workflow_name text not null,
            workflow_version integer not null,
            task_queue text not null,
            current_event_id integer not null,
            ready_reason text,
            ready_at_ms integer not null default 0,
            workflow_claim_token integer,
            claim_lease_until_ms integer,
            terminal integer not null,
            parent_run_id text,
            parent_command_seq integer,
            parent_close_policy text,
            parent_child_map_ordinal integer,
            unique(namespace, workflow_id)
        );

        create table if not exists history_events (
            run_id text not null,
            event_id integer not null,
            event_type text not null,
            command_seq integer,
            data blob not null,
            primary key(run_id, event_id)
        );

        create table if not exists payload_blobs (
            digest text primary key,
            codec text not null,
            schema_fingerprint text not null,
            compression text not null,
            encryption blob,
            size integer not null,
            bytes blob,
            created_at_ms integer not null default 0
        );

        create table if not exists activity_tasks (
            activity_id text primary key,
            namespace text not null,
            run_id text not null,
            activity_name text not null,
            task_queue text not null,
            task blob not null,
            claim_token integer,
            completed integer not null,
            timeout_at_ms integer,
            heartbeat_deadline_at_ms integer
        );

        create table if not exists activity_maps (
            map_command_id text primary key,
            namespace text not null,
            run_id text not null,
            command_seq integer not null,
            task blob not null,
            item_count integer not null,
            next_ordinal integer not null,
            in_flight integer not null,
            completed integer not null
        );

        create table if not exists activity_map_results (
            map_command_id text not null,
            item_ordinal integer not null,
            result blob not null,
            primary key(map_command_id, item_ordinal)
        );

        create table if not exists child_workflow_maps (
            map_command_id text primary key,
            namespace text not null,
            run_id text not null,
            command_seq integer not null,
            task blob not null,
            item_count integer not null,
            next_ordinal integer not null,
            in_flight integer not null,
            completed integer not null
        );

        create table if not exists child_workflow_map_results (
            map_command_id text not null,
            item_ordinal integer not null,
            outcome blob not null,
            primary key(map_command_id, item_ordinal)
        );

        create table if not exists child_outbox (
            outbox_id text primary key,
            namespace text not null,
            parent_run_id text not null,
            command_seq integer not null,
            child_workflow_id text not null,
            parent_close_policy text not null default 'cancel',
            message blob not null,
            dispatched integer not null,
            child_run_id text
        );

        create index if not exists idx_child_outbox_dispatch
            on child_outbox(namespace, dispatched, outbox_id);

        create table if not exists active_waits (
            wait_id text primary key,
            run_id text not null,
            command_seq integer not null,
            kind text not null,
            wait_key text not null,
            ready_at_ms integer
        );

        create table if not exists query_projections (
            namespace text not null,
            workflow_id text not null,
            run_id text not null,
            event_id integer not null,
            payload blob not null,
            primary key(namespace, workflow_id)
        );

        create table if not exists workflow_change_versions (
            namespace text not null,
            workflow_id text not null,
            workflow_name text not null,
            workflow_version integer not null,
            run_id text not null,
            change_id text not null,
            version integer not null,
            marker_kind text not null,
            command_seq integer not null,
            first_event_id integer not null,
            last_seen_at_ms integer not null,
            primary key(run_id, change_id)
        );

        create index if not exists idx_workflow_change_versions_change
            on workflow_change_versions(namespace, change_id, run_id);

        create index if not exists idx_workflow_change_versions_workflow
            on workflow_change_versions(namespace, workflow_id, change_id);

        create index if not exists idx_active_waits_timer_due
            on active_waits(kind, ready_at_ms, wait_id);

        create index if not exists idx_active_waits_signal
            on active_waits(run_id, kind, wait_key);

        create table if not exists signals (
            signal_id text primary key,
            namespace text not null,
            run_id text not null,
            signal_name text not null,
            payload blob not null,
            received_sequence integer not null,
            consumed integer not null
        );

        create index if not exists idx_signals_inbox
            on signals(run_id, signal_name, consumed, received_sequence);

        create index if not exists idx_activity_tasks_timeout_due
            on activity_tasks(namespace, completed, timeout_at_ms, activity_id);

        create index if not exists idx_activity_tasks_heartbeat_due
            on activity_tasks(namespace, completed, heartbeat_deadline_at_ms, activity_id);

        create index if not exists idx_activity_tasks_claim
            on activity_tasks(namespace, task_queue, activity_id)
            where completed = 0
              and claim_token is null;
        ",
    )
    .map_err(sqlite_error)?;
    ensure_column(
        conn,
        "workflow_instances",
        "ready_at_ms",
        "integer not null default 0",
    )?;
    ensure_column(conn, "workflow_instances", "parent_run_id", "text")?;
    ensure_column(conn, "workflow_instances", "parent_command_seq", "integer")?;
    ensure_column(conn, "workflow_instances", "parent_close_policy", "text")?;
    ensure_column(
        conn,
        "workflow_instances",
        "parent_child_map_ordinal",
        "integer",
    )?;
    ensure_column(
        conn,
        "child_outbox",
        "parent_close_policy",
        "text not null default 'cancel'",
    )?;
    ensure_column(
        conn,
        "workflow_instances",
        "claim_lease_until_ms",
        "integer",
    )?;
    ensure_column(conn, "activity_tasks", "timeout_at_ms", "integer")?;
    ensure_column(
        conn,
        "activity_tasks",
        "heartbeat_deadline_at_ms",
        "integer",
    )?;
    // Default 0 makes pre-existing blobs immediately past any GC grace period,
    // which is correct: they are old.
    ensure_column(
        conn,
        "payload_blobs",
        "created_at_ms",
        "integer not null default 0",
    )?;
    // One transaction for the whole command_seq migration: the backfill runs
    // only on the open that adds the column, so a crash between the ALTER and
    // the backfill must roll the column back too — otherwise the next open
    // would skip the backfill and child-terminal dedup would miss legacy
    // rows. The index is created after the column exists.
    let command_seq_migration = conn.unchecked_transaction().map_err(sqlite_error)?;
    if ensure_column(
        &command_seq_migration,
        "history_events",
        "command_seq",
        "integer",
    )? {
        backfill_history_event_command_seqs(&command_seq_migration)?;
    }
    command_seq_migration
        .execute(
            "create index if not exists idx_history_events_command_seq
             on history_events(run_id, command_seq)
             where command_seq is not null",
            [],
        )
        .map_err(sqlite_error)?;
    command_seq_migration.commit().map_err(sqlite_error)?;
    ensure_index(
        conn,
        "idx_workflow_instances_ready",
        WORKFLOW_INSTANCES_READY_INDEX_SQL,
    )
}

// Databases created before the `command_seq` column carry null for existing
// rows; child-terminal dedup reads the column, so a one-time decode of the
// legacy child lifecycle events keeps dedup correct across the migration.
fn backfill_history_event_command_seqs(conn: &Connection) -> Result<()> {
    let mut stmt = conn
        .prepare(
            "select rowid, data from history_events
             where command_seq is null
               and event_type in (
                 'child_workflow_started',
                 'child_workflow_completed',
                 'child_workflow_failed',
                 'child_workflow_cancelled'
               )",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    drop(stmt);
    for (rowid, blob) in rows {
        let data: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let Some(command_seq) = data.command_seq() else {
            continue;
        };
        conn.execute(
            "update history_events set command_seq = ?1 where rowid = ?2",
            params![command_seq.0, rowid],
        )
        .map_err(sqlite_error)?;
    }
    Ok(())
}

// The ready-scan index must not filter on workflow_claim_token: lease-expiry
// reclaims match rows whose token is still set, so a claim-token predicate
// would exclude them from the index. Databases created before lease-based
// reclaim carry that narrower predicate, and `create index if not exists`
// never updates an existing index, so init_schema recreates the index from
// this definition whenever the stored one differs.
const WORKFLOW_INSTANCES_READY_INDEX_SQL: &str = "create index idx_workflow_instances_ready
    on workflow_instances(namespace, task_queue, ready_at_ms, run_id)
    where ready_reason is not null
      and terminal = 0";

fn ensure_index(conn: &Connection, index: &str, create_sql: &str) -> Result<()> {
    // sqlite_master stores the create statement verbatim, so a stale
    // definition (or a missing index) shows up as a text mismatch.
    let existing = conn
        .query_row(
            "select sql from sqlite_master where type = 'index' and name = ?1",
            params![index],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if existing.as_deref() == Some(create_sql) {
        return Ok(());
    }
    conn.execute(&format!("drop index if exists {index}"), [])
        .map_err(sqlite_error)?;
    conn.execute(create_sql, []).map_err(sqlite_error)?;
    Ok(())
}

// Returns whether the column was added by this call.
fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<bool> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info({table})"))
        .map_err(sqlite_error)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sqlite_error)?;
    for existing in columns {
        if existing.map_err(sqlite_error)? == column {
            return Ok(false);
        }
    }
    conn.execute(
        &format!("alter table {table} add column {column} {definition}"),
        [],
    )
    .map_err(sqlite_error)?;
    Ok(true)
}

fn signal_wait_ready(tx: &Transaction<'_>, run_id: &RunId) -> Result<bool> {
    Ok(tx
        .query_row(
            "select 1
             from active_waits w
             join signals s on s.run_id = w.run_id
                and s.signal_name = w.wait_key
                and s.consumed = 0
             where w.run_id = ?1 and w.kind = ?2
             limit 1",
            params![run_id.0, wait_kind_to_str(&WaitKind::Signal)],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?
        .is_some())
}

// Deletes the terminal run's operational rows; see `TerminalCleanup` for the
// contract (history stays authoritative, missing activity rows answer late
// calls as `AlreadyCompleted`, signal rows survive continue-as-new). Undis-
// patched child outbox rows stay: an abandoned child may still start after
// its parent closes.
fn cleanup_run_operational_state(
    tx: &Transaction<'_>,
    run_id: &RunId,
    cleanup: TerminalCleanup,
) -> Result<()> {
    tx.execute(
        "delete from active_waits where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from activity_tasks where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from activity_map_results
         where map_command_id in (select map_command_id from activity_maps where run_id = ?1)",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from activity_maps where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from child_workflow_map_results
         where map_command_id in (select map_command_id from child_workflow_maps where run_id = ?1)",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from child_workflow_maps where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "delete from child_outbox where parent_run_id = ?1 and dispatched = 1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    if cleanup.deletes_consumed_signals() {
        // Unconsumed deliveries stay readable through the inbox after the run
        // closes; only the consumed dedup rows go.
        tx.execute(
            "delete from signals where run_id = ?1 and consumed = 1",
            params![run_id.0],
        )
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn handle_terminal_run(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    notify_parent_of_child_terminal(tx, config, run_id, terminal_event)?;
    cancel_children_for_parent(tx, run_id)?;
    Ok(())
}

fn continue_run_as_new(
    tx: &Transaction<'_>,
    old_run_id: &RunId,
    event: HistoryEventData,
) -> Result<()> {
    let HistoryEventData::WorkflowContinuedAsNew { input } = event else {
        return Ok(());
    };
    let Some((workflow_name, workflow_version)) = tx
        .query_row(
            "select workflow_name, workflow_version
             from workflow_instances
             where run_id = ?1",
            params![old_run_id.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(old_run_id.clone()));
    };
    let workflow_type = WorkflowType::new(workflow_name, workflow_version);
    let new_run_id = RunId::new(format!("run-{}", next_counter(tx, "run")?));
    insert_history_event(
        tx,
        &new_run_id,
        EventId(1),
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        },
    )?;
    tx.execute(
        "update workflow_instances
         set run_id = ?1,
             current_event_id = 1,
             ready_reason = ?2,
             ready_at_ms = 0,
             workflow_claim_token = null,
             terminal = 0
         where run_id = ?3",
        params![
            new_run_id.0,
            reason_to_str(&WorkflowTaskReason::WorkflowStarted),
            old_run_id.0
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn notify_parent_of_child_terminal(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let Some((parent_run_id, parent_command_seq, parent_child_map_ordinal)) = tx
        .query_row(
            "select parent_run_id, parent_command_seq, parent_child_map_ordinal
             from workflow_instances
             where run_id = ?1",
            params![child_run_id.0],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<u64>>(1)?,
                    row.get::<_, Option<u64>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
        .and_then(|(run_id, seq, ordinal)| Some((RunId::new(run_id?), CommandSeq(seq?), ordinal)))
    else {
        return Ok(());
    };
    let command_id = CommandId {
        run_id: parent_run_id.clone(),
        seq: parent_command_seq,
    };
    if let Some(item_ordinal) = parent_child_map_ordinal {
        let Some(outcome) = child_terminal_map_item_outcome(terminal_event) else {
            return Ok(());
        };
        return complete_child_workflow_map_item(
            tx,
            config,
            ChildWorkflowMapItem {
                map_command_id: command_id,
                item_ordinal,
            },
            outcome,
        );
    }
    if child_terminal_event_exists(tx, &command_id)? {
        return Ok(());
    }
    let Some((tail, terminal)) = parent_tail_and_terminal(tx, &parent_run_id)? else {
        return Ok(());
    };
    if terminal {
        return Ok(());
    }
    let event_id = EventId(tail).next();
    let Some((data, reason)) = child_terminal_event_data_and_reason(command_id, terminal_event)
    else {
        return Ok(());
    };
    insert_history_event(tx, &parent_run_id, event_id, data)?;
    set_workflow_ready(tx, &parent_run_id, event_id, reason)
}

fn child_terminal_event_exists(tx: &Transaction<'_>, command_id: &CommandId) -> Result<bool> {
    // Indexed lookup on (run_id, command_seq); the event-type filter
    // distinguishes a terminal notification from the started event that
    // shares the child command's sequence.
    Ok(tx
        .query_row(
            "select 1 from history_events
             where run_id = ?1
               and command_seq = ?2
               and event_type in (
                 'child_workflow_completed',
                 'child_workflow_failed',
                 'child_workflow_cancelled'
               )
             limit 1",
            params![command_id.run_id.0, command_id.seq.0],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?
        .is_some())
}

fn cancel_children_for_parent(tx: &Transaction<'_>, parent_run_id: &RunId) -> Result<()> {
    tx.execute(
        "update child_outbox
         set dispatched = 1
         where parent_run_id = ?1
           and child_run_id is null
           and parent_close_policy = ?2",
        params![
            parent_run_id.0,
            parent_close_policy_to_str(ParentClosePolicy::Cancel)
        ],
    )
    .map_err(sqlite_error)?;

    let children = {
        let mut stmt = tx
            .prepare(
                "select run_id, current_event_id
                 from workflow_instances
                 where parent_run_id = ?1
                   and parent_close_policy = ?2
                   and terminal = 0",
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![
                    parent_run_id.0,
                    parent_close_policy_to_str(ParentClosePolicy::Cancel)
                ],
                |row| Ok((RunId::new(row.get::<_, String>(0)?), row.get::<_, u64>(1)?)),
            )
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?
    };
    for (child_run_id, tail) in children {
        let terminal_event = HistoryEventData::WorkflowCancelled {
            reason: format!("parent workflow `{parent_run_id}` closed"),
        };
        let event_id = EventId(tail).next();
        insert_history_event(tx, &child_run_id, event_id, terminal_event)?;
        cleanup_run_operational_state(tx, &child_run_id, TerminalCleanup::Closed)?;
        tx.execute(
            "update workflow_instances
             set current_event_id = ?1,
                 workflow_claim_token = null,
                 terminal = 1,
                 ready_reason = null,
                 ready_at_ms = 0
             where run_id = ?2",
            params![event_id.0, child_run_id.0],
        )
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn cancel_command_operational_state(tx: &Transaction<'_>, command_id: &CommandId) -> Result<()> {
    let activity_id = ActivityId::new(command_id);
    let map_prefix = format!("{}:map:%", activity_id.0);
    tx.execute(
        "update activity_tasks
         set completed = 1,
             claim_token = null,
             heartbeat_deadline_at_ms = null
         where activity_id = ?1 or activity_id like ?2",
        params![activity_id.0, map_prefix],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update activity_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![map_command_key(command_id)],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update child_workflow_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![map_command_key(command_id)],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update child_outbox
         set dispatched = 1
         where outbox_id = ?1
            or (parent_run_id = ?2 and command_seq = ?3)",
        params![
            child_outbox_id_for_command(command_id),
            command_id.run_id.0,
            command_id.seq.0
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn child_outbox_id(message: &ChildStartOutboxMessage) -> String {
    if let Some(item) = &message.child_map_item {
        return format!(
            "{}:{}:child-map:{}",
            item.map_command_id.run_id, item.map_command_id.seq.0, item.item_ordinal
        );
    }
    child_outbox_id_for_command(&message.command_id)
}

fn child_outbox_id_for_command(command_id: &CommandId) -> String {
    format!("{}:{}:child-start", command_id.run_id, command_id.seq.0)
}

fn insert_activity_map(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    namespace: &str,
    map_task: &ActivityMapTask,
) -> Result<()> {
    let manifest_payload = hydrate_activity_map_input_manifest_from_storage(
        tx,
        config,
        map_task.input_manifest.clone(),
    )?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into activity_maps
         (map_command_id, namespace, run_id, command_seq, task, item_count,
          next_ordinal, in_flight, completed)
         values (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, 0)",
        params![
            map_command_key(&map_task.map_command_id),
            namespace,
            map_task.map_command_id.run_id.0,
            map_task.map_command_id.seq.0,
            task_blob,
            u64::try_from(manifest.item_count).unwrap_or(u64::MAX),
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn insert_child_workflow_map(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    namespace: &str,
    map_task: &ChildWorkflowMapTask,
) -> Result<()> {
    let manifest_payload = hydrate_activity_map_input_manifest_from_storage(
        tx,
        config,
        map_task.input_manifest.clone(),
    )?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into child_workflow_maps
         (map_command_id, namespace, run_id, command_seq, task, item_count,
          next_ordinal, in_flight, completed)
         values (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, 0)",
        params![
            map_command_key(&map_task.map_command_id),
            namespace,
            map_task.map_command_id.run_id.0,
            map_task.map_command_id.seq.0,
            task_blob,
            u64::try_from(manifest.item_count).unwrap_or(u64::MAX),
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn insert_child_outbox(
    tx: &Transaction<'_>,
    namespace: &str,
    message: &ChildStartOutboxMessage,
) -> Result<()> {
    let blob =
        rmp_serde::to_vec_named(message).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into child_outbox
         (outbox_id, namespace, parent_run_id, command_seq, child_workflow_id,
          parent_close_policy, message,
          dispatched, child_run_id)
        values (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, null)
         on conflict(outbox_id) do nothing",
        params![
            child_outbox_id(message),
            namespace,
            message.command_id.run_id.0,
            message.command_id.seq.0,
            message.workflow_id.0,
            parent_close_policy_to_str(message.parent_close_policy),
            blob
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn dispatch_child_start(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    outbox_id: &str,
) -> Result<()> {
    let Some((message_blob, dispatched)) = tx
        .query_row(
            "select message, dispatched from child_outbox where outbox_id = ?1",
            params![outbox_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Ok(());
    };
    if dispatched {
        return Ok(());
    }
    let message: ChildStartOutboxMessage = rmp_serde::from_slice(&message_blob)
        .map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let Some((namespace, parent_terminal)) = tx
        .query_row(
            "select namespace, terminal from workflow_instances where run_id = ?1",
            params![message.command_id.run_id.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(message.command_id.run_id.clone()));
    };
    if parent_terminal && message.parent_close_policy == ParentClosePolicy::Cancel {
        mark_child_outbox_dispatched(tx, outbox_id, None)?;
        return Ok(());
    }

    let existing = tx
        .query_row(
            "select run_id, parent_run_id, parent_command_seq, parent_child_map_ordinal
             from workflow_instances
             where namespace = ?1 and workflow_id = ?2",
            params![namespace.as_str(), message.workflow_id.0],
            |row| {
                Ok((
                    RunId::new(row.get::<_, String>(0)?),
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<u64>>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;

    let child_run_id =
        if let Some((run_id, parent_run_id, parent_seq, parent_map_ordinal)) = existing {
            let expected_map_ordinal = message
                .child_map_item
                .as_ref()
                .map(|item| item.item_ordinal);
            let same_child = parent_run_id.as_deref() == Some(message.command_id.run_id.0.as_str())
                && parent_seq == Some(message.command_id.seq.0)
                && parent_map_ordinal == expected_map_ordinal;
            if !same_child {
                let failure = crate::DurableFailure::non_retryable(
                    "durust.child_workflow_id_conflict",
                    format!("workflow id `{}` is already started", message.workflow_id),
                );
                if let Some(item) = message.child_map_item.clone() {
                    complete_child_workflow_map_item(
                        tx,
                        config,
                        item,
                        ChildWorkflowMapItemOutcome::Failed { failure },
                    )?;
                } else {
                    append_child_start_failed(tx, &message.command_id, failure)?;
                }
                mark_child_outbox_dispatched(tx, outbox_id, None)?;
                return Ok(());
            }
            run_id
        } else {
            start_child_run(tx, &namespace, &message)?
        };

    append_child_started(tx, &message, &child_run_id)?;
    mark_child_outbox_dispatched(tx, outbox_id, Some(&child_run_id))?;
    Ok(())
}

fn start_child_run(
    tx: &Transaction<'_>,
    namespace: &str,
    message: &ChildStartOutboxMessage,
) -> Result<RunId> {
    let run_id = RunId::new(format!("run-{}", next_counter(tx, "run")?));
    let parent_child_map_ordinal = message
        .child_map_item
        .as_ref()
        .map(|item| i64::try_from(item.item_ordinal).unwrap_or(i64::MAX));
    tx.execute(
        "insert into workflow_instances
         (namespace, workflow_id, run_id, workflow_name, workflow_version, task_queue,
          current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
          parent_run_id, parent_command_seq, parent_close_policy, parent_child_map_ordinal)
         values (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, 0, null, 0, ?8, ?9, ?10, ?11)",
        params![
            namespace,
            message.workflow_id.0,
            run_id.0,
            message.workflow_type.name,
            message.workflow_type.version,
            message.task_queue.0,
            reason_to_str(&WorkflowTaskReason::WorkflowStarted),
            message.command_id.run_id.0,
            message.command_id.seq.0,
            parent_close_policy_to_str(message.parent_close_policy),
            parent_child_map_ordinal
        ],
    )
    .map_err(sqlite_error)?;
    insert_history_event(
        tx,
        &run_id,
        EventId(1),
        HistoryEventData::WorkflowStarted {
            workflow_type: message.workflow_type.clone(),
            input: message.input.clone(),
        },
    )?;
    Ok(run_id)
}

fn append_child_started(
    tx: &Transaction<'_>,
    message: &ChildStartOutboxMessage,
    child_run_id: &RunId,
) -> Result<()> {
    if message.child_map_item.is_some() {
        return Ok(());
    }
    if child_event_exists(tx, &message.command_id)? {
        return Ok(());
    }
    let Some((tail, terminal)) = parent_tail_and_terminal(tx, &message.command_id.run_id)? else {
        return Err(Error::RunNotFound(message.command_id.run_id.clone()));
    };
    if terminal {
        return Ok(());
    }
    let event_id = EventId(tail).next();
    insert_history_event(
        tx,
        &message.command_id.run_id,
        event_id,
        HistoryEventData::ChildWorkflowStarted(crate::ChildWorkflowStarted {
            command_id: message.command_id.clone(),
            workflow_id: message.workflow_id.clone(),
            run_id: child_run_id.clone(),
        }),
    )?;
    set_workflow_ready(
        tx,
        &message.command_id.run_id,
        event_id,
        WorkflowTaskReason::ChildWorkflowStarted,
    )
}

fn append_child_start_failed(
    tx: &Transaction<'_>,
    command_id: &CommandId,
    failure: crate::DurableFailure,
) -> Result<()> {
    if child_event_exists(tx, command_id)? {
        return Ok(());
    }
    let Some((tail, terminal)) = parent_tail_and_terminal(tx, &command_id.run_id)? else {
        return Err(Error::RunNotFound(command_id.run_id.clone()));
    };
    if terminal {
        return Ok(());
    }
    let event_id = EventId(tail).next();
    insert_history_event(
        tx,
        &command_id.run_id,
        event_id,
        HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
            command_id: command_id.clone(),
            failure,
        }),
    )?;
    set_workflow_ready(
        tx,
        &command_id.run_id,
        event_id,
        WorkflowTaskReason::ChildWorkflowFailed,
    )
}

fn mark_child_outbox_dispatched(
    tx: &Transaction<'_>,
    outbox_id: &str,
    child_run_id: Option<&RunId>,
) -> Result<()> {
    tx.execute(
        "update child_outbox
         set dispatched = 1, child_run_id = ?1
         where outbox_id = ?2",
        params![child_run_id.map(|run_id| run_id.0.as_str()), outbox_id],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn parent_tail_and_terminal(tx: &Transaction<'_>, run_id: &RunId) -> Result<Option<(u64, bool)>> {
    tx.query_row(
        "select current_event_id, terminal from workflow_instances where run_id = ?1",
        params![run_id.0],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
    )
    .optional()
    .map_err(sqlite_error)
}

fn set_workflow_ready(
    tx: &Transaction<'_>,
    run_id: &RunId,
    event_id: EventId,
    reason: WorkflowTaskReason,
) -> Result<()> {
    tx.execute(
        "update workflow_instances
         set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
         where run_id = ?3",
        params![event_id.0, reason_to_str(&reason), run_id.0],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn child_event_exists(tx: &Transaction<'_>, command_id: &CommandId) -> Result<bool> {
    Ok(tx
        .query_row(
            "select 1 from history_events
             where run_id = ?1
               and command_seq = ?2
               and event_type in (
                 'child_workflow_started',
                 'child_workflow_completed',
                 'child_workflow_failed',
                 'child_workflow_cancelled'
               )
             limit 1",
            params![command_id.run_id.0, command_id.seq.0],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?
        .is_some())
}

fn materialize_activity_map_items(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    map_command_id: &CommandId,
) -> Result<()> {
    let key = map_command_key(map_command_id);
    let Some((namespace, task_blob, item_count, next_ordinal, in_flight, completed)) = tx
        .query_row(
            "select namespace, task, item_count, next_ordinal, in_flight, completed
             from activity_maps
             where map_command_id = ?1",
            params![key.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Ok(());
    };
    if completed {
        return Ok(());
    }

    let task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let mut next_ordinal = next_ordinal;
    let mut in_flight = in_flight;
    let max_in_flight = u64::try_from(task.max_in_flight.max(1)).unwrap_or(u64::MAX);
    let manifest_payload =
        hydrate_activity_map_input_manifest_from_storage(tx, config, task.input_manifest.clone())?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;

    while in_flight < max_in_flight && next_ordinal < item_count {
        let input = activity_map_input_at(&manifest, next_ordinal)?;
        let activity_id = ActivityId::map_item(map_command_id, next_ordinal);
        let item_task = ActivityTask {
            activity_id: activity_id.clone(),
            run_id: map_command_id.run_id.clone(),
            command_id: map_command_id.clone(),
            activity_name: task.activity_name.clone(),
            task_queue: task.task_queue.clone(),
            retry_policy: task.retry_policy.clone(),
            start_to_close_timeout: task.start_to_close_timeout,
            heartbeat_timeout: task.heartbeat_timeout,
            attempt: 1,
            input,
            map_item: Some(ActivityMapItem {
                map_command_id: map_command_id.clone(),
                item_ordinal: next_ordinal,
            }),
        };
        let item_task = normalize_activity_task_for_storage(tx, config, item_task)?;
        let item_blob = rmp_serde::to_vec_named(&item_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            "insert into activity_tasks
             (activity_id, namespace, run_id, activity_name, task_queue, task,
              claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
             values (?1, ?2, ?3, ?4, ?5, ?6, null, 0, ?7, null)",
            params![
                activity_id.0,
                namespace.as_str(),
                item_task.run_id.0,
                item_task.activity_name.0,
                item_task.task_queue.0,
                item_blob,
                activity_timeout_at_ms(item_task.start_to_close_timeout),
            ],
        )
        .map_err(sqlite_error)?;
        next_ordinal += 1;
        in_flight += 1;
    }

    tx.execute(
        "update activity_maps
         set next_ordinal = ?1, in_flight = ?2
         where map_command_id = ?3",
        params![next_ordinal, in_flight, key.as_str()],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn materialize_child_workflow_map_items(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    map_command_id: &CommandId,
) -> Result<()> {
    let key = map_command_key(map_command_id);
    let Some((namespace, task_blob, item_count, mut next_ordinal, mut in_flight, completed)) = tx
        .query_row(
            "select namespace, task, item_count, next_ordinal, in_flight, completed
             from child_workflow_maps
             where map_command_id = ?1",
            params![key.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Ok(());
    };
    if completed {
        return Ok(());
    }

    let task: ChildWorkflowMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let max_in_flight = u64::try_from(task.max_in_flight.max(1)).unwrap_or(u64::MAX);
    let manifest_payload =
        hydrate_activity_map_input_manifest_from_storage(tx, config, task.input_manifest.clone())?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;

    while in_flight < max_in_flight && next_ordinal < item_count {
        let input = activity_map_input_at(&manifest, next_ordinal)?;
        let child_map_item = ChildWorkflowMapItem {
            map_command_id: map_command_id.clone(),
            item_ordinal: next_ordinal,
        };
        let message = ChildStartOutboxMessage {
            command_id: map_command_id.clone(),
            workflow_type: task.workflow_type.clone(),
            workflow_id: WorkflowId::new(format!("{}/{}", task.workflow_id_prefix, next_ordinal)),
            task_queue: task.task_queue.clone(),
            input,
            parent_close_policy: task.parent_close_policy,
            child_map_item: Some(child_map_item),
        };
        let message = normalize_child_start_message_for_storage(tx, config, message)?;
        insert_child_outbox(tx, namespace.as_str(), &message)?;
        next_ordinal = next_ordinal.saturating_add(1);
        in_flight = in_flight.saturating_add(1);
    }

    tx.execute(
        "update child_workflow_maps
         set next_ordinal = ?1, in_flight = ?2
         where map_command_id = ?3",
        params![next_ordinal, in_flight, key.as_str()],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn complete_child_workflow_map_item(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    map_item: ChildWorkflowMapItem,
    outcome: ChildWorkflowMapItemOutcome,
) -> Result<()> {
    let key = map_command_key(&map_item.map_command_id);
    let Some((task_blob, item_count, completed)) = tx
        .query_row(
            "select task, item_count, completed
             from child_workflow_maps
             where map_command_id = ?1",
            params![key.as_str()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::Backend(format!(
            "child workflow map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    if completed {
        return Ok(());
    }
    if map_item.item_ordinal >= item_count {
        return Err(Error::Backend(format!(
            "child workflow map item ordinal {} out of bounds",
            map_item.item_ordinal
        )));
    }

    let map_task: ChildWorkflowMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let outcome = normalize_child_workflow_map_outcome_for_storage(tx, config, outcome)?;
    let outcome_blob =
        rmp_serde::to_vec_named(&outcome).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    let inserted = tx
        .execute(
            "insert or ignore into child_workflow_map_results(map_command_id, item_ordinal, outcome)
             values (?1, ?2, ?3)",
            params![key.as_str(), map_item.item_ordinal, outcome_blob],
        )
        .map_err(sqlite_error)?;
    if inserted == 0 {
        return Ok(());
    }

    tx.execute(
        "update child_workflow_maps
         set in_flight = case when in_flight > 0 then in_flight - 1 else 0 end
         where map_command_id = ?1",
        params![key.as_str()],
    )
    .map_err(sqlite_error)?;

    if map_task.failure_mode == ChildWorkflowMapFailureMode::FailFast {
        let failure = match &outcome {
            ChildWorkflowMapItemOutcome::Failed { failure } => Some(failure.clone()),
            ChildWorkflowMapItemOutcome::Cancelled { reason } => {
                Some(crate::DurableFailure::non_retryable(
                    "durust.child_workflow_cancelled",
                    format!(
                        "child workflow map item {} was cancelled: {reason}",
                        map_item.item_ordinal
                    ),
                ))
            }
            ChildWorkflowMapItemOutcome::Succeeded { .. } => None,
        };
        if let Some(failure) = failure {
            append_child_workflow_map_failed(tx, config, &map_item.map_command_id, failure)?;
            cancel_child_workflow_map_children(tx, &map_item.map_command_id)?;
            return Ok(());
        }
    }

    let outcome_count = tx
        .query_row(
            "select count(*) from child_workflow_map_results where map_command_id = ?1",
            params![key.as_str()],
            |row| row.get::<_, u64>(0),
        )
        .map_err(sqlite_error)?;
    if outcome_count < item_count {
        materialize_child_workflow_map_items(tx, config, &map_item.map_command_id)?;
        return Ok(());
    }

    let outcomes = child_workflow_map_outcomes(tx, key.as_str())?;
    append_child_workflow_map_completed(tx, config, &map_task, item_count, outcomes)
}

fn append_child_workflow_map_completed(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    map_task: &ChildWorkflowMapTask,
    item_count: u64,
    outcomes: Vec<ChildWorkflowMapItemOutcome>,
) -> Result<()> {
    let input_manifest_payload = hydrate_activity_map_input_manifest_from_storage(
        tx,
        config,
        map_task.input_manifest.clone(),
    )?;
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&input_manifest_payload)?;
    let result_manifest = encode_child_workflow_map_result_manifest_with_codec(
        map_task.result_manifest_name.clone(),
        outcomes.clone(),
        &input_manifest.page_lengths,
        config.codec,
    )?;
    let result_manifest =
        normalize_child_workflow_map_result_manifest_for_storage(tx, config, result_manifest)?;
    let Some((tail, terminal)) = parent_tail_and_terminal(tx, &map_task.map_command_id.run_id)?
    else {
        return Err(Error::RunNotFound(map_task.map_command_id.run_id.clone()));
    };
    if terminal {
        return Ok(());
    }
    let event_id = EventId(tail).next();
    let success_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ChildWorkflowMapItemOutcome::Succeeded { .. }))
        .count();
    let failure_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ChildWorkflowMapItemOutcome::Failed { .. }))
        .count();
    let cancellation_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ChildWorkflowMapItemOutcome::Cancelled { .. }))
        .count();
    insert_history_event(
        tx,
        &map_task.map_command_id.run_id,
        event_id,
        HistoryEventData::ChildWorkflowMapCompleted(crate::ChildWorkflowMapCompleted {
            command_id: map_task.map_command_id.clone(),
            result_manifest,
            item_count: usize::try_from(item_count).unwrap_or(usize::MAX),
            success_count,
            failure_count,
            cancellation_count,
        }),
    )?;
    tx.execute(
        "update child_workflow_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![map_command_key(&map_task.map_command_id)],
    )
    .map_err(sqlite_error)?;
    set_workflow_ready(
        tx,
        &map_task.map_command_id.run_id,
        event_id,
        WorkflowTaskReason::ChildWorkflowMapCompleted,
    )
}

fn append_child_workflow_map_failed(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    map_command_id: &CommandId,
    failure: crate::DurableFailure,
) -> Result<()> {
    let Some((tail, terminal)) = parent_tail_and_terminal(tx, &map_command_id.run_id)? else {
        return Err(Error::RunNotFound(map_command_id.run_id.clone()));
    };
    if terminal {
        return Ok(());
    }
    let event_id = EventId(tail).next();
    let failure = normalize_failure_for_storage(tx, config, failure)?;
    insert_history_event(
        tx,
        &map_command_id.run_id,
        event_id,
        HistoryEventData::ChildWorkflowMapFailed(crate::ChildWorkflowMapFailed {
            command_id: map_command_id.clone(),
            failure,
        }),
    )?;
    tx.execute(
        "update child_workflow_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![map_command_key(map_command_id)],
    )
    .map_err(sqlite_error)?;
    set_workflow_ready(
        tx,
        &map_command_id.run_id,
        event_id,
        WorkflowTaskReason::ChildWorkflowMapFailed,
    )
}

fn cancel_child_workflow_map_children(
    tx: &Transaction<'_>,
    map_command_id: &CommandId,
) -> Result<()> {
    tx.execute(
        "update child_outbox
         set dispatched = 1
         where parent_run_id = ?1
           and command_seq = ?2
           and child_run_id is null",
        params![map_command_id.run_id.0, map_command_id.seq.0],
    )
    .map_err(sqlite_error)?;

    let children = {
        let mut stmt = tx
            .prepare(
                "select run_id, current_event_id
                 from workflow_instances
                 where parent_run_id = ?1
                   and parent_command_seq = ?2
                   and parent_child_map_ordinal is not null
                   and terminal = 0
                   and not exists (
                     select 1
                     from history_events h
                     where h.run_id = workflow_instances.run_id
                       and h.event_type in (
                         'workflow_completed',
                         'workflow_failed',
                         'workflow_cancelled',
                         'workflow_continued_as_new'
                       )
                   )",
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![map_command_id.run_id.0, map_command_id.seq.0],
                |row| Ok((RunId::new(row.get::<_, String>(0)?), row.get::<_, u64>(1)?)),
            )
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?
    };
    for (child_run_id, tail) in children {
        let terminal_event = HistoryEventData::WorkflowCancelled {
            reason: format!(
                "child workflow map `{}`:{} failed",
                map_command_id.run_id, map_command_id.seq.0
            ),
        };
        let event_id = EventId(tail).next();
        insert_history_event(tx, &child_run_id, event_id, terminal_event)?;
        cleanup_run_operational_state(tx, &child_run_id, TerminalCleanup::Closed)?;
        tx.execute(
            "update workflow_instances
             set current_event_id = ?1,
                 workflow_claim_token = null,
                 terminal = 1,
                 ready_reason = null,
                 ready_at_ms = 0
             where run_id = ?2",
            params![event_id.0, child_run_id.0],
        )
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn child_workflow_map_outcomes(
    tx: &Transaction<'_>,
    map_command_key: &str,
) -> Result<Vec<ChildWorkflowMapItemOutcome>> {
    let mut stmt = tx
        .prepare(
            "select outcome
             from child_workflow_map_results
             where map_command_id = ?1
             order by item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map(params![map_command_key], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    rows.map(|row| {
        let blob = row.map_err(sqlite_error)?;
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .collect()
}

fn complete_map_item(
    tx: &Transaction<'_>,
    config: &PayloadStorageConfig,
    task: ActivityTask,
    map_item: ActivityMapItem,
    result: PayloadRef,
    activity_id: ActivityId,
) -> Result<CompleteActivityOutcome> {
    tx.execute(
        "update activity_tasks
         set completed = 1,
             heartbeat_deadline_at_ms = null
         where activity_id = ?1",
        params![activity_id.0],
    )
    .map_err(sqlite_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let Some((task_blob, item_count, completed)) = tx
        .query_row(
            "select task, item_count, completed
             from activity_maps
             where map_command_id = ?1",
            params![key.as_str()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    if completed {
        return Ok(CompleteActivityOutcome::AlreadyCompleted);
    }
    if map_item.item_ordinal >= item_count {
        return Err(Error::Backend(format!(
            "activity map item ordinal {} out of bounds",
            map_item.item_ordinal
        )));
    }

    let result_blob =
        rmp_serde::to_vec_named(&result).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert or ignore into activity_map_results(map_command_id, item_ordinal, result)
         values (?1, ?2, ?3)",
        params![key.as_str(), map_item.item_ordinal, result_blob],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update activity_maps
         set in_flight = case when in_flight > 0 then in_flight - 1 else 0 end
         where map_command_id = ?1",
        params![key.as_str()],
    )
    .map_err(sqlite_error)?;

    let success_count = tx
        .query_row(
            "select count(*) from activity_map_results where map_command_id = ?1",
            params![key.as_str()],
            |row| row.get::<_, u64>(0),
        )
        .map_err(sqlite_error)?;

    if success_count < item_count {
        materialize_activity_map_items(tx, config, &map_item.map_command_id)?;
        let tail = tx
            .query_row(
                "select current_event_id from workflow_instances where run_id = ?1",
                params![task.run_id.0],
                |row| row.get::<_, u64>(0),
            )
            .map_err(sqlite_error)?;
        return Ok(CompleteActivityOutcome::Completed {
            event_id: EventId(tail),
        });
    }

    let map_task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let input_manifest_payload = hydrate_activity_map_input_manifest_from_storage(
        tx,
        config,
        map_task.input_manifest.clone(),
    )?;
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&input_manifest_payload)?;
    let result_refs = activity_map_results(tx, key.as_str())?;
    let result_manifest = encode_activity_map_result_manifest_with_codec(
        map_task.result_manifest_name,
        result_refs,
        &input_manifest.page_lengths,
        config.codec,
    )?;
    let Some((tail, terminal)) = tx
        .query_row(
            "select current_event_id, terminal from workflow_instances where run_id = ?1",
            params![task.run_id.0],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = EventId(tail).next();
    let item_count_usize = usize::try_from(item_count).unwrap_or(usize::MAX);
    let success_count_usize = usize::try_from(success_count).unwrap_or(usize::MAX);
    let result_manifest =
        normalize_activity_map_result_manifest_for_storage(tx, config, result_manifest)?;
    insert_history_event(
        tx,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapCompleted(crate::ActivityMapCompleted {
            command_id: map_item.map_command_id,
            result_manifest,
            item_count: item_count_usize,
            success_count: success_count_usize,
            failure_count: 0,
        }),
    )?;
    tx.execute(
        "update activity_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![key.as_str()],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update workflow_instances
         set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
         where run_id = ?3",
        params![
            event_id.0,
            reason_to_str(&WorkflowTaskReason::ActivityMapCompleted),
            task.run_id.0
        ],
    )
    .map_err(sqlite_error)?;
    Ok(CompleteActivityOutcome::Completed { event_id })
}

fn fail_map_item(
    tx: &Transaction<'_>,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: crate::DurableFailure,
    activity_id: ActivityId,
) -> Result<FailActivityOutcome> {
    tx.execute(
        "update activity_tasks
         set completed = 1,
             heartbeat_deadline_at_ms = null
         where activity_id = ?1",
        params![activity_id.0],
    )
    .map_err(sqlite_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let completed = tx
        .query_row(
            "select completed from activity_maps where map_command_id = ?1",
            params![key.as_str()],
            |row| row.get::<_, bool>(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .unwrap_or(false);
    if completed {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    }

    let Some((tail, terminal)) = tx
        .query_row(
            "select current_event_id, terminal from workflow_instances where run_id = ?1",
            params![task.run_id.0],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = EventId(tail).next();
    insert_history_event(
        tx,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapFailed(crate::ActivityMapFailed {
            command_id: map_item.map_command_id,
            failure,
        }),
    )?;
    tx.execute(
        "update activity_maps
         set completed = 1, in_flight = 0
         where map_command_id = ?1",
        params![key.as_str()],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update workflow_instances
         set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
         where run_id = ?3",
        params![
            event_id.0,
            reason_to_str(&WorkflowTaskReason::ActivityMapFailed),
            task.run_id.0
        ],
    )
    .map_err(sqlite_error)?;
    Ok(FailActivityOutcome::Failed { event_id })
}

fn timeout_activity(
    tx: &Transaction<'_>,
    activity_id: ActivityId,
    now: TimestampMs,
) -> Result<bool> {
    let Some((task_blob, completed, timeout_at_ms, heartbeat_deadline_at_ms)) = tx
        .query_row(
            "select task, completed, timeout_at_ms, heartbeat_deadline_at_ms
             from activity_tasks
             where activity_id = ?1",
            params![activity_id.0],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Ok(false);
    };
    let start_timeout_due = timeout_at_ms.is_some_and(|timeout_at_ms| timeout_at_ms <= now.0);
    let heartbeat_timeout_due = heartbeat_deadline_at_ms.is_some_and(|deadline| deadline <= now.0);
    if completed || !(start_timeout_due || heartbeat_timeout_due) {
        return Ok(false);
    }
    let timed_out_by_heartbeat = timed_out_by_heartbeat(heartbeat_timeout_due, start_timeout_due);

    let task: ActivityTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    if let ActivityFailureDecision::Retry { next_attempt } = activity_timeout_decision(&task) {
        let mut retry_task = task.clone();
        retry_task.attempt = next_attempt;
        let retry_blob = rmp_serde::to_vec_named(&retry_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            "update activity_tasks
             set task = ?1,
                 claim_token = null,
                 timeout_at_ms = ?2,
                 heartbeat_deadline_at_ms = null
             where activity_id = ?3",
            params![
                retry_blob,
                activity_timeout_at_ms_from(now, retry_task.start_to_close_timeout),
                activity_id.0
            ],
        )
        .map_err(sqlite_error)?;
        return Ok(true);
    }

    tx.execute(
        "update activity_tasks
         set completed = 1,
             heartbeat_deadline_at_ms = null
         where activity_id = ?1",
        params![activity_id.0],
    )
    .map_err(sqlite_error)?;

    if let Some(map_item) = task.map_item.clone() {
        fail_map_item(
            tx,
            task.clone(),
            map_item,
            crate::DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
            ),
            activity_id,
        )?;
        return Ok(true);
    }

    let Some((tail, terminal)) = tx
        .query_row(
            "select current_event_id, terminal from workflow_instances where run_id = ?1",
            params![task.run_id.0],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = EventId(tail).next();
    insert_history_event(
        tx,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityTimedOut(crate::ActivityTimedOut {
            command_id: task.command_id,
            message: timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
        }),
    )?;
    tx.execute(
        "update workflow_instances
         set current_event_id = ?1, ready_reason = ?2, ready_at_ms = 0
         where run_id = ?3",
        params![
            event_id.0,
            reason_to_str(&WorkflowTaskReason::ActivityTimedOut),
            task.run_id.0
        ],
    )
    .map_err(sqlite_error)?;
    Ok(true)
}

fn activity_map_results(tx: &Transaction<'_>, map_command_key: &str) -> Result<Vec<PayloadRef>> {
    let mut stmt = tx
        .prepare(
            "select result
             from activity_map_results
             where map_command_id = ?1
             order by item_ordinal asc",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map(params![map_command_key], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    rows.map(|row| {
        let blob = row.map_err(sqlite_error)?;
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .collect()
}

fn activity_map_is_completed(tx: &Transaction<'_>, map_command_id: &CommandId) -> Result<bool> {
    Ok(tx
        .query_row(
            "select completed from activity_maps where map_command_id = ?1",
            params![map_command_key(map_command_id)],
            |row| row.get::<_, bool>(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .unwrap_or(false))
}

fn map_command_key(command_id: &CommandId) -> String {
    format!("{}:{}", command_id.run_id, command_id.seq.0)
}

fn next_counter(tx: &Transaction<'_>, key: &str) -> Result<u64> {
    let next = tx
        .query_row(
            "select value from meta where key = ?1",
            params![key],
            |row| row.get::<_, u64>(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .unwrap_or(0)
        + 1;
    tx.execute(
        "insert into meta(key, value) values (?1, ?2)
         on conflict(key) do update set value = excluded.value",
        params![key, next],
    )
    .map_err(sqlite_error)?;
    Ok(next)
}

fn insert_history_event(
    tx: &Transaction<'_>,
    run_id: &RunId,
    event_id: EventId,
    data: HistoryEventData,
) -> Result<()> {
    let event_type = event_type_to_str(&data.event_type());
    let command_seq = data.command_seq().map(|seq| seq.0);
    let blob =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into history_events(run_id, event_id, event_type, command_seq, data)
         values (?1, ?2, ?3, ?4, ?5)",
        params![run_id.0, event_id.0, event_type, command_seq, blob],
    )
    .map_err(sqlite_error)?;
    index_workflow_change_marker(tx, run_id, event_id, &data)?;
    Ok(())
}

fn index_workflow_change_marker(
    tx: &Transaction<'_>,
    run_id: &RunId,
    event_id: EventId,
    data: &HistoryEventData,
) -> Result<()> {
    let (change_id, version, marker_kind, command_seq) = match data {
        HistoryEventData::VersionMarker(marker) => (
            marker.change_id.clone(),
            marker.version,
            WorkflowChangeMarkerKind::Version,
            marker.command_id.seq,
        ),
        HistoryEventData::DeprecatedPatchMarker(marker) => (
            marker.patch_id.clone(),
            1,
            WorkflowChangeMarkerKind::DeprecatedPatch,
            marker.command_id.seq,
        ),
        _ => return Ok(()),
    };
    let Some((namespace, workflow_id, workflow_name, workflow_version, terminal)) = tx
        .query_row(
            "select namespace, workflow_id, workflow_name, workflow_version, terminal
             from workflow_instances where run_id = ?1",
            params![run_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u32>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
    else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    let _status = if terminal {
        WorkflowChangeVersionStatus::Closed
    } else {
        WorkflowChangeVersionStatus::Open
    };
    tx.execute(
        "insert into workflow_change_versions
         (namespace, workflow_id, workflow_name, workflow_version, run_id, change_id,
          version, marker_kind, command_seq, first_event_id, last_seen_at_ms)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         on conflict(run_id, change_id) do update set
            version = excluded.version,
            marker_kind = excluded.marker_kind,
            command_seq = excluded.command_seq,
            first_event_id = excluded.first_event_id,
            last_seen_at_ms = excluded.last_seen_at_ms",
        params![
            namespace,
            workflow_id,
            workflow_name,
            workflow_version,
            run_id.0,
            change_id,
            version,
            marker_kind_to_str(marker_kind),
            command_seq.0,
            event_id.0,
            unix_epoch_millis(),
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn sqlite_error(err: rusqlite::Error) -> Error {
    Error::Backend(err.to_string())
}

fn configure_connection_defaults(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error)?;
    let synchronous: u8 = conn
        .query_row("pragma synchronous", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if synchronous != 2 {
        return Err(Error::Backend(format!(
            "sqlite refused FULL synchronous mode and returned `{synchronous}`"
        )));
    }
    Ok(())
}

fn configure_journal_mode(conn: &Connection) -> Result<()> {
    let journal_mode: String = conn
        .query_row("pragma journal_mode = WAL", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        Err(Error::Backend(format!(
            "sqlite refused WAL journal mode and returned `{journal_mode}`"
        )))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_connections_set_default_busy_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("busy-timeout.sqlite3")).unwrap();
        let conn = backend.connection().unwrap();
        let timeout_ms: u64 = conn
            .query_row("pragma busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(timeout_ms, DEFAULT_BUSY_TIMEOUT.as_millis() as u64);
    }

    #[test]
    fn sqlite_open_sets_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("wal.sqlite3")).unwrap();
        let conn = backend.connection().unwrap();
        let journal_mode: String = conn
            .query_row("pragma journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    }

    #[test]
    fn sqlite_connections_set_full_synchronous_mode() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("full-sync.sqlite3")).unwrap();
        let conn = backend.connection().unwrap();
        let synchronous: u8 = conn
            .query_row("pragma synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(synchronous, 2);
    }

    #[test]
    fn sqlite_open_creates_queue_perf_indexes_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue-indexes.sqlite3");
        drop(SqliteBackend::open(&path).unwrap());

        let reopened = SqliteBackend::open(&path).unwrap();
        let conn = reopened.connection().unwrap();
        for index_name in ["idx_workflow_instances_ready", "idx_activity_tasks_claim"] {
            let exists = conn
                .query_row(
                    "select 1 from sqlite_master where type = 'index' and name = ?1",
                    params![index_name],
                    |_| Ok(()),
                )
                .optional()
                .unwrap()
                .is_some();
            assert!(exists, "missing SQLite index `{index_name}`");
        }
    }

    // Databases created before `history_events.command_seq` must have their
    // child lifecycle rows backfilled on the open that adds the column, and
    // the backfilled values must feed child-terminal dedup; otherwise a
    // re-delivered terminal notification would append a duplicate child event
    // to the parent (silent replay corruption). Follows the Phase 2G legacy
    // reopen pattern: build the history, strip the column with a raw
    // connection, and reopen through `SqliteBackend::open`.
    #[test]
    fn sqlite_reopen_backfills_command_seq_and_preserves_child_terminal_dedup() {
        use futures::executor::block_on;

        block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("command-seq-backfill.sqlite3");
            let backend = SqliteBackend::open(&path).unwrap();
            let namespace = crate::Namespace::default();
            let parent_type = WorkflowType::new("tests.backfill-parent", 1);
            let child_type = WorkflowType::new("tests.backfill-child", 1);
            let parent_outcome = backend
                .start_workflow(crate::StartWorkflowRequest {
                    namespace: namespace.clone(),
                    workflow_id: WorkflowId::new("wf/backfill-parent"),
                    workflow_type: parent_type.clone(),
                    task_queue: crate::TaskQueue::new("backfill-workflows"),
                    input: crate::encode_payload(&0_u64).unwrap(),
                })
                .await
                .unwrap();
            let parent_run_id = parent_outcome.run_id().clone();
            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("backfill-parent-worker"),
                    ClaimWorkflowTaskOptions {
                        namespace: namespace.clone(),
                        task_queue: crate::TaskQueue::new("backfill-workflows"),
                        registered_workflow_types: vec![parent_type],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("parent workflow task");
            let command_id = crate::command_id(&parent_run_id, 1);
            let input = crate::encode_payload(&7_u64).unwrap();
            let requested = crate::ChildWorkflowStartRequested {
                command_id: command_id.clone(),
                workflow_type: child_type.clone(),
                workflow_id: WorkflowId::new("wf/backfill-child"),
                task_queue: crate::TaskQueue::new("backfill-children"),
                input: input.clone(),
                parent_close_policy: ParentClosePolicy::Abandon,
                fingerprint: crate::child_workflow_fingerprint(
                    child_type.clone(),
                    WorkflowId::new("wf/backfill-child"),
                    crate::payload_digest(&input),
                    crate::TaskQueue::new("backfill-children"),
                    ParentClosePolicy::Abandon,
                ),
            };
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                        )],
                        start_child_workflows: vec![ChildStartOutboxMessage::from_requested(
                            &requested,
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            let dispatched = backend
                .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                    namespace: namespace.clone(),
                    limit: 16,
                })
                .await
                .unwrap();
            assert_eq!(dispatched.dispatched, 1);
            let child_claim = backend
                .claim_workflow_task(
                    WorkerId::new("backfill-child-worker"),
                    ClaimWorkflowTaskOptions {
                        namespace: namespace.clone(),
                        task_queue: crate::TaskQueue::new("backfill-children"),
                        registered_workflow_types: vec![child_type],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("child workflow task");
            let child_run_id = child_claim.run_id.clone();
            // The child completes, appending ChildWorkflowCompleted to the
            // parent through the terminal notification path.
            backend
                .commit_workflow_task(
                    child_claim.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowCompleted {
                                result: crate::encode_payload(&14_u64).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            drop(backend);

            // Simulate the pre-command_seq schema: drop the index that
            // references the column, then the column itself.
            let raw = Connection::open(&path).unwrap();
            raw.execute_batch(
                "drop index if exists idx_history_events_command_seq;
                 alter table history_events drop column command_seq;",
            )
            .unwrap();
            drop(raw);

            // Reopen: the migration adds the column and backfills the legacy
            // child lifecycle rows in the same transaction.
            let reopened = SqliteBackend::open(&path).unwrap();
            let conn = reopened.connection().unwrap();
            let unbackfilled: i64 = conn
                .query_row(
                    "select count(*) from history_events
                     where command_seq is null
                       and event_type in (
                         'child_workflow_started',
                         'child_workflow_completed',
                         'child_workflow_failed',
                         'child_workflow_cancelled'
                       )",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                unbackfilled, 0,
                "legacy child lifecycle rows must be backfilled on reopen"
            );
            let backfilled_seqs: i64 = conn
                .query_row(
                    "select count(*) from history_events
                     where run_id = ?1 and command_seq = 1",
                    params![parent_run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(
                backfilled_seqs >= 2,
                "started + completed rows should carry the child command seq"
            );
            let parent_tail_before: u64 = conn
                .query_row(
                    "select current_event_id from workflow_instances where run_id = ?1",
                    params![parent_run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            // Simulate a re-delivered terminal transition: forge the child
            // live again and cancel it, so notify_parent_of_child_terminal
            // runs against the backfilled dedup data.
            conn.execute(
                "update workflow_instances set terminal = 0 where run_id = ?1",
                params![child_run_id.0],
            )
            .unwrap();
            drop(conn);
            reopened
                .cancel_workflow(crate::CancelWorkflowRequest {
                    namespace,
                    workflow_id: WorkflowId::new("wf/backfill-child"),
                    reason: "duplicate terminal delivery".to_owned(),
                })
                .await
                .unwrap();

            let conn = reopened.connection().unwrap();
            let parent_terminal_events: i64 = conn
                .query_row(
                    "select count(*) from history_events
                     where run_id = ?1
                       and event_type in (
                         'child_workflow_completed',
                         'child_workflow_failed',
                         'child_workflow_cancelled'
                       )",
                    params![parent_run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                parent_terminal_events, 1,
                "backfilled dedup must suppress the duplicate child-terminal notify"
            );
            let parent_tail_after: u64 = conn
                .query_row(
                    "select current_event_id from workflow_instances where run_id = ?1",
                    params![parent_run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(parent_tail_after, parent_tail_before);
        });
    }

    #[test]
    fn terminal_cleanup_deletes_operational_rows_across_reopen() {
        use futures::executor::block_on;

        block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("terminal-cleanup.sqlite3");
            let backend = SqliteBackend::open(&path).unwrap();
            let workflow_type = WorkflowType::new("tests.sqlite-terminal-cleanup", 1);
            let namespace = crate::Namespace::default();
            let outcome = backend
                .start_workflow(crate::StartWorkflowRequest {
                    namespace: namespace.clone(),
                    workflow_id: WorkflowId::new("wf/sqlite-terminal-cleanup"),
                    workflow_type: workflow_type.clone(),
                    task_queue: crate::TaskQueue::new("sqlite-cleanup-workflows"),
                    input: crate::encode_payload(&0_u64).unwrap(),
                })
                .await
                .unwrap();
            let run_id = outcome.run_id().clone();
            for (signal_id, name) in [
                ("signal/cleanup/consumed", "go"),
                ("signal/cleanup/pending", "go"),
            ] {
                backend
                    .signal_workflow(crate::SignalWorkflowRequest {
                        namespace: namespace.clone(),
                        workflow_id: WorkflowId::new("wf/sqlite-terminal-cleanup"),
                        signal_id: crate::SignalId::new(signal_id),
                        signal_name: crate::SignalName::new(name),
                        payload: crate::encode_payload(&"x").unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("sqlite-cleanup-worker"),
                    ClaimWorkflowTaskOptions {
                        namespace: namespace.clone(),
                        task_queue: crate::TaskQueue::new("sqlite-cleanup-workflows"),
                        registered_workflow_types: vec![workflow_type],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("workflow task");
            let activity_command = crate::command_id(&run_id, 1);
            let map_command = crate::command_id(&run_id, 2);
            let signal_command = crate::command_id(&run_id, 3);
            let input = crate::encode_payload(&1_u64).unwrap();
            let scheduled = crate::ActivityScheduled {
                command_id: activity_command.clone(),
                activity_name: crate::ActivityName::new("tests.echo"),
                task_queue: crate::TaskQueue::new("sqlite-cleanup-activities"),
                retry_policy: crate::RetryPolicy::none(),
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                fingerprint: crate::activity_fingerprint(
                    crate::ActivityName::new("tests.echo"),
                    crate::payload_digest(&input),
                    "sha256:test-options".to_owned(),
                ),
                input: input.clone(),
            };
            let input_manifest =
                crate::encode_activity_map_input_manifest(vec![input.clone(), input.clone()], 2)
                    .unwrap();
            let map_task = ActivityMapTask {
                map_command_id: map_command.clone(),
                activity_name: crate::ActivityName::new("tests.echo"),
                task_queue: crate::TaskQueue::new("sqlite-cleanup-activities"),
                retry_policy: crate::RetryPolicy::none(),
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                input_manifest: input_manifest.clone(),
                result_manifest_name: "cleanup-results".to_owned(),
                max_in_flight: 2,
            };
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                scheduled.clone(),
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(
                                crate::ActivityMapScheduled {
                                    command_id: map_command.clone(),
                                    activity_name: crate::ActivityName::new("tests.echo"),
                                    task_queue: crate::TaskQueue::new("sqlite-cleanup-activities"),
                                    retry_policy: crate::RetryPolicy::none(),
                                    start_to_close_timeout: None,
                                    heartbeat_timeout: None,
                                    input_manifest: input_manifest.clone(),
                                    result_manifest_name: "cleanup-results".to_owned(),
                                    max_in_flight: 2,
                                    fingerprint: crate::activity_map_fingerprint(
                                        crate::ActivityName::new("tests.echo"),
                                        crate::payload_digest(&input_manifest),
                                        "cleanup-results".to_owned(),
                                        2,
                                        "sha256:test-options".to_owned(),
                                    ),
                                },
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::SignalConsumed(
                                crate::SignalConsumed {
                                    command_id: signal_command,
                                    signal_id: crate::SignalId::new("signal/cleanup/consumed"),
                                    signal_name: crate::SignalName::new("go"),
                                    payload: crate::encode_payload(&"x").unwrap(),
                                    fingerprint: crate::signal_fingerprint(crate::SignalName::new(
                                        "go",
                                    )),
                                },
                            )),
                        ],
                        schedule_activities: vec![ActivityTask::from_scheduled(&scheduled)],
                        schedule_activity_maps: vec![map_task],
                        consume_signals: vec![crate::SignalId::new("signal/cleanup/consumed")],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            let activity = backend
                .claim_activity_task(
                    WorkerId::new("sqlite-cleanup-activity-worker"),
                    crate::ClaimActivityOptions {
                        namespace: namespace.clone(),
                        task_queue: crate::TaskQueue::new("sqlite-cleanup-activities"),
                        registered_activity_names: vec![crate::ActivityName::new("tests.echo")],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("activity task");

            backend
                .cancel_workflow(crate::CancelWorkflowRequest {
                    namespace: namespace.clone(),
                    workflow_id: WorkflowId::new("wf/sqlite-terminal-cleanup"),
                    reason: "cleanup test".to_owned(),
                })
                .await
                .unwrap();
            drop(backend);

            // Reopen: the deletions are durable, history is untouched, and
            // only the undelivered signal survives as an inbox row.
            let reopened = SqliteBackend::open(&path).unwrap();
            let conn = reopened.connection().unwrap();
            for (table, expected) in [
                ("activity_tasks", 0_i64),
                ("activity_maps", 0),
                ("activity_map_results", 0),
                ("active_waits", 0),
            ] {
                let count: i64 = conn
                    .query_row(&format!("select count(*) from {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, expected, "terminal cleanup should empty `{table}`");
            }
            let (signal_count, unconsumed_count): (i64, i64) = conn
                .query_row(
                    "select count(*), sum(case when consumed = 0 then 1 else 0 end)
                     from signals where run_id = ?1",
                    params![run_id.0],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(
                (signal_count, unconsumed_count),
                (1, 1),
                "only the undelivered signal survives cleanup"
            );
            let history_count: i64 = conn
                .query_row(
                    "select count(*) from history_events where run_id = ?1",
                    params![run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(history_count, 5, "history stays authoritative");
            drop(conn);

            // Late calls answer from row absence after the reopen.
            let late = reopened
                .complete_activity(crate::CompleteActivityRequest {
                    claim: activity.claim,
                    result: crate::encode_payload(&1_u64).unwrap(),
                })
                .await
                .unwrap();
            assert_eq!(late, crate::CompleteActivityOutcome::AlreadyCompleted);
        });
    }

    #[test]
    fn terminal_run_with_live_claim_rejects_every_mutating_commit_kind() {
        use crate::provider_util::commit_test_support;
        use futures::executor::block_on;

        block_on(async {
            // Every terminal transition clears the workflow claim, so the
            // guard is defense-in-depth: forge the terminal flag while a valid
            // claim and matching tail survive, then require each mutation kind
            // to be rejected (SPEC: "terminal workflow rejects new
            // workflow-visible commands").
            let dir = tempfile::tempdir().unwrap();
            let backend = SqliteBackend::open(dir.path().join("terminal-guard.sqlite3")).unwrap();
            let workflow_type = WorkflowType::new("tests.sqlite-terminal-guard", 1);
            backend
                .start_workflow(crate::StartWorkflowRequest {
                    namespace: crate::Namespace::default(),
                    workflow_id: WorkflowId::new("wf/sqlite-terminal-guard"),
                    workflow_type: workflow_type.clone(),
                    task_queue: crate::TaskQueue::new("sqlite-terminal-guard"),
                    input: crate::encode_payload(&0_u64).unwrap(),
                })
                .await
                .unwrap();
            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("sqlite-terminal-guard"),
                    ClaimWorkflowTaskOptions {
                        namespace: crate::Namespace::default(),
                        task_queue: crate::TaskQueue::new("sqlite-terminal-guard"),
                        registered_workflow_types: vec![workflow_type],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("claimable workflow task");
            backend
                .connection()
                .unwrap()
                .execute(
                    "update workflow_instances set terminal = 1 where run_id = ?1",
                    params![claimed.run_id.0],
                )
                .unwrap();

            for (kind, commit) in commit_test_support::mutating_commits(&claimed.run_id, EventId(1))
            {
                let err = backend
                    .commit_workflow_task(claimed.claim.clone(), commit)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::TerminalWorkflow),
                    "commit kind `{kind}` should be rejected as TerminalWorkflow, got {err:?}"
                );
            }
            // The rejection must not consume the claim, and a fully empty
            // commit stays an accepted no-op against the terminal run.
            let outcome = backend
                .commit_workflow_task(
                    claimed.claim.clone(),
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                outcome,
                CommitOutcome::Committed {
                    new_tail_event_id: EventId(1)
                }
            );
        });
    }

    #[test]
    fn sqlite_reopen_recreates_legacy_ready_index_and_claims_ready_workflows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-ready-index.sqlite3");
        {
            let backend = SqliteBackend::open(&path).unwrap();
            futures::executor::block_on(backend.start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: WorkflowId::new("wf/legacy-index"),
                workflow_type: WorkflowType::new("tests.legacy-index", 1),
                task_queue: crate::TaskQueue::new("legacy-index-workflows"),
                input: crate::encode_payload(&0_u64).unwrap(),
            }))
            .unwrap();
        }
        // Simulate a database migrated from before lease-based reclaim: its
        // ready index still filters on the claim token, which the lease-aware
        // claim query no longer implies.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "drop index if exists idx_workflow_instances_ready;
                 create index idx_workflow_instances_ready
                     on workflow_instances(namespace, task_queue, ready_at_ms, run_id)
                     where ready_reason is not null
                       and workflow_claim_token is null
                       and terminal = 0;",
            )
            .unwrap();
        }

        let reopened = SqliteBackend::open(&path).unwrap();
        {
            let conn = reopened.connection().unwrap();
            let sql = conn
                .query_row(
                    "select sql from sqlite_master
                     where type = 'index' and name = 'idx_workflow_instances_ready'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap();
            assert!(
                !sql.contains("workflow_claim_token"),
                "legacy ready-index predicate survived reopen: {sql}"
            );
        }
        let claimed = futures::executor::block_on(reopened.claim_workflow_task(
            WorkerId::new("legacy-index-worker"),
            ClaimWorkflowTaskOptions {
                namespace: crate::Namespace::default(),
                task_queue: crate::TaskQueue::new("legacy-index-workflows"),
                registered_workflow_types: vec![WorkflowType::new("tests.legacy-index", 1)],
                lease_duration: Duration::from_secs(30),
            },
        ))
        .unwrap();
        assert!(
            claimed.is_some(),
            "migrated database should still claim ready workflows"
        );
    }
}
