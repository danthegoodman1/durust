use crate::{
    ActivityFailed, ActivityId, ActivityMapInputManifest, ActivityMapItem, ActivityMapTask,
    ActivityTask, ActivityTaskClaim, CancelWorkflowOutcome, CancelWorkflowRequest,
    ChildStartOutboxMessage, ClaimActivityOptions, ClaimWorkflowTaskOptions, ClaimedActivityTask,
    ClaimedWorkflowTask, CommandId, CommandSeq, CommitOutcome, CompleteActivityOutcome,
    CompleteActivityRequest, DispatchChildWorkflowStartsOutcome,
    DispatchChildWorkflowStartsRequest, DurableBackend, Error, EventId, FailActivityOutcome,
    FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest, HistoryChunk, HistoryEvent,
    HistoryEventData, HistoryEventType, ParentClosePolicy, PayloadRef, ReadSignalInboxRequest,
    Result, RunId, SignalInboxRecord, SignalWorkflowOutcome, SignalWorkflowRequest,
    StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, TimestampMs, WaitKind, WorkerId, WorkflowChangeMarkerKind,
    WorkflowChangeVersionRecord, WorkflowChangeVersionStatus, WorkflowChangeVersionsOutcome,
    WorkflowChangeVersionsRequest, WorkflowId, WorkflowTaskClaim, WorkflowTaskCommit,
    WorkflowTaskReason, WorkflowType, activity_map_input_at, encode_activity_map_result_manifest,
    event_payload_len, is_terminal,
};
use futures::future::{BoxFuture, ready};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct SqliteBackend {
    path: PathBuf,
}

impl SqliteBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let backend = Self {
            path: path.as_ref().to_path_buf(),
        };
        let conn = backend.connect()?;
        init_schema(&conn)?;
        Ok(backend)
    }

    fn connect(&self) -> Result<Connection> {
        Connection::open(&self.path).map_err(sqlite_error)
    }
}

impl DurableBackend for SqliteBackend {
    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>> {
        let result = (|| {
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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

            let run_id = RunId::new(format!("run-{}", next_counter(&tx, "run")?));
            let start = HistoryEventData::WorkflowStarted {
                workflow_type: req.workflow_type.clone(),
                input: req.input,
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            cleanup_run_operational_state(&tx, &run_id)?;
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
            handle_terminal_run(&tx, &run_id, &terminal_event)?;
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
            let now_ms = unix_epoch_millis();
            let mut stmt = tx
                .prepare(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason
                     from workflow_instances
                     where namespace = ?1
                       and task_queue = ?2
                       and ready_reason is not null
                       and ready_at_ms <= ?3
                       and workflow_claim_token is null
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
            let token = next_counter(&tx, "claim")?;
            tx.execute(
                "update workflow_instances
                 set workflow_claim_token = ?1, ready_reason = null, ready_at_ms = 0
                 where run_id = ?2",
                params![token, run_id.0],
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
            }))
        })();
        Box::pin(ready(result))
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let result = (|| {
            let conn = self.connect()?;
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

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>> {
        let result = (|| {
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            if terminal && !batch.append_events.is_empty() {
                return Err(Error::TerminalWorkflow);
            }

            let mut next_event_id = EventId(current_tail);
            let mut became_terminal = false;
            let mut terminal_event = None;
            for event in batch.append_events {
                next_event_id = next_event_id.next();
                became_terminal |= is_terminal(&event.data);
                if is_terminal(&event.data) {
                    terminal_event = Some(event.data.clone());
                }
                insert_history_event(&tx, &claim.run_id, next_event_id, event.data)?;
            }
            for task in batch.schedule_activities {
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
            for map_task in batch.schedule_activity_maps {
                insert_activity_map(&tx, namespace.as_str(), &map_task)?;
                materialize_activity_map_items(&tx, &map_task.map_command_id)?;
            }
            for message in batch.start_child_workflows {
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
            if let Some(payload) = batch.query_projection {
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
                cleanup_run_operational_state(&tx, &claim.run_id)?;
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
                    handle_terminal_run(&tx, &claim.run_id, &event)?;
                }
            }
            let ready_reason = if !terminal_after_commit && signal_wait_ready(&tx, &claim.run_id)? {
                Some(reason_to_str(&WorkflowTaskReason::SignalReceived))
            } else {
                None
            };
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            let payload = rmp_serde::to_vec_named(&req.payload)
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
            let conn = self.connect()?;
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
            tx.execute(
                "update activity_tasks
                 set claim_token = ?1, heartbeat_deadline_at_ms = ?2
                 where activity_id = ?3",
                params![
                    token,
                    activity_timeout_at_ms_from(now, task.heartbeat_timeout),
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
                return Err(Error::Backend(format!(
                    "activity `{}` not found",
                    req.claim.activity_id.0
                )));
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
                return Err(Error::Backend(format!(
                    "activity `{}` not found",
                    req.claim.activity_id.0
                )));
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
            if let Some(map_item) = task.map_item.clone() {
                let outcome = complete_map_item(
                    &tx,
                    task,
                    map_item,
                    req.result,
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
                    result: req.result,
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
                return Err(Error::Backend(format!(
                    "activity `{}` not found",
                    req.claim.activity_id.0
                )));
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
            if should_retry_activity(&task) && !req.failure.non_retryable {
                let mut retry_task = task.clone();
                retry_task.attempt = retry_task.attempt.saturating_add(1);
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
                return Ok(FailActivityOutcome::RetryScheduled {
                    next_attempt: retry_task.attempt,
                });
            }
            if let Some(map_item) = task.map_item.clone() {
                let outcome = fail_map_item(
                    &tx,
                    task,
                    map_item,
                    req.failure,
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
                HistoryEventData::ActivityFailed(ActivityFailed {
                    command_id: task.command_id,
                    failure: req.failure,
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
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
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
                dispatch_child_start(&tx, &outbox_id)?;
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
            let conn = self.connect()?;
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
            let conn = self.connect()?;
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
            terminal integer not null,
            parent_run_id text,
            parent_command_seq integer,
            parent_close_policy text,
            unique(namespace, workflow_id)
        );

        create table if not exists history_events (
            run_id text not null,
            event_id integer not null,
            event_type text not null,
            data blob not null,
            primary key(run_id, event_id)
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
        "child_outbox",
        "parent_close_policy",
        "text not null default 'cancel'",
    )?;
    ensure_column(conn, "activity_tasks", "timeout_at_ms", "integer")?;
    ensure_column(
        conn,
        "activity_tasks",
        "heartbeat_deadline_at_ms",
        "integer",
    )
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info({table})"))
        .map_err(sqlite_error)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sqlite_error)?;
    for existing in columns {
        if existing.map_err(sqlite_error)? == column {
            return Ok(());
        }
    }
    conn.execute(
        &format!("alter table {table} add column {column} {definition}"),
        [],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn ready_at_ms_for_delay(delay: Duration) -> i64 {
    if delay.is_zero() {
        0
    } else {
        unix_epoch_millis().saturating_add(duration_millis_i64(delay))
    }
}

fn activity_timeout_at_ms(timeout: Option<Duration>) -> Option<i64> {
    activity_timeout_at_ms_from(TimestampMs(unix_epoch_millis()), timeout)
}

fn activity_timeout_at_ms_from(now: TimestampMs, timeout: Option<Duration>) -> Option<i64> {
    timeout.map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
}

fn timeout_message(activity_id: &ActivityId, attempt: u32, heartbeat: bool) -> String {
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

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn unix_epoch_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
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

fn cleanup_run_operational_state(tx: &Transaction<'_>, run_id: &RunId) -> Result<()> {
    tx.execute(
        "delete from active_waits where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update activity_tasks
         set completed = 1,
             claim_token = null,
             heartbeat_deadline_at_ms = null
         where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "update activity_maps
         set completed = 1, in_flight = 0
         where run_id = ?1",
        params![run_id.0],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn handle_terminal_run(
    tx: &Transaction<'_>,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    notify_parent_of_child_terminal(tx, run_id, terminal_event)?;
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
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let Some((parent_run_id, parent_command_seq)) = tx
        .query_row(
            "select parent_run_id, parent_command_seq
             from workflow_instances
             where run_id = ?1",
            params![child_run_id.0],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<u64>>(1)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
        .and_then(|(run_id, seq)| Some((RunId::new(run_id?), CommandSeq(seq?))))
    else {
        return Ok(());
    };
    let command_id = CommandId {
        run_id: parent_run_id.clone(),
        seq: parent_command_seq,
    };
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
    let (data, reason) = match terminal_event {
        HistoryEventData::WorkflowCompleted { result } => (
            HistoryEventData::ChildWorkflowCompleted(crate::ChildWorkflowCompleted {
                command_id,
                result: result.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCompleted,
        ),
        HistoryEventData::WorkflowFailed { failure } => (
            HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                command_id,
                failure: failure.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowFailed,
        ),
        HistoryEventData::WorkflowCancelled { reason } => (
            HistoryEventData::ChildWorkflowCancelled(crate::ChildWorkflowCancelled {
                command_id,
                reason: reason.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCancelled,
        ),
        _ => return Ok(()),
    };
    insert_history_event(tx, &parent_run_id, event_id, data)?;
    set_workflow_ready(tx, &parent_run_id, event_id, reason)
}

fn child_terminal_event_exists(tx: &Transaction<'_>, command_id: &CommandId) -> Result<bool> {
    let mut stmt = tx
        .prepare(
            "select data from history_events
             where run_id = ?1
               and event_type in (
                 'child_workflow_completed',
                 'child_workflow_failed',
                 'child_workflow_cancelled'
               )",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map(params![command_id.run_id.0], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let event: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let matches = match event {
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        };
        if matches {
            return Ok(true);
        }
    }
    Ok(false)
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
        cleanup_run_operational_state(tx, &child_run_id)?;
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
        "update child_outbox
         set dispatched = 1
         where outbox_id = ?1",
        params![child_outbox_id(command_id)],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn child_outbox_id(command_id: &CommandId) -> String {
    format!("{}:{}:child-start", command_id.run_id, command_id.seq.0)
}

fn parent_close_policy_to_str(policy: ParentClosePolicy) -> &'static str {
    match policy {
        ParentClosePolicy::Cancel => "cancel",
        ParentClosePolicy::Abandon => "abandon",
    }
}

fn insert_activity_map(
    tx: &Transaction<'_>,
    namespace: &str,
    map_task: &ActivityMapTask,
) -> Result<()> {
    let manifest: ActivityMapInputManifest = crate::decode_payload(&map_task.input_manifest)?;
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
            child_outbox_id(&message.command_id),
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

fn dispatch_child_start(tx: &Transaction<'_>, outbox_id: &str) -> Result<()> {
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
            "select run_id, parent_run_id, parent_command_seq
             from workflow_instances
             where namespace = ?1 and workflow_id = ?2",
            params![namespace.as_str(), message.workflow_id.0],
            |row| {
                Ok((
                    RunId::new(row.get::<_, String>(0)?),
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<u64>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;

    let child_run_id = if let Some((run_id, parent_run_id, parent_seq)) = existing {
        let same_child = parent_run_id.as_deref() == Some(message.command_id.run_id.0.as_str())
            && parent_seq == Some(message.command_id.seq.0);
        if !same_child {
            append_child_start_failed(
                tx,
                &message.command_id,
                crate::DurableFailure::non_retryable(
                    "durust.child_workflow_id_conflict",
                    format!("workflow id `{}` is already started", message.workflow_id),
                ),
            )?;
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
    tx.execute(
        "insert into workflow_instances
         (namespace, workflow_id, run_id, workflow_name, workflow_version, task_queue,
          current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
          parent_run_id, parent_command_seq, parent_close_policy)
         values (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, 0, null, 0, ?8, ?9, ?10)",
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
            parent_close_policy_to_str(message.parent_close_policy)
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
    let mut stmt = tx
        .prepare(
            "select data from history_events
             where run_id = ?1
               and event_type in (
                 'child_workflow_started',
                 'child_workflow_completed',
                 'child_workflow_failed',
                 'child_workflow_cancelled'
               )",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map(params![command_id.run_id.0], |row| row.get::<_, Vec<u8>>(0))
        .map_err(sqlite_error)?;
    for row in rows {
        let blob = row.map_err(sqlite_error)?;
        let event: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let matches = match event {
            HistoryEventData::ChildWorkflowStarted(started) => started.command_id == *command_id,
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        };
        if matches {
            return Ok(true);
        }
    }
    Ok(false)
}

fn materialize_activity_map_items(tx: &Transaction<'_>, map_command_id: &CommandId) -> Result<()> {
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
    let manifest: ActivityMapInputManifest = crate::decode_payload(&task.input_manifest)?;

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

fn complete_map_item(
    tx: &Transaction<'_>,
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
        materialize_activity_map_items(tx, &map_item.map_command_id)?;
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
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&map_task.input_manifest)?;
    let result_refs = activity_map_results(tx, key.as_str())?;
    let result_manifest = encode_activity_map_result_manifest(
        map_task.result_manifest_name,
        result_refs,
        &input_manifest.page_lengths,
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
    let timed_out_by_heartbeat = heartbeat_timeout_due && !start_timeout_due;

    let task: ActivityTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    if should_retry_activity(&task) {
        let mut retry_task = task.clone();
        retry_task.attempt = retry_task.attempt.saturating_add(1);
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

fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
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
    let blob =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into history_events(run_id, event_id, event_type, data)
         values (?1, ?2, ?3, ?4)",
        params![run_id.0, event_id.0, event_type, blob],
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

fn reason_to_str(reason: &WorkflowTaskReason) -> &'static str {
    match reason {
        WorkflowTaskReason::WorkflowStarted => "workflow_started",
        WorkflowTaskReason::ActivityCompleted => "activity_completed",
        WorkflowTaskReason::ActivityFailed => "activity_failed",
        WorkflowTaskReason::ActivityTimedOut => "activity_timed_out",
        WorkflowTaskReason::ActivityMapCompleted => "activity_map_completed",
        WorkflowTaskReason::ActivityMapFailed => "activity_map_failed",
        WorkflowTaskReason::ChildWorkflowStarted => "child_workflow_started",
        WorkflowTaskReason::ChildWorkflowCompleted => "child_workflow_completed",
        WorkflowTaskReason::ChildWorkflowFailed => "child_workflow_failed",
        WorkflowTaskReason::ChildWorkflowCancelled => "child_workflow_cancelled",
        WorkflowTaskReason::TimerFired => "timer_fired",
        WorkflowTaskReason::SignalReceived => "signal_received",
        WorkflowTaskReason::CacheEvicted => "cache_evicted",
    }
}

fn reason_from_str(value: &str) -> Result<WorkflowTaskReason> {
    match value {
        "workflow_started" => Ok(WorkflowTaskReason::WorkflowStarted),
        "activity_completed" => Ok(WorkflowTaskReason::ActivityCompleted),
        "activity_failed" => Ok(WorkflowTaskReason::ActivityFailed),
        "activity_timed_out" => Ok(WorkflowTaskReason::ActivityTimedOut),
        "activity_map_completed" => Ok(WorkflowTaskReason::ActivityMapCompleted),
        "activity_map_failed" => Ok(WorkflowTaskReason::ActivityMapFailed),
        "child_workflow_started" => Ok(WorkflowTaskReason::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(WorkflowTaskReason::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(WorkflowTaskReason::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(WorkflowTaskReason::ChildWorkflowCancelled),
        "timer_fired" => Ok(WorkflowTaskReason::TimerFired),
        "signal_received" => Ok(WorkflowTaskReason::SignalReceived),
        "cache_evicted" => Ok(WorkflowTaskReason::CacheEvicted),
        other => Err(Error::Backend(format!(
            "unknown workflow task reason `{other}`"
        ))),
    }
}

fn marker_kind_to_str(kind: WorkflowChangeMarkerKind) -> &'static str {
    match kind {
        WorkflowChangeMarkerKind::Version => "version",
        WorkflowChangeMarkerKind::DeprecatedPatch => "deprecated_patch",
    }
}

fn marker_kind_from_str(value: &str) -> Result<WorkflowChangeMarkerKind> {
    match value {
        "version" => Ok(WorkflowChangeMarkerKind::Version),
        "deprecated_patch" => Ok(WorkflowChangeMarkerKind::DeprecatedPatch),
        other => Err(Error::Backend(format!(
            "unknown workflow change marker kind `{other}`"
        ))),
    }
}

fn event_type_to_str(event_type: &HistoryEventType) -> &'static str {
    match event_type {
        HistoryEventType::WorkflowStarted => "workflow_started",
        HistoryEventType::WorkflowCompleted => "workflow_completed",
        HistoryEventType::WorkflowFailed => "workflow_failed",
        HistoryEventType::WorkflowCancelled => "workflow_cancelled",
        HistoryEventType::WorkflowContinuedAsNew => "workflow_continued_as_new",
        HistoryEventType::WorkflowTaskStarted => "workflow_task_started",
        HistoryEventType::ActivityScheduled => "activity_scheduled",
        HistoryEventType::ActivityMapScheduled => "activity_map_scheduled",
        HistoryEventType::ActivityMapCompleted => "activity_map_completed",
        HistoryEventType::ActivityMapFailed => "activity_map_failed",
        HistoryEventType::ActivityCompleted => "activity_completed",
        HistoryEventType::ActivityFailed => "activity_failed",
        HistoryEventType::ActivityTimedOut => "activity_timed_out",
        HistoryEventType::ChildWorkflowStartRequested => "child_workflow_start_requested",
        HistoryEventType::ChildWorkflowStarted => "child_workflow_started",
        HistoryEventType::ChildWorkflowCompleted => "child_workflow_completed",
        HistoryEventType::ChildWorkflowFailed => "child_workflow_failed",
        HistoryEventType::ChildWorkflowCancelled => "child_workflow_cancelled",
        HistoryEventType::TimerStarted => "timer_started",
        HistoryEventType::TimerFired => "timer_fired",
        HistoryEventType::SignalConsumed => "signal_consumed",
        HistoryEventType::SelectWinner => "select_winner",
        HistoryEventType::VersionMarker => "version_marker",
        HistoryEventType::DeprecatedPatchMarker => "deprecated_patch_marker",
    }
}

fn event_type_from_str(value: &str) -> Result<HistoryEventType> {
    match value {
        "workflow_started" => Ok(HistoryEventType::WorkflowStarted),
        "workflow_completed" => Ok(HistoryEventType::WorkflowCompleted),
        "workflow_failed" => Ok(HistoryEventType::WorkflowFailed),
        "workflow_cancelled" => Ok(HistoryEventType::WorkflowCancelled),
        "workflow_continued_as_new" => Ok(HistoryEventType::WorkflowContinuedAsNew),
        "workflow_task_started" => Ok(HistoryEventType::WorkflowTaskStarted),
        "activity_scheduled" => Ok(HistoryEventType::ActivityScheduled),
        "activity_map_scheduled" => Ok(HistoryEventType::ActivityMapScheduled),
        "activity_map_completed" => Ok(HistoryEventType::ActivityMapCompleted),
        "activity_map_failed" => Ok(HistoryEventType::ActivityMapFailed),
        "activity_completed" => Ok(HistoryEventType::ActivityCompleted),
        "activity_failed" => Ok(HistoryEventType::ActivityFailed),
        "activity_timed_out" => Ok(HistoryEventType::ActivityTimedOut),
        "child_workflow_start_requested" => Ok(HistoryEventType::ChildWorkflowStartRequested),
        "child_workflow_started" => Ok(HistoryEventType::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(HistoryEventType::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(HistoryEventType::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(HistoryEventType::ChildWorkflowCancelled),
        "timer_started" => Ok(HistoryEventType::TimerStarted),
        "timer_fired" => Ok(HistoryEventType::TimerFired),
        "signal_consumed" => Ok(HistoryEventType::SignalConsumed),
        "select_winner" => Ok(HistoryEventType::SelectWinner),
        "version_marker" => Ok(HistoryEventType::VersionMarker),
        "deprecated_patch_marker" => Ok(HistoryEventType::DeprecatedPatchMarker),
        other => Err(Error::Backend(format!("unknown event type `{other}`"))),
    }
}

fn wait_kind_to_str(kind: &WaitKind) -> &'static str {
    match kind {
        WaitKind::Timer => "timer",
        WaitKind::Signal => "signal",
    }
}
