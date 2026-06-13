use crate::{
    ActivityId, ActivityTask, ActivityTaskClaim, ClaimActivityOptions, ClaimWorkflowTaskOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommitOutcome, CompleteActivityOutcome,
    CompleteActivityRequest, DurableBackend, Error, EventId, HistoryChunk, HistoryEvent,
    HistoryEventData, HistoryEventType, Result, RunId, StartWorkflowOutcome, StartWorkflowRequest,
    WorkerId, WorkflowId, WorkflowTaskClaim, WorkflowTaskCommit, WorkflowTaskReason, WorkflowType,
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
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal)
                 values (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, 0, null, 0)",
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
                     where namespace = ?1 and task_queue = ?2 and ready_reason is not null and ready_at_ms <= ?3 and terminal = 0
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
            let Some((current_tail, claim_token, terminal, namespace)) = tx
                .query_row(
                    "select current_event_id, workflow_claim_token, terminal, namespace
                     from workflow_instances where run_id = ?1",
                    params![claim.run_id.0],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, Option<u64>>(1)?,
                            row.get::<_, bool>(2)?,
                            row.get::<_, String>(3)?,
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
            for event in batch.append_events {
                next_event_id = next_event_id.next();
                became_terminal |= is_terminal(&event.data);
                insert_history_event(&tx, &claim.run_id, next_event_id, event.data)?;
            }
            for task in batch.schedule_activities {
                let task_blob = rmp_serde::to_vec_named(&task)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?;
                tx.execute(
                    "insert into activity_tasks
                     (activity_id, namespace, run_id, activity_name, task_queue, task, claim_token, completed)
                     values (?1, ?2, ?3, ?4, ?5, ?6, null, 0)",
                    params![
                        task.activity_id.0,
                        namespace,
                        task.run_id.0,
                        task.activity_name.0,
                        task.task_queue.0,
                        task_blob
                    ],
                )
                .map_err(sqlite_error)?;
            }
            tx.execute(
                "update workflow_instances
                 set current_event_id = ?1,
                     workflow_claim_token = null,
                     terminal = ?2,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = ?3",
                params![next_event_id.0, became_terminal || terminal, claim.run_id.0],
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

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let result = (|| {
            let mut conn = self.connect()?;
            let tx = conn.transaction().map_err(sqlite_error)?;
            let mut stmt = tx
                .prepare(
                    "select activity_id, activity_name, task
                     from activity_tasks
                     where namespace = ?1 and task_queue = ?2 and completed = 0 and claim_token is null
                     order by rowid asc",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(params![opts.namespace.0, opts.task_queue.0], |row| {
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
                "update activity_tasks set claim_token = ?1 where activity_id = ?2",
                params![token, activity_id.0],
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
                "update activity_tasks set completed = 1 where activity_id = ?1",
                params![req.claim.activity_id.0],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            Ok(CompleteActivityOutcome::Completed { event_id })
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
            completed integer not null
        );
        ",
    )
    .map_err(sqlite_error)?;
    ensure_column(
        conn,
        "workflow_instances",
        "ready_at_ms",
        "integer not null default 0",
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
    let data =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        "insert into history_events(run_id, event_id, event_type, data)
         values (?1, ?2, ?3, ?4)",
        params![run_id.0, event_id.0, event_type, data],
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
        WorkflowTaskReason::CacheEvicted => "cache_evicted",
    }
}

fn reason_from_str(value: &str) -> Result<WorkflowTaskReason> {
    match value {
        "workflow_started" => Ok(WorkflowTaskReason::WorkflowStarted),
        "activity_completed" => Ok(WorkflowTaskReason::ActivityCompleted),
        "cache_evicted" => Ok(WorkflowTaskReason::CacheEvicted),
        other => Err(Error::Backend(format!(
            "unknown workflow task reason `{other}`"
        ))),
    }
}

fn event_type_to_str(event_type: &HistoryEventType) -> &'static str {
    match event_type {
        HistoryEventType::WorkflowStarted => "workflow_started",
        HistoryEventType::WorkflowCompleted => "workflow_completed",
        HistoryEventType::WorkflowFailed => "workflow_failed",
        HistoryEventType::WorkflowTaskStarted => "workflow_task_started",
        HistoryEventType::ActivityScheduled => "activity_scheduled",
        HistoryEventType::ActivityCompleted => "activity_completed",
    }
}

fn event_type_from_str(value: &str) -> Result<HistoryEventType> {
    match value {
        "workflow_started" => Ok(HistoryEventType::WorkflowStarted),
        "workflow_completed" => Ok(HistoryEventType::WorkflowCompleted),
        "workflow_failed" => Ok(HistoryEventType::WorkflowFailed),
        "workflow_task_started" => Ok(HistoryEventType::WorkflowTaskStarted),
        "activity_scheduled" => Ok(HistoryEventType::ActivityScheduled),
        "activity_completed" => Ok(HistoryEventType::ActivityCompleted),
        other => Err(Error::Backend(format!("unknown event type `{other}`"))),
    }
}
