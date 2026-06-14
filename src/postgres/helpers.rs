use super::*;
use tokio_postgres::Transaction;

pub(super) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(Error::Backend(
            "postgres schema identifier must not be empty".to_owned(),
        ));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::Backend(format!(
            "postgres schema identifier `{identifier}` must start with an ASCII letter or underscore"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(Error::Backend(format!(
            "postgres schema identifier `{identifier}` must contain only ASCII letters, digits, or underscores"
        )));
    }
    Ok(())
}

pub(super) fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn postgres_error(err: tokio_postgres::Error) -> Error {
    Error::Backend(format!("postgres error: {err}"))
}

pub(super) fn ready_at_ms_for_delay(delay: Duration) -> i64 {
    if delay.is_zero() {
        0
    } else {
        unix_epoch_millis().saturating_add(duration_millis_i64(delay))
    }
}

pub(super) fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

pub(super) fn activity_timeout_at_ms(timeout: Option<Duration>) -> Option<i64> {
    timeout.map(|timeout| unix_epoch_millis().saturating_add(duration_millis_i64(timeout)))
}

pub(super) fn activity_timeout_at_ms_from(
    now: TimestampMs,
    timeout: Option<Duration>,
) -> Option<i64> {
    timeout.map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
}

pub(super) fn timeout_message(activity_id: &ActivityId, attempt: u32, heartbeat: bool) -> String {
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

pub(super) fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
}

pub(super) fn wait_kind_to_str(kind: &WaitKind) -> &'static str {
    match kind {
        WaitKind::Timer => "timer",
        WaitKind::Signal => "signal",
    }
}

pub(super) async fn signal_wait_ready(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
) -> Result<bool> {
    Ok(tx
        .query_opt(
            &format!(
                "select 1
                 from {schema}.active_waits w
                 join {schema}.signals s on s.run_id = w.run_id
                    and s.signal_name = w.wait_key
                    and s.consumed = false
                 where w.run_id = $1 and w.kind = $2
                 limit 1"
            ),
            &[&run_id.0, &wait_kind_to_str(&WaitKind::Signal)],
        )
        .await
        .map_err(postgres_error)?
        .is_some())
}

pub(super) async fn cleanup_run_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
) -> Result<()> {
    tx.execute(
        &format!("delete from {schema}.active_waits where run_id = $1"),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where run_id = $1"
        ),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where run_id = $1"
        ),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) async fn handle_terminal_run_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    notify_parent_of_child_terminal_tx(tx, schema, run_id, terminal_event).await?;
    cancel_children_for_parent_tx(tx, schema, run_id).await?;
    Ok(())
}

pub(super) async fn continue_run_as_new_tx(
    tx: &Transaction<'_>,
    schema: &str,
    old_run_id: &RunId,
    event: HistoryEventData,
) -> Result<()> {
    let HistoryEventData::WorkflowContinuedAsNew { input } = event else {
        return Ok(());
    };
    let Some(row) = tx
        .query_opt(
            &format!(
                "select workflow_name, workflow_version
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&old_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(old_run_id.clone()));
    };
    let workflow_type = WorkflowType::new(
        row.get::<_, String>(0),
        u32::try_from(row.get::<_, i32>(1)).unwrap_or(0),
    );
    let new_run_id = RunId::new(format!("run-{}", next_counter(tx, schema, "run").await?));
    insert_history_event(
        tx,
        schema,
        &new_run_id,
        EventId(1),
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        },
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set run_id = $1,
                 current_event_id = 1,
                 ready_reason = $2,
                 ready_at_ms = 0,
                 workflow_claim_token = null,
                 terminal = false
             where run_id = $3"
        ),
        &[
            &new_run_id.0,
            &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
            &old_run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) async fn notify_parent_of_child_terminal_tx(
    tx: &Transaction<'_>,
    schema: &str,
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let Some(row) = tx
        .query_opt(
            &format!(
                "select parent_run_id, parent_command_seq
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&child_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(child_run_id.clone()));
    };
    let parent_run_id: Option<String> = row.get(0);
    let parent_command_seq: Option<i64> = row.get(1);
    let Some((parent_run_id, parent_command_seq)) = parent_run_id
        .zip(parent_command_seq)
        .and_then(|(run_id, seq)| Some((RunId::new(run_id), CommandSeq(u64::try_from(seq).ok()?))))
    else {
        return Ok(());
    };
    let command_id = CommandId {
        run_id: parent_run_id.clone(),
        seq: parent_command_seq,
    };
    if child_terminal_event_exists_tx(tx, schema, &command_id).await? {
        return Ok(());
    }
    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&parent_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(());
    };
    let parent_tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let parent_terminal: bool = row.get(1);
    if parent_terminal {
        return Ok(());
    }
    let event_id = parent_tail.next();
    let (event_data, reason) = match terminal_event {
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
    insert_history_event(tx, schema, &parent_run_id, event_id, event_data).await?;
    set_workflow_ready_tx(tx, schema, &parent_run_id, event_id, reason).await
}

pub(super) async fn child_terminal_event_exists_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<bool> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    let rows = tx
        .query(
            &format!(
                "select data
                 from {schema}.history_events
                 where run_id = $1
                   and event_type = any($2::text[])"
            ),
            &[&command_id.run_id.0, &child_event_types],
        )
        .await
        .map_err(postgres_error)?;
    for row in rows {
        let blob: Vec<u8> = row.get(0);
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

pub(super) async fn cancel_children_for_parent_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_id: &RunId,
) -> Result<()> {
    let rows = tx
        .query(
            &format!(
                "select run_id, current_event_id
                 from {schema}.workflow_instances
                 where parent_run_id = $1
                   and parent_close_policy = $2
                   and terminal = false
                 order by run_id asc
                 for update"
            ),
            &[
                &parent_run_id.0,
                &parent_close_policy_to_str(ParentClosePolicy::Cancel),
            ],
        )
        .await
        .map_err(postgres_error)?;
    let children = rows
        .into_iter()
        .map(|row| {
            (
                RunId::new(row.get::<_, String>(0)),
                EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX)),
            )
        })
        .collect::<Vec<_>>();
    for (child_run_id, tail) in children {
        let event_id = tail.next();
        insert_history_event(
            tx,
            schema,
            &child_run_id,
            event_id,
            HistoryEventData::WorkflowCancelled {
                reason: format!("parent workflow `{parent_run_id}` closed"),
            },
        )
        .await?;
        cleanup_run_operational_state_tx(tx, schema, &child_run_id).await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1,
                     workflow_claim_token = null,
                     terminal = true,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = $2"
            ),
            &[
                &i64::try_from(event_id.0).unwrap_or(i64::MAX),
                &child_run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
    }
    Ok(())
}

pub(super) async fn cancel_command_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<()> {
    let activity_id = ActivityId::new(command_id);
    let map_prefix = format!("{}:map:%", activity_id.0);
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1 or activity_id like $2"
        ),
        &[&activity_id.0, &map_prefix],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&map_command_key(command_id)],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) async fn set_workflow_ready_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    reason: WorkflowTaskReason,
) -> Result<()> {
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
             where run_id = $3"
        ),
        &[
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &reason_to_str(&reason),
            &run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) fn unix_epoch_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

pub(super) async fn next_counter(tx: &Transaction<'_>, schema: &str, key: &str) -> Result<u64> {
    let row = tx
        .query_one(
            &format!(
                "insert into {schema}.meta(key, value)
                 values ($1, 1)
                 on conflict(key) do update set value = {schema}.meta.value + 1
                 returning value"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?;
    let value: i64 = row.get(0);
    u64::try_from(value).map_err(|_| {
        Error::Backend(format!(
            "postgres counter `{key}` returned invalid value {value}"
        ))
    })
}

pub(super) async fn insert_history_event(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    data: HistoryEventData,
) -> Result<()> {
    let event_type = event_type_to_str(&data.event_type());
    let blob =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "insert into {schema}.history_events(run_id, event_id, event_type, data)
             values ($1, $2, $3, $4)"
        ),
        &[
            &run_id.0,
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &event_type,
            &blob,
        ],
    )
    .await
    .map_err(postgres_error)?;
    index_workflow_change_marker(tx, schema, run_id, event_id, &data).await?;
    Ok(())
}

pub(super) enum InlineChildStartOutcome {
    Started(RunId),
    Failed(DurableFailure),
    Skipped,
}

pub(super) async fn start_child_workflow_inline_tx(
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    message: &ChildStartOutboxMessage,
) -> Result<InlineChildStartOutcome> {
    let run_id = RunId::new(format!("run-{}", next_counter(tx, schema, "run").await?));
    let inserted = tx
        .query_opt(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy)
                 values ($1, $2, $3, $4, $5, $6, 1, $7, 0, null, false, $8, $9, $10)
                 on conflict(namespace, workflow_id) do nothing
                 returning run_id"
            ),
            &[
                &namespace,
                &message.workflow_id.0,
                &run_id.0,
                &message.workflow_type.name,
                &(i32::try_from(message.workflow_type.version).unwrap_or(i32::MAX)),
                &message.task_queue.0,
                &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
                &message.command_id.run_id.0,
                &i64::try_from(message.command_id.seq.0).unwrap_or(i64::MAX),
                &parent_close_policy_to_str(message.parent_close_policy),
            ],
        )
        .await
        .map_err(postgres_error)?;

    if inserted.is_some() {
        insert_history_event(
            tx,
            schema,
            &run_id,
            EventId(1),
            HistoryEventData::WorkflowStarted {
                workflow_type: message.workflow_type.clone(),
                input: message.input.clone(),
            },
        )
        .await?;
        return Ok(InlineChildStartOutcome::Started(run_id));
    }

    let Some(row) = tx
        .query_opt(
            &format!(
                "select run_id, parent_run_id, parent_command_seq
                 from {schema}.workflow_instances
                 where namespace = $1 and workflow_id = $2
                 for update"
            ),
            &[&namespace, &message.workflow_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(InlineChildStartOutcome::Skipped);
    };
    let existing_run_id = RunId::new(row.get::<_, String>(0));
    let parent_run_id: Option<String> = row.get(1);
    let parent_command_seq: Option<i64> = row.get(2);
    let same_child = parent_run_id.as_deref() == Some(message.command_id.run_id.0.as_str())
        && parent_command_seq.and_then(|seq| u64::try_from(seq).ok())
            == Some(message.command_id.seq.0);
    if same_child {
        return Ok(InlineChildStartOutcome::Started(existing_run_id));
    }

    Ok(InlineChildStartOutcome::Failed(
        DurableFailure::non_retryable(
            "durust.child_workflow_id_conflict",
            format!("workflow id `{}` is already started", message.workflow_id),
        ),
    ))
}

pub(super) async fn child_event_exists_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<bool> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowStarted),
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    let rows = tx
        .query(
            &format!(
                "select data
                 from {schema}.history_events
                 where run_id = $1
                   and event_type = any($2::text[])"
            ),
            &[&command_id.run_id.0, &child_event_types],
        )
        .await
        .map_err(postgres_error)?;
    for row in rows {
        let blob: Vec<u8> = row.get(0);
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

pub(super) async fn insert_activity_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    map_task: &ActivityMapTask,
) -> Result<()> {
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "insert into {schema}.activity_maps
             (map_command_id, namespace, run_id, command_seq, task, item_count,
              next_ordinal, in_flight, completed)
             values ($1, $2, $3, $4, $5, $6, 0, 0, false)
             on conflict(map_command_id) do nothing"
        ),
        &[
            &map_command_key(&map_task.map_command_id),
            &namespace,
            &map_task.map_command_id.run_id.0,
            &i64::try_from(map_task.map_command_id.seq.0).unwrap_or(i64::MAX),
            &task_blob,
            &i64::try_from(manifest.item_count).unwrap_or(i64::MAX),
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) async fn materialize_activity_map_items_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
) -> Result<()> {
    let key = map_command_key(map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select namespace, task, item_count, next_ordinal, in_flight, completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(());
    };
    let namespace: String = row.get(0);
    let task_blob: Vec<u8> = row.get(1);
    let item_count = u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX);
    let mut next_ordinal = u64::try_from(row.get::<_, i64>(3)).unwrap_or(u64::MAX);
    let mut in_flight = u64::try_from(row.get::<_, i64>(4)).unwrap_or(u64::MAX);
    let completed: bool = row.get(5);
    if completed {
        return Ok(());
    }

    let task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let max_in_flight = u64::try_from(task.max_in_flight.max(1)).unwrap_or(u64::MAX);
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, task.input_manifest.clone())
        .await?;
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
        let item_task = backend
            .normalize_activity_task_for_storage_tx(tx, item_task)
            .await?;
        let item_blob = rmp_serde::to_vec_named(&item_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            &format!(
                "insert into {schema}.activity_tasks
                 (activity_id, namespace, run_id, activity_name, task_queue, task,
                  claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
                 values ($1, $2, $3, $4, $5, $6, null, false, $7, null)
                 on conflict(activity_id) do nothing"
            ),
            &[
                &activity_id.0,
                &namespace,
                &item_task.run_id.0,
                &item_task.activity_name.0,
                &item_task.task_queue.0,
                &item_blob,
                &activity_timeout_at_ms(item_task.start_to_close_timeout),
            ],
        )
        .await
        .map_err(postgres_error)?;
        next_ordinal = next_ordinal.saturating_add(1);
        in_flight = in_flight.saturating_add(1);
    }

    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set next_ordinal = $1, in_flight = $2
             where map_command_id = $3"
        ),
        &[
            &i64::try_from(next_ordinal).unwrap_or(i64::MAX),
            &i64::try_from(in_flight).unwrap_or(i64::MAX),
            &key,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) async fn complete_map_item_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    task: ActivityTask,
    map_item: ActivityMapItem,
    result: PayloadRef,
    activity_id: &ActivityId,
) -> Result<CompleteActivityOutcome> {
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select task, item_count, completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    let task_blob: Vec<u8> = row.get(0);
    let item_count = u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX);
    let completed: bool = row.get(2);
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
    let inserted = tx
        .query_opt(
            &format!(
                "insert into {schema}.activity_map_results(map_command_id, item_ordinal, result)
                 values ($1, $2, $3)
                 on conflict(map_command_id, item_ordinal) do nothing
                 returning item_ordinal"
            ),
            &[
                &key,
                &i64::try_from(map_item.item_ordinal).unwrap_or(i64::MAX),
                &result_blob,
            ],
        )
        .await
        .map_err(postgres_error)?
        .is_some();
    if inserted {
        tx.execute(
            &format!(
                "update {schema}.activity_maps
                 set in_flight = case when in_flight > 0 then in_flight - 1 else 0 end
                 where map_command_id = $1"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?;
    }

    let success_count = tx
        .query_one(
            &format!(
                "select count(*)
                 from {schema}.activity_map_results
                 where map_command_id = $1"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
        .get::<_, i64>(0);
    let success_count = u64::try_from(success_count).unwrap_or(u64::MAX);

    if success_count < item_count {
        materialize_activity_map_items_tx(backend, tx, schema, &map_item.map_command_id).await?;
        let tail = tx
            .query_opt(
                &format!(
                    "select current_event_id
                     from {schema}.workflow_instances
                     where run_id = $1"
                ),
                &[&task.run_id.0],
            )
            .await
            .map_err(postgres_error)?
            .map(|row| EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX)))
            .unwrap_or(EventId::ZERO);
        return Ok(CompleteActivityOutcome::Completed { event_id: tail });
    }

    let map_task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let input_manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&input_manifest_payload)?;
    let result_refs = activity_map_results_tx(tx, schema, &key).await?;
    let result_manifest = encode_activity_map_result_manifest_with_codec(
        map_task.result_manifest_name,
        result_refs,
        &input_manifest.page_lengths,
        backend.payload_config.codec,
    )?;
    let result_manifest = backend
        .normalize_activity_map_result_manifest_for_storage_tx(tx, result_manifest)
        .await?;
    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    let item_count_usize = usize::try_from(item_count).unwrap_or(usize::MAX);
    let success_count_usize = usize::try_from(success_count).unwrap_or(usize::MAX);
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapCompleted(crate::ActivityMapCompleted {
            command_id: map_item.map_command_id,
            result_manifest,
            item_count: item_count_usize,
            success_count: success_count_usize,
            failure_count: 0,
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&key],
    )
    .await
    .map_err(postgres_error)?;
    set_workflow_ready_tx(
        tx,
        schema,
        &task.run_id,
        event_id,
        WorkflowTaskReason::ActivityMapCompleted,
    )
    .await?;
    Ok(CompleteActivityOutcome::Completed { event_id })
}

pub(super) async fn fail_map_item_tx(
    tx: &Transaction<'_>,
    schema: &str,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: DurableFailure,
    activity_id: &ActivityId,
) -> Result<FailActivityOutcome> {
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    let completed: bool = row.get(0);
    if completed {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    }

    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapFailed(crate::ActivityMapFailed {
            command_id: map_item.map_command_id.clone(),
            failure,
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&key],
    )
    .await
    .map_err(postgres_error)?;
    let map_prefix = format!(
        "{}:{}:map:%",
        map_item.map_command_id.run_id, map_item.map_command_id.seq.0
    );
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where activity_id like $1"
        ),
        &[&map_prefix],
    )
    .await
    .map_err(postgres_error)?;
    set_workflow_ready_tx(
        tx,
        schema,
        &task.run_id,
        event_id,
        WorkflowTaskReason::ActivityMapFailed,
    )
    .await?;
    Ok(FailActivityOutcome::Failed { event_id })
}

pub(super) async fn activity_map_results_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_key: &str,
) -> Result<Vec<PayloadRef>> {
    let rows = tx
        .query(
            &format!(
                "select result
                 from {schema}.activity_map_results
                 where map_command_id = $1
                 order by item_ordinal asc"
            ),
            &[&map_command_key],
        )
        .await
        .map_err(postgres_error)?;
    rows.into_iter()
        .map(|row| {
            let blob: Vec<u8> = row.get(0);
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
        })
        .collect()
}

pub(super) async fn timeout_activity_tx(
    tx: &Transaction<'_>,
    schema: &str,
    activity_id: ActivityId,
    now: TimestampMs,
) -> Result<bool> {
    let Some(row) = tx
        .query_opt(
            &format!(
                "select task, completed, timeout_at_ms, heartbeat_deadline_at_ms
                 from {schema}.activity_tasks
                 where activity_id = $1
                 for update"
            ),
            &[&activity_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(false);
    };
    let task_blob: Vec<u8> = row.get(0);
    let completed: bool = row.get(1);
    let timeout_at_ms: Option<i64> = row.get(2);
    let heartbeat_deadline_at_ms: Option<i64> = row.get(3);
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
            &format!(
                "update {schema}.activity_tasks
                 set task = $1,
                     claim_token = null,
                     timeout_at_ms = $2,
                     heartbeat_deadline_at_ms = null
                 where activity_id = $3"
            ),
            &[
                &retry_blob,
                &activity_timeout_at_ms_from(now, retry_task.start_to_close_timeout),
                &activity_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        return Ok(true);
    }

    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    if let Some(map_item) = task.map_item.clone() {
        fail_map_item_tx(
            tx,
            schema,
            task.clone(),
            map_item,
            DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
            ),
            &activity_id,
        )
        .await?;
        return Ok(true);
    }

    let Some(run_row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(run_row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = run_row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityTimedOut(crate::ActivityTimedOut {
            command_id: task.command_id,
            message: timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
             where run_id = $3"
        ),
        &[
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &reason_to_str(&WorkflowTaskReason::ActivityTimedOut),
            &task.run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(true)
}

pub(super) async fn index_workflow_change_marker(
    tx: &Transaction<'_>,
    schema: &str,
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
    let Some(row) = tx
        .query_opt(
            &format!(
                "select namespace, workflow_id, workflow_name, workflow_version, terminal
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    let namespace: String = row.get(0);
    let workflow_id: String = row.get(1);
    let workflow_name: String = row.get(2);
    let workflow_version: i32 = row.get(3);
    let marker_kind = marker_kind_to_str(marker_kind);
    let command_seq = i64::try_from(command_seq.0).unwrap_or(i64::MAX);
    let first_event_id = i64::try_from(event_id.0).unwrap_or(i64::MAX);
    let last_seen_at_ms = unix_epoch_millis();
    tx.execute(
        &format!(
            "insert into {schema}.workflow_change_versions
             (namespace, workflow_id, workflow_name, workflow_version, run_id, change_id,
              version, marker_kind, command_seq, first_event_id, last_seen_at_ms)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             on conflict(run_id, change_id) do update set
                version = excluded.version,
                marker_kind = excluded.marker_kind,
                command_seq = excluded.command_seq,
                first_event_id = excluded.first_event_id,
                last_seen_at_ms = excluded.last_seen_at_ms"
        ),
        &[
            &namespace,
            &workflow_id,
            &workflow_name,
            &workflow_version,
            &run_id.0,
            &change_id,
            &version,
            &marker_kind,
            &command_seq,
            &first_event_id,
            &last_seen_at_ms,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

pub(super) fn is_opaque_external_payload_ref(payload: &PayloadRef) -> bool {
    matches!(payload, PayloadRef::Blob { uri, .. } if uri.starts_with("memory-blob://payload/") || uri.starts_with("s3://"))
}

pub(super) fn collect_failure_payload_roots(
    failure: &DurableFailure,
    roots: &mut Vec<PayloadRootRef>,
) {
    if let Some(details) = &failure.details {
        roots.push(PayloadRootRef::Payload(details.clone()));
    }
}

pub(super) fn decode_payload_blob_row(
    _payload: &PayloadRef,
    row_codec: String,
    row_schema_fingerprint: String,
    row_compression: String,
    encryption_blob: Option<Vec<u8>>,
    stored_size: i64,
    bytes: Vec<u8>,
    ref_codec: crate::CodecId,
    ref_schema_fingerprint: &crate::SchemaFingerprint,
    ref_compression: crate::CompressionId,
    ref_encryption: &Option<crate::EncryptionMetadata>,
    digest: &str,
    size: u64,
) -> Result<PayloadBlob> {
    let actual_digest = digest_bytes(&bytes);
    if actual_digest != digest {
        return Err(Error::PayloadDecode(format!(
            "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
        )));
    }
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != size || u64::try_from(stored_size).unwrap_or(u64::MAX) != size {
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
    if blob.codec != ref_codec
        || blob.schema_fingerprint != *ref_schema_fingerprint
        || blob.compression != ref_compression
        || blob.encryption != *ref_encryption
    {
        return Err(Error::PayloadDecode(format!(
            "payload blob metadata mismatch for `{digest}`"
        )));
    }
    Ok(blob)
}

pub(super) fn encode_encryption_metadata(
    encryption: &Option<crate::EncryptionMetadata>,
) -> Result<Option<Vec<u8>>> {
    encryption
        .as_ref()
        .map(|metadata| {
            rmp_serde::to_vec_named(metadata).map_err(|err| Error::PayloadEncode(err.to_string()))
        })
        .transpose()
}

pub(super) fn decode_encryption_metadata(
    blob: Option<Vec<u8>>,
) -> Result<Option<crate::EncryptionMetadata>> {
    blob.map(|blob| {
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .transpose()
}

pub(super) fn codec_to_str(codec: crate::CodecId) -> &'static str {
    match codec {
        crate::CodecId::MessagePack => "messagepack",
        crate::CodecId::Json => "json",
        crate::CodecId::Protobuf => "protobuf",
    }
}

pub(super) fn codec_from_str(value: &str) -> Result<crate::CodecId> {
    match value {
        "messagepack" => Ok(crate::CodecId::MessagePack),
        "json" => Ok(crate::CodecId::Json),
        "protobuf" => Ok(crate::CodecId::Protobuf),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload codec `{other}`"
        ))),
    }
}

pub(super) fn compression_to_str(compression: crate::CompressionId) -> &'static str {
    match compression {
        crate::CompressionId::None => "none",
    }
}

pub(super) fn compression_from_str(value: &str) -> Result<crate::CompressionId> {
    match value {
        "none" => Ok(crate::CompressionId::None),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload compression `{other}`"
        ))),
    }
}

pub(super) fn reason_to_str(reason: &WorkflowTaskReason) -> &'static str {
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

pub(super) fn reason_from_str(value: &str) -> Result<WorkflowTaskReason> {
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

pub(super) fn parent_close_policy_to_str(policy: ParentClosePolicy) -> &'static str {
    match policy {
        ParentClosePolicy::Cancel => "cancel",
        ParentClosePolicy::Abandon => "abandon",
    }
}

pub(super) fn map_command_key(command_id: &CommandId) -> String {
    format!("{}:{}", command_id.run_id, command_id.seq.0)
}

pub(super) fn marker_kind_to_str(kind: WorkflowChangeMarkerKind) -> &'static str {
    match kind {
        WorkflowChangeMarkerKind::Version => "version",
        WorkflowChangeMarkerKind::DeprecatedPatch => "deprecated_patch",
    }
}

pub(super) fn marker_kind_from_str(value: &str) -> Result<WorkflowChangeMarkerKind> {
    match value {
        "version" => Ok(WorkflowChangeMarkerKind::Version),
        "deprecated_patch" => Ok(WorkflowChangeMarkerKind::DeprecatedPatch),
        other => Err(Error::Backend(format!(
            "unknown workflow change marker kind `{other}`"
        ))),
    }
}

pub(super) fn event_type_to_str(event_type: &HistoryEventType) -> &'static str {
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
        HistoryEventType::SideEffectMarker => "side_effect_marker",
    }
}

pub(super) fn event_type_from_str(value: &str) -> Result<HistoryEventType> {
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
        "side_effect_marker" => Ok(HistoryEventType::SideEffectMarker),
        other => Err(Error::Backend(format!("unknown event type `{other}`"))),
    }
}
