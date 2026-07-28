import { createHash } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import {
  activityOutcomeCounts,
  completeMapItems,
  eventId,
  historyEventType,
  itemRetryDecision,
  mapCommandCancelledReason,
  mapRejectMessage,
  outcomeCounts,
  recordedOutcomeCount,
  runId,
  step as stepMap,
  workflowTaskCommitHasWorkflowVisibleMutations,
  type ItemAttemptFailureKind,
  type ItemRetryDecision,
  type MapEffect,
  type MapEvent,
  type MapState
} from "@durust/core";
import {
  activityHeartbeatDeadlineAt,
  activityLeaseMatches,
  activityMapItemId,
  activityTimeoutDeadline,
  activityTimeoutFailure,
  activityTimeoutMessage,
  childWorkflowMapItemOutcome,
  commandKey,
  decodeActivityMapInputs,
  encodeActivityMapResultManifest,
  encodeChildWorkflowMapResultManifest,
  makeHistoryEvent,
  mapItemInFlight,
  parseJson,
  retryActivityAfterFailure,
  retryActivityAfterTimeout,
  retryActivityTaskAfterTimeout,
  sameCommandId,
  stringifyJson,
  tailEventId,
  workflowKey,
  workflowLeaseMatches,
  type ChildTerminalUpdate
} from "@durust/core";
import type {
  ActivityMapTask,
  ActivityHeartbeatOutcome,
  ActivityHeartbeatRequest,
  ActivityTask,
  ActivityTaskClaim,
  ChildWorkflowMapItemOutcome,
  ChildWorkflowMapTask,
  ChildWorkflowStartRequested,
  ClaimedActivityTask,
  ClaimedWorkflowTask,
  ClaimActivityOptions,
  ClaimWorkflowBatchOptions,
  ClaimWorkflowTaskOptions,
  CommandId,
  CommitOutcome,
  CompleteActivitiesOutcome,
  CompleteActivitiesRequest,
  CompleteActivityItemOutcome,
  CompleteActivityOutcome,
  CompleteActivityRequest,
  DurableBackend,
  DurableFailure,
  EventId,
  FailActivityOutcome,
  FailActivityRequest,
  FireDueTimersOutcome,
  FireDueTimersRequest,
  HistoryChunk,
  HistoryEvent,
  HistoryEventData,
  Namespace,
  PayloadRef,
  QueryWorkflowOutcome,
  QueryWorkflowRequest,
  ReadSignalInboxRequest,
  ReleaseWorkflowTaskOptions,
  RunId,
  SignalInboxRecord,
  SignalName,
  SignalWorkflowOutcome,
  SignalWorkflowRequest,
  StartWorkflowOutcome,
  StartWorkflowRequest,
  StreamHistoryRequest,
  TimestampMs,
  TimeoutDueActivitiesOutcome,
  TimeoutDueActivitiesRequest,
  WaitRecord,
  WorkerId,
  WorkflowId,
  WorkflowTaskClaim,
  WorkflowTaskCommit,
  WorkflowTaskReason,
  WorkflowType
} from "@durust/core";

interface WorkflowState {
  readonly namespace: string;
  readonly workflowId: string;
  readonly workflowType: WorkflowType;
  readonly taskQueue: string;
  readonly runId: RunId;
  history: HistoryEvent[];
  readyReason: WorkflowTaskReason | null;
  readyAtMs: number;
  claim: WorkflowLease | null;
  queryProjection: PayloadRef | null;
  terminal: boolean;
  parent: ParentWorkflowLink | null;
}

interface WorkflowLease {
  readonly claim: WorkflowTaskClaim;
  readonly reason: WorkflowTaskReason;
  readonly expiresAtMs: number;
}

type ParentWorkflowLink = ChildParentWorkflowLink | ChildWorkflowMapParentLink;

interface ChildParentWorkflowLink {
  readonly kind: "Child";
  readonly parentRunId: RunId;
  readonly commandId: CommandId;
  readonly parentClosePolicy: string;
}

interface ChildWorkflowMapParentLink {
  readonly kind: "ChildWorkflowMap";
  readonly parentRunId: RunId;
  readonly mapCommandId: CommandId;
  readonly itemOrdinal: number;
  readonly parentClosePolicy: string;
}

interface ActivityState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  task: ActivityTask;
  claim: ActivityLease | null;
  availableAtMs: number;
  terminalEventId: EventId | null;
}

interface ActivityLease {
  readonly claim: ActivityTaskClaim;
  readonly startedAtMs: number;
  readonly heartbeatDeadlineAtMs: number | null;
  readonly expiresAtMs: number;
  readonly leaseDurationMs: number;
}

interface ActivityMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ActivityMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly results: (PayloadRef | null)[];
  /**
   * Slots taken by admitted, not-yet-terminal items. Written only by
   * `AdvanceDescriptor` and `MarkDescriptorTerminal`, never by the provider,
   * so the engine's admission accounting has exactly one owner.
   */
  inFlight: number;
  nextOrdinal: number;
  terminal: boolean;
}

interface ChildWorkflowMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ChildWorkflowMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly outcomes: (ChildWorkflowMapItemOutcome<unknown> | null)[];
  inFlight: number;
  nextOrdinal: number;
  terminal: boolean;
}

interface SignalState {
  readonly runId: RunId;
  readonly signalName: SignalName | string;
  readonly payload: PayloadRef;
  readonly receivedSequence: number;
  consumed: boolean;
}

export interface PostgresBackendOptions {
  readonly url?: string;
  readonly connectionString?: string;
  readonly tableName?: string;
  readonly poolSize?: number;
  readonly pool?: Pool;
  readonly nowMs?: () => number;
}

export interface PostgresBackendStatsSnapshot {
  readonly walBytes: number;
  readonly walRecords: number;
  readonly walFpi: number;
  readonly walBuffersFull: number;
  readonly walWrite: number;
  readonly walSync: number;
  readonly walWriteTimeMs: number;
  readonly walSyncTimeMs: number;
  readonly xactCommit: number;
  readonly xactRollback: number;
  readonly rowsReturned: number;
  readonly rowsFetched: number;
  readonly rowsInserted: number;
  readonly rowsUpdated: number;
  readonly rowsDeleted: number;
  readonly blocksRead: number;
  readonly blocksHit: number;
  readonly tempFiles: number;
  readonly tempBytes: number;
  readonly deadlocks: number;
  readonly blockReadTimeMs: number;
  readonly blockWriteTimeMs: number;
  readonly activeConnections: number;
  readonly statements: readonly PostgresStatementStatsSnapshot[];
  /**
   * Why `statements` is empty, when it is empty for a reason.
   *
   * `null` means nothing went wrong — either the statements were collected, or
   * the run genuinely issued none. A string means the snapshot *failed*, and
   * it keeps the server's own error plus the hint that fixes it.
   *
   * This exists because the two states used to be indistinguishable. Both
   * `create extension if not exists pg_stat_statements` and the
   * `pg_stat_statements` select swallowed their failures — the same bare
   * discard the Rust harness carried — so a missing extension arrived as an
   * empty array and `require_postgres_statement_stats: true` failed with
   * `expected null not to be null`, a message that names neither the cause nor
   * the fix.
   */
  readonly statementStatsUnavailable: string | null;
}

export interface PostgresStatementStatsSnapshot {
  readonly queryId: string;
  readonly query: string;
  readonly calls: number;
  readonly totalExecTimeMs: number;
}

interface NormalizedRewriteScope {
  readonly history: boolean;
  readonly workflowIds: boolean;
  readonly workflows: boolean;
  readonly queryProjections: boolean;
  readonly waits: boolean;
  readonly signals: boolean;
  readonly activityTasks: boolean;
  readonly mapStates: boolean;
}

type NormalizedRewriteScopeSelector<T> =
  | NormalizedRewriteScope
  | ((result: T) => NormalizedRewriteScope);

const fullNormalizedRewriteScope: NormalizedRewriteScope = {
  history: true,
  workflowIds: true,
  workflows: true,
  queryProjections: true,
  waits: true,
  signals: true,
  activityTasks: true,
  mapStates: true
};

const simpleWorkflowCommitRewriteScope: NormalizedRewriteScope = {
  ...fullNormalizedRewriteScope,
  workflowIds: false,
  workflows: false,
  queryProjections: false,
  waits: false,
  signals: false,
  activityTasks: false,
  mapStates: false
};

const activityTaskRewriteScope: NormalizedRewriteScope = {
  ...fullNormalizedRewriteScope,
  history: false,
  workflowIds: false,
  workflows: false,
  queryProjections: false,
  waits: false,
  signals: false,
  activityTasks: false,
  mapStates: false
};

const scalarActivityMutationRewriteScope: NormalizedRewriteScope = {
  ...fullNormalizedRewriteScope,
  workflowIds: false,
  workflows: false,
  queryProjections: false,
  waits: false,
  signals: false,
  activityTasks: false,
  mapStates: false
};

const activityTimeoutRewriteScope: NormalizedRewriteScope = {
  ...fullNormalizedRewriteScope,
  workflowIds: false,
  queryProjections: false,
  waits: false,
  signals: false,
  mapStates: false
};

interface TargetedActivityProjectionUpdate {
  readonly activity: ActivityState;
  readonly workflow: WorkflowState | null;
}

export class PostgresBackend implements DurableBackend {
  readonly #pool: Pool;
  readonly #ownsPool: boolean;
  readonly #rawTableName: string;
  readonly #historyTableName: string;
  readonly #queryProjectionsTableName: string;
  readonly #workflowIdsTableName: string;
  readonly #workflowRunsTableName: string;
  readonly #waitsTableName: string;
  readonly #signalsTableName: string;
  readonly #activityTasksTableName: string;
  readonly #activityMapsTableName: string;
  readonly #activityMapItemsTableName: string;
  readonly #childWorkflowMapsTableName: string;
  readonly #childWorkflowMapItemsTableName: string;
  readonly #countersTableName: string;
  readonly #nowMs: () => number;
  #ready: Promise<void> | null = null;
  readonly #workflowsById = new Map<string, WorkflowState>();
  readonly #workflowsByRun = new Map<string, WorkflowState>();
  readonly #activitiesById = new Map<string, ActivityState>();
  readonly #activityMapsByCommand = new Map<string, ActivityMapState>();
  readonly #childWorkflowMapsByCommand = new Map<string, ChildWorkflowMapState>();
  readonly #waitsById = new Map<string, WaitRecord>();
  readonly #signalsById = new Map<string, SignalState>();
  #nextRun = 1;
  #nextClaimToken = 1;
  #nextActivityClaimToken = 1;
  #nextSignalSequence = 1;

  constructor(options: PostgresBackendOptions = {}) {
    const baseTableName = options.tableName ?? "durust_ts_provider_state";
    this.#rawTableName = baseTableName;
    this.#historyTableName = derivedSqlIdentifier(baseTableName, "history_events");
    this.#queryProjectionsTableName = derivedSqlIdentifier(baseTableName, "query_projections");
    this.#workflowIdsTableName = derivedSqlIdentifier(baseTableName, "workflow_ids");
    this.#workflowRunsTableName = derivedSqlIdentifier(baseTableName, "workflow_runs");
    this.#waitsTableName = derivedSqlIdentifier(baseTableName, "waits");
    this.#signalsTableName = derivedSqlIdentifier(baseTableName, "signals");
    this.#activityTasksTableName = derivedSqlIdentifier(baseTableName, "activity_tasks");
    this.#activityMapsTableName = derivedSqlIdentifier(baseTableName, "activity_maps");
    this.#activityMapItemsTableName = derivedSqlIdentifier(baseTableName, "activity_map_items");
    this.#childWorkflowMapsTableName = derivedSqlIdentifier(baseTableName, "child_workflow_maps");
    this.#childWorkflowMapItemsTableName = derivedSqlIdentifier(
      baseTableName,
      "child_workflow_map_items"
    );
    this.#countersTableName = derivedSqlIdentifier(baseTableName, "counters");
    this.#nowMs = options.nowMs ?? Date.now;
    if (options.pool) {
      this.#pool = options.pool;
      this.#ownsPool = false;
    } else {
      const connectionString =
        options.connectionString ?? options.url ?? process.env.DURUST_POSTGRES_URL;
      if (!connectionString) {
        throw new Error(
          "PostgresBackend requires options.url, options.connectionString, or DURUST_POSTGRES_URL"
        );
      }
      this.#pool = new Pool({
        connectionString,
        max: options.poolSize ?? 10
      });
      // `pg`'s Pool is an EventEmitter, and an idle client that dies emits
      // `'error'`. With no listener Node treats that as an unhandled `'error'`
      // event and **kills the process**, so a server-side disconnect crashes
      // the host application instead of failing a query.
      //
      // Reachable, measured with `pg_terminate_backend`: `Unhandled 'error'
      // event … terminating connection due to administrator command`, exit 1.
      // The benchmark's `drop database … with (force)` is one producer — it
      // terminates live backends by design — and the happy path escapes only
      // because `destroy()` calls `pool.end()` first. So the crash is reachable
      // exactly when `destroy()` throws on one of its twelve `drop table`
      // statements, and then this stack *replaces* the real failure, hiding it.
      //
      // Only for a pool this backend owns. A caller-supplied pool is the
      // caller's to instrument, and attaching here would silently suppress a
      // listener they are entitled to own.
      this.#pool.on("error", (error) => {
        console.error(
          `postgres provider: idle client error (${
            error instanceof Error ? error.message : String(error)
          }); the pool will discard it`
        );
      });
      this.#ownsPool = true;
    }
  }

  async close(): Promise<void> {
    await this.#ensureReady();
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  async destroy(): Promise<void> {
    await this.#ensureReady();
    await this.#pool.query(`drop table if exists ${this.#childWorkflowMapItemsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#childWorkflowMapsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#activityMapItemsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#activityMapsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#activityTasksTableName}`);
    await this.#pool.query(`drop table if exists ${this.#signalsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#waitsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#queryProjectionsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#workflowIdsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#workflowRunsTableName}`);
    await this.#pool.query(`drop table if exists ${this.#historyTableName}`);
    await this.#pool.query(`drop table if exists ${this.#countersTableName}`);
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  /**
   * Why `pg_stat_statements` could not be set up at open, or `null` if it
   * could. Read by `statsSnapshot`, which prefers it over a query-time
   * failure.
   */
  #statementStatsSetupFailure: string | null = null;

  /** So a run that loses its statement stats says so once, not once per snapshot. */
  #statementStatsUnavailableReported = false;

  #reportStatementStatsUnavailable(reason: string | null): string | null {
    if (reason !== null && !this.#statementStatsUnavailableReported) {
      this.#statementStatsUnavailableReported = true;
      console.error(`postgres provider: ${reason}`);
    }
    return reason;
  }

  async statsSnapshot(): Promise<PostgresBackendStatsSnapshot> {
    await this.#ensureReady();
    const client = await this.#pool.connect();
    try {
      const database = await client.query(`
        select
          xact_commit,
          xact_rollback,
          tup_returned,
          tup_fetched,
          tup_inserted,
          tup_updated,
          tup_deleted,
          blks_read,
          blks_hit,
          temp_files,
          temp_bytes,
          deadlocks,
          blk_read_time,
          blk_write_time
        from pg_stat_database
        where datname = current_database()
      `);
      const databaseRow = database.rows[0] as Record<string, unknown> | undefined;
      if (databaseRow === undefined) {
        throw new Error("pg_stat_database did not return current database stats");
      }
      const connections = await client.query(`
        select count(*) as active_connections
        from pg_stat_activity
        where datname = current_database()
      `);
      const connectionRow = connections.rows[0] as Record<string, unknown> | undefined;
      const wal = await postgresWalStats(client);
      const statementStats = await postgresStatementStats(client);
      return {
        ...wal,
        xactCommit: postgresStatNumber(databaseRow, "xact_commit"),
        xactRollback: postgresStatNumber(databaseRow, "xact_rollback"),
        rowsReturned: postgresStatNumber(databaseRow, "tup_returned"),
        rowsFetched: postgresStatNumber(databaseRow, "tup_fetched"),
        rowsInserted: postgresStatNumber(databaseRow, "tup_inserted"),
        rowsUpdated: postgresStatNumber(databaseRow, "tup_updated"),
        rowsDeleted: postgresStatNumber(databaseRow, "tup_deleted"),
        blocksRead: postgresStatNumber(databaseRow, "blks_read"),
        blocksHit: postgresStatNumber(databaseRow, "blks_hit"),
        tempFiles: postgresStatNumber(databaseRow, "temp_files"),
        tempBytes: postgresStatNumber(databaseRow, "temp_bytes"),
        deadlocks: postgresStatNumber(databaseRow, "deadlocks"),
        blockReadTimeMs: postgresStatNumber(databaseRow, "blk_read_time"),
        blockWriteTimeMs: postgresStatNumber(databaseRow, "blk_write_time"),
        activeConnections:
          connectionRow === undefined
            ? 0
            : postgresStatNumber(connectionRow, "active_connections"),
        statements: statementStats.statements,
        statementStatsUnavailable: this.#reportStatementStatsUnavailable(
          // The setup failure is the more useful of the two when both fire:
          // it is the one that says the extension could not be created, while
          // the query failure only says the view is missing.
          this.#statementStatsSetupFailure ?? statementStats.unavailable
        )
      };
    } finally {
      client.release();
    }
  }

  /**
   * This provider's clock — the same `#nowMs` every lease, deadline and
   * due-scan in this file reads, not `Date.now()`.
   *
   * It has to be the same one. `PostgresBackendOptions.nowMs` already decides
   * when a lease has expired and when an activity has missed its deadline, so
   * a `currentTime()` that answered from the process clock would hand the
   * runtime an instant this provider does not believe in: `sleep(d)` would
   * record a deadline the provider's own `fireDueTimers` scan never reaches.
   */
  async currentTime(): Promise<TimestampMs> {
    return this.#nowMs() as TimestampMs;
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    return this.#withSqlTransaction(async (client) => {
      const existing = await this.#selectCurrentWorkflowRunId(
        client,
        req.namespace,
        req.workflowId
      );
      if (existing !== null) {
        return { kind: "AlreadyStarted", runId: runId(existing) };
      }

      const nextRun = await this.#allocateCounterRange(client, "run", 1);
      const newRunId = runId(`run-${nextRun}`);
      const inserted = await client.query<{ readonly run_id: string }>(
        `
          insert into ${this.#workflowIdsTableName}(namespace, workflow_id, run_id)
          values ($1, $2, $3)
          on conflict (namespace, workflow_id) do nothing
          returning run_id
        `,
        [String(req.namespace), String(req.workflowId), String(newRunId)]
      );
      if (inserted.rows.length === 0) {
        const selected = await this.#selectCurrentWorkflowRunId(
          client,
          req.namespace,
          req.workflowId
        );
        if (selected === null) {
          throw new Error(`workflow id conflict without current run: ${req.workflowId}`);
        }
        return { kind: "AlreadyStarted", runId: runId(selected) };
      }

      const started = makeHistoryEvent(eventId(1), {
        kind: "WorkflowStarted",
        workflowType: req.workflowType,
        input: req.input
      });
      await this.#appendNormalizedHistoryRows(client, [normalizedHistoryRow(newRunId, started)]);
      await client.query(
        `
          insert into ${this.#workflowRunsTableName}(
            run_id,
            namespace,
            workflow_id,
            workflow_type_name,
            workflow_type_version,
            workflow_type,
            task_queue,
            tail_event_id,
            ready_reason,
            ready_at_ms,
            claim_worker_id,
            claim_token,
            claim_reason,
            claim_expires_at_ms,
            query_projection,
            terminal,
            parent
          )
          values ($1, $2, $3, $4, $5::integer, $6::jsonb, $7, 1, 'WorkflowStarted', 0,
            null, null, null, null, null, false, null)
        `,
        [
          String(newRunId),
          String(req.namespace),
          String(req.workflowId),
          req.workflowType.name,
          req.workflowType.version,
          stringifyJson(req.workflowType),
          String(req.taskQueue)
        ]
      );
      return { kind: "Started", runId: newRunId };
    });
  }

  async claimWorkflowTask(
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    const claimed = await this.claimWorkflowTasks(workerId, { ...opts, limit: 1 });
    return claimed[0] ?? null;
  }

  async claimWorkflowTasks(
    workerId: WorkerId | string,
    opts: ClaimWorkflowBatchOptions
  ): Promise<readonly ClaimedWorkflowTask[]> {
    if (opts.registeredWorkflowTypes.length === 0) {
      return [];
    }
    const limit = Math.max(1, Math.trunc(opts.limit));
    return this.#withSqlTransaction(async (client) => {
      const registeredTypes = stringifyJson(
        opts.registeredWorkflowTypes.map((workflowType) => ({
          name: workflowType.name,
          version: workflowType.version
        }))
      );
      const now = this.#nowMs();
      const leaseExpiresAt = this.#leaseExpiresAt(opts.leaseDurationMs);
      let rows = await this.#claimReadyWorkflowTaskRows(
        client,
        workerId,
        opts,
        registeredTypes,
        leaseExpiresAt,
        limit
      );
      if (rows.length === 0) {
        rows = await this.#claimExpiredWorkflowTaskRows(
          client,
          workerId,
          opts,
          registeredTypes,
          now,
          leaseExpiresAt,
          limit
        );
      }
      return await this.#hydrateWorkflowClaimRows(
        client,
        workerId,
        rows,
        opts.registeredSignalNames ?? []
      );
    });
  }

  async #claimReadyWorkflowTaskRows(
    client: PoolClient,
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions,
    registeredTypes: string,
    leaseExpiresAt: number,
    limit: number
  ): Promise<readonly WorkflowClaimRow[]> {
    const result = await client.query<WorkflowClaimRow>(
      `
        with selected as (
          select
            runs.run_id,
            runs.workflow_id,
            runs.workflow_type,
            runs.tail_event_id,
            runs.ready_reason as reason
          from ${this.#workflowRunsTableName} runs
          join jsonb_to_recordset($3::jsonb) as registered(name text, version integer)
            on registered.name = runs.workflow_type_name
           and registered.version = runs.workflow_type_version
          where runs.namespace = $1
            and runs.task_queue = $2
            and runs.terminal = false
            and runs.ready_reason is not null
            and runs.ready_at_ms <= $7::bigint
            and runs.claim_token is null
          order by runs.run_id asc
          limit $6::bigint
          for update of runs skip locked
        ),
        numbered as (
          select
            selected.*,
            row_number() over (order by selected.run_id asc) - 1 as token_offset
          from selected
        ),
        claimed_count as (
          select count(*)::bigint as value from numbered
        ),
        token as (
          update ${this.#countersTableName} counters
          set next_value = next_value + claimed_count.value
          from claimed_count
          where counters.name = 'workflow_claim'
            and claimed_count.value > 0
          returning counters.next_value - claimed_count.value as first_token
        )
        update ${this.#workflowRunsTableName} runs
        set
          ready_reason = null,
          claim_worker_id = $4,
          claim_token = token.first_token + numbered.token_offset,
          claim_reason = numbered.reason,
          claim_expires_at_ms = $5::bigint
        from numbered, token
        where runs.run_id = numbered.run_id
        returning
          runs.run_id,
          numbered.workflow_id,
          numbered.workflow_type,
          numbered.tail_event_id,
          numbered.reason,
          token.first_token + numbered.token_offset as token
      `,
      [
        String(opts.namespace),
        String(opts.taskQueue),
        registeredTypes,
        String(workerId),
        String(leaseExpiresAt),
        String(limit),
        String(this.#nowMs())
      ]
    );
    return workflowClaimRowsInDeterministicOrder(result.rows);
  }

  async #claimExpiredWorkflowTaskRows(
    client: PoolClient,
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions,
    registeredTypes: string,
    now: number,
    leaseExpiresAt: number,
    limit: number
  ): Promise<readonly WorkflowClaimRow[]> {
    const result = await client.query<WorkflowClaimRow>(
      `
        with selected as (
          select
            runs.run_id,
            runs.workflow_id,
            runs.workflow_type,
            runs.tail_event_id,
            runs.claim_reason as reason
          from ${this.#workflowRunsTableName} runs
          join jsonb_to_recordset($3::jsonb) as registered(name text, version integer)
            on registered.name = runs.workflow_type_name
           and registered.version = runs.workflow_type_version
          where runs.namespace = $1
            and runs.task_queue = $2
            and runs.terminal = false
            and runs.claim_token is not null
            and runs.claim_reason is not null
            and runs.claim_expires_at_ms is not null
            and runs.claim_expires_at_ms <= $4::bigint
          order by runs.run_id asc
          limit $7::bigint
          for update of runs skip locked
        ),
        numbered as (
          select
            selected.*,
            row_number() over (order by selected.run_id asc) - 1 as token_offset
          from selected
        ),
        claimed_count as (
          select count(*)::bigint as value from numbered
        ),
        token as (
          update ${this.#countersTableName} counters
          set next_value = next_value + claimed_count.value
          from claimed_count
          where counters.name = 'workflow_claim'
            and claimed_count.value > 0
          returning counters.next_value - claimed_count.value as first_token
        )
        update ${this.#workflowRunsTableName} runs
        set
          ready_reason = null,
          claim_worker_id = $5,
          claim_token = token.first_token + numbered.token_offset,
          claim_reason = numbered.reason,
          claim_expires_at_ms = $6::bigint
        from numbered, token
        where runs.run_id = numbered.run_id
        returning
          runs.run_id,
          numbered.workflow_id,
          numbered.workflow_type,
          numbered.tail_event_id,
          numbered.reason,
          token.first_token + numbered.token_offset as token
      `,
      [
        String(opts.namespace),
        String(opts.taskQueue),
        registeredTypes,
        String(now),
        String(workerId),
        String(leaseExpiresAt),
        String(limit)
      ]
    );
    return workflowClaimRowsInDeterministicOrder(result.rows);
  }

  async #hydrateWorkflowClaimRows(
    client: PoolClient,
    workerId: WorkerId | string,
    rows: readonly WorkflowClaimRow[],
    signalNames: readonly (SignalName | string)[]
  ): Promise<readonly ClaimedWorkflowTask[]> {
    if (rows.length === 0) {
      return [];
    }
    const orderedRows = workflowClaimRowsInDeterministicOrder(rows);
    const targets = orderedRows.map((row) => ({
      runId: runId(row.run_id),
      replayTargetEventId: eventId(postgresRequiredNumber(row.tail_event_id))
    }));
    const histories = await this.#readNormalizedHistoriesForClaims(client, targets);
    const liveSignals = await this.#readSignalInboxesForClaims(
      client,
      targets.map((target) => target.runId),
      signalNames
    );
    return orderedRows.map((row) => {
      const target = targets.find((item) => String(item.runId) === row.run_id);
      if (target === undefined) {
        throw new Error(`missing workflow claim target for ${row.run_id}`);
      }
      const claim: WorkflowTaskClaim = {
        runId: target.runId,
        workerId,
        token: postgresRequiredNumber(row.token)
      };
      return {
        runId: claim.runId,
        workflowId: row.workflow_id,
        workflowType: parsePostgresJson(row.workflow_type) as WorkflowType,
        claim,
        replayTargetEventId: target.replayTargetEventId,
        reason: row.reason as WorkflowTaskReason,
        prefetchedHistory: histories.get(row.run_id) ?? [],
        liveSignals: liveSignals.get(row.run_id) ?? []
      };
    });
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    await this.#ensureReady();
    const maxEvents = Math.max(0, Math.trunc(req.maxEvents));
    const fetchLimit = Math.min(maxEvents + 1, 2_147_483_647);
    const result = await this.#pool.query<NormalizedHistorySelectRow>(
      `
        select event_id, event_type, data
        from ${this.#historyTableName}
        where run_id = $1 and event_id::bigint > $2::bigint and event_id::bigint <= $3::bigint
        order by event_id asc
        limit $4::bigint
      `,
      [
        String(req.runId),
        String(Number(req.afterEventId)),
        String(Number(req.upToEventId)),
        String(fetchLimit)
      ]
    );
    const events = result.rows.slice(0, maxEvents).map(historyEventFromNormalizedRow);
    const lastEvent = events.at(-1);
    return {
      events,
      lastEventId: lastEvent?.eventId ?? req.afterEventId,
      hasMore: result.rows.length > events.length
    };
  }

  async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    if (canUseSqlNativeWorkflowCommit(commit)) {
      const direct = await this.#commitWorkflowTaskSqlNative(claim, commit);
      if (direct !== null) {
        return direct;
      }
    }
    const targetedProjectionUpdates = canUseTargetedWorkflowCommitProjectionUpdates(commit);
    return this.#withState(async (client) => {
    const state = this.#stateForRun(claim.runId);
    this.#restoreExpiredWorkflowLease(state);
    if (
      state.claim === null ||
      !workflowLeaseMatches(state.claim, claim)
    ) {
      throw new Error("stale workflow task lease");
    }
    if (state.terminal && workflowTaskCommitHasWorkflowVisibleMutations(commit)) {
      throw new Error("terminal workflow rejects workflow-visible mutations");
    }

    if (commit.expectedTailEventId !== tailEventId(state)) {
      state.claim = null;
      state.readyReason = "CacheEvicted";
      state.readyAtMs = 0;
      if (targetedProjectionUpdates) {
        await this.#upsertNormalizedWorkflowRow(client, state);
      }
      return { kind: "Conflict" };
    }

    let continuedInput: PayloadRef | null = null;
    let childTerminal: ChildTerminalUpdate | null = null;
    for (const event of commit.appendEvents ?? []) {
      const nextEventId = eventId(Number(tailEventId(state)) + 1);
      state.history.push(makeHistoryEvent(nextEventId, event.data));
      if (event.data.kind === "WorkflowCompleted") {
        state.terminal = true;
        childTerminal = { kind: "Completed", result: event.data.result };
      }
      if (event.data.kind === "WorkflowFailed") {
        state.terminal = true;
        childTerminal = { kind: "Failed", failure: event.data.failure };
      }
      if (event.data.kind === "WorkflowCancelled") {
        state.terminal = true;
        childTerminal = { kind: "Cancelled", reason: event.data.reason };
      }
      if (event.data.kind === "WorkflowContinuedAsNew") {
        state.terminal = true;
        continuedInput = event.data.input;
      }
    }
    for (const wait of commit.upsertWaits ?? []) {
      this.#waitsById.set(String(wait.waitId), wait);
      if (targetedProjectionUpdates) {
        await this.#upsertNormalizedWaitRow(client, String(wait.waitId), wait);
      }
    }
    for (const waitId of commit.deleteWaits ?? []) {
      this.#waitsById.delete(String(waitId));
      if (targetedProjectionUpdates) {
        await this.#deleteNormalizedWaitRow(client, String(waitId));
      }
    }
    for (const signalId of commit.consumeSignals ?? []) {
      const signal = this.#signalsById.get(String(signalId));
      if (signal) {
        signal.consumed = true;
        if (targetedProjectionUpdates) {
          await this.#upsertNormalizedSignalRow(client, String(signalId), signal);
        }
      }
    }
    for (const task of commit.scheduleActivities ?? []) {
      const activityState: ActivityState = {
        namespace: state.namespace,
        workflow: state,
        task,
        claim: null,
        availableAtMs: 0,
        terminalEventId: null
      };
      this.#activitiesById.set(task.activityId, activityState);
      if (targetedProjectionUpdates) {
        await this.#upsertNormalizedActivityTaskRow(client, activityState);
      }
    }
    for (const task of commit.scheduleActivityMaps ?? []) {
      this.#createActivityMap(state, task);
    }
    for (const child of commit.startChildWorkflows ?? []) {
      this.#startChildWorkflow(state, child);
    }
    for (const task of commit.scheduleChildWorkflowMaps ?? []) {
      this.#createChildWorkflowMap(state, task);
    }
    // After every schedule loop; see `MemoryBackend.commitWorkflowTask` for
    // why, and Rust's `src/memory.rs` for the order all four providers now
    // share. A commit that both schedules and withdraws one command — a
    // `select` settling on its first pass — cancelled nothing and then
    // inserted the task live.
    //
    // Note what makes this safe under `targetedProjectionUpdates`, because it
    // is not what it looks like: `#cancelCommandOperationalState` is
    // synchronous and writes **no** normalized rows of its own — no `await`,
    // no `client.query`, no `#upsertNormalized…`. It mutates in-memory state
    // and relies entirely on the closing full rewrite to persist it. The only
    // reason that is correct is that
    // `canUseTargetedWorkflowCommitProjectionUpdates` refuses any commit with
    // a non-empty `cancelCommands`, so this loop never runs under the targeted
    // scope. **Narrowing that predicate without first giving this path its own
    // row writes would silently drop the cancellation.**
    for (const cancelled of commit.cancelCommands ?? []) {
      this.#cancelCommandOperationalState(cancelled);
    }
    if (commit.queryProjection !== undefined) {
      state.queryProjection = commit.queryProjection;
    }

    state.claim = null;
    this.#wakeIfSignalWaitReady(state);
    if (continuedInput !== null) {
      this.#startContinuedRun(state, continuedInput);
    }
    if (childTerminal !== null && state.parent !== null) {
      this.#notifyParentOfChildTerminal(state.parent, childTerminal);
    }
    if (state.terminal) {
      this.#abandonWorkForClosedRun(state);
      this.#cancelChildrenForClosedParent(state);
    }
    if (targetedProjectionUpdates) {
      await this.#upsertNormalizedWorkflowRow(client, state);
      if (commit.queryProjection !== undefined) {
        await this.#upsertNormalizedQueryProjectionRow(client, state);
      }
    }
    return { kind: "Committed", newTailEventId: tailEventId(state) };
    }, targetedProjectionUpdates ? simpleWorkflowCommitRewriteScope : fullNormalizedRewriteScope);
  }

  async releaseWorkflowTask(
    claim: WorkflowTaskClaim,
    options: ReleaseWorkflowTaskOptions = {}
  ): Promise<void> {
    await this.#ensureReady();
    const readyAtMs = this.#nowMs() + Math.max(0, options.visibilityDelayMs ?? 0);
    await this.#pool.query(
      `
        update ${this.#workflowRunsTableName}
        set
          ready_reason = claim_reason,
          ready_at_ms = $4::bigint,
          claim_worker_id = null,
          claim_token = null,
          claim_reason = null,
          claim_expires_at_ms = null
        where run_id = $1
          and claim_worker_id = $2
          and claim_token = $3::bigint
          and claim_reason is not null
      `,
      [
        String(claim.runId),
        String(claim.workerId),
        String(claim.token),
        String(readyAtMs)
      ]
    );
  }

  async #commitWorkflowTaskSqlNative(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome | null> {
    return this.#withSqlTransaction(async (client) => {
      const selected = await client.query<NormalizedWorkflowRunLoadRow>(
        `
          select
            run_id,
            namespace,
            workflow_id,
            workflow_type_name,
            workflow_type_version,
            workflow_type,
            task_queue,
            tail_event_id,
            ready_reason,
            ready_at_ms,
            claim_worker_id,
            claim_token,
            claim_reason,
            claim_expires_at_ms,
            query_projection,
            terminal,
            parent
          from ${this.#workflowRunsTableName}
          where run_id = $1
          for update
        `,
        [String(claim.runId)]
      );
      const row = selected.rows[0];
      if (row === undefined) {
        throw new Error(`workflow run not found: ${claim.runId}`);
      }
      const claimToken = postgresOptionalNumber(row.claim_token);
      const claimExpiresAt = postgresOptionalNumber(row.claim_expires_at_ms);
      if (
        claimToken === null ||
        claimToken !== claim.token ||
        String(row.claim_worker_id ?? "") !== String(claim.workerId) ||
        claimExpiresAt === null ||
        claimExpiresAt <= this.#nowMs()
      ) {
        throw new Error("stale workflow task lease");
      }
      if (row.terminal && workflowTaskCommitHasWorkflowVisibleMutations(commit)) {
        throw new Error("terminal workflow rejects workflow-visible mutations");
      }

      const currentTail = postgresRequiredNumber(row.tail_event_id);
      if (Number(commit.expectedTailEventId) !== currentTail) {
        await client.query(
          `
            update ${this.#workflowRunsTableName}
            set
              ready_reason = 'CacheEvicted',
              ready_at_ms = 0,
              claim_worker_id = null,
              claim_token = null,
              claim_reason = null,
              claim_expires_at_ms = null
            where run_id = $1
          `,
          [String(claim.runId)]
        );
        return { kind: "Conflict" };
      }

      const appendEvents = commit.appendEvents ?? [];
      const terminal = appendEvents.some((event) => workflowCommitEventClosesWorkflow(event.data));
      const childStarts = commit.startChildWorkflows ?? [];
      if (terminal && childStarts.length > 0) {
        return null;
      }
      const parentLink =
        row.parent === null ? null : (parsePostgresJson(row.parent) as ParentWorkflowLink);
      const terminalUpdate = childTerminalUpdateFromAppendEvents(appendEvents);
      if (terminal && parentLink?.kind === "ChildWorkflowMap") {
        return null;
      }
      if (
        terminal &&
        parentLink === null &&
        await this.#hasCancelableOpenChildren(client, claim.runId)
      ) {
        return null;
      }
      if (terminal && await this.#hasOpenMapDescriptors(client, claim.runId)) {
        return null;
      }

      let nextEventId = currentTail;
      const historyRows: NormalizedHistoryRow[] = appendEvents.map((event) => {
        nextEventId += 1;
        return normalizedHistoryRow(claim.runId, makeHistoryEvent(eventId(nextEventId), event.data));
      });
      let workflowReadyReason: WorkflowTaskReason | null = null;

      if (childStarts.length > 0) {
        const firstChildRun = await this.#allocateCounterRange(client, "run", childStarts.length);
        const childWorkflowRows: NormalizedWorkflowRunRow[] = [];
        for (const [index, child] of childStarts.entries()) {
          const childRunId = runId(`run-${firstChildRun + index}`);
          const insertedChildId = await client.query<{ readonly run_id: string }>(
            `
              insert into ${this.#workflowIdsTableName}(namespace, workflow_id, run_id)
              values ($1, $2, $3)
              on conflict (namespace, workflow_id) do nothing
              returning run_id
            `,
            [row.namespace, String(child.workflowId), String(childRunId)]
          );
          if (insertedChildId.rows.length === 0) {
            nextEventId += 1;
            historyRows.push(normalizedHistoryRow(claim.runId, makeHistoryEvent(eventId(nextEventId), {
              kind: "ChildWorkflowFailed",
              failed: {
                commandId: child.commandId,
                failure: {
                  errorType: "durust.child_workflow_id_conflict",
                  message: `child workflow id already exists: ${child.workflowId}`,
                  nonRetryable: true
                }
              }
            })));
            workflowReadyReason = "ChildWorkflowFailed";
            continue;
          }

          const childStarted = makeHistoryEvent(eventId(1), {
            kind: "WorkflowStarted",
            workflowType: child.workflowType,
            input: child.input
          });
          historyRows.push(normalizedHistoryRow(childRunId, childStarted));
          const childState: WorkflowState = {
            namespace: row.namespace,
            workflowId: String(child.workflowId),
            workflowType: child.workflowType,
            taskQueue: String(child.taskQueue),
            runId: childRunId,
            history: [childStarted],
            readyReason: "WorkflowStarted",
            readyAtMs: 0,
            claim: null,
            queryProjection: null,
            terminal: false,
            parent: {
              kind: "Child",
              parentRunId: claim.runId,
              commandId: child.commandId,
              parentClosePolicy: child.parentClosePolicy
            }
          };
          childWorkflowRows.push(normalizedWorkflowRunRow(childState));
          nextEventId += 1;
          historyRows.push(normalizedHistoryRow(claim.runId, makeHistoryEvent(eventId(nextEventId), {
            kind: "ChildWorkflowStarted",
            started: {
              commandId: child.commandId,
              workflowId: child.workflowId,
              runId: childRunId
            }
          })));
          workflowReadyReason = "ChildWorkflowStarted";
        }
        await this.#insertNormalizedWorkflowRows(client, childWorkflowRows);
      }

      const upsertWaits = commit.upsertWaits ?? [];
      if (upsertWaits.length > 0) {
        await this.#upsertNormalizedWaitRowsForNamespace(client, row.namespace, upsertWaits);
      }
      const deleteWaits = (commit.deleteWaits ?? []).map(String);
      if (deleteWaits.length > 0) {
        await client.query(
          `delete from ${this.#waitsTableName} where wait_id = any($1::text[])`,
          [deleteWaits]
        );
      }
      // Terminal cleanup's wait half on this path. It runs after the upsert
      // loop above on purpose: a commit that both registers a wait and closes
      // the run — a losing `select` branch whose winner settles in the same
      // task — must leave nothing behind, and cancelling before scheduling is
      // the inversion `cancelCommands` already had to be moved to avoid. The
      // loaded-state path reaches the same end through
      // `#abandonWorkForClosedRun`, whose deletions are persisted by the full
      // normalized rewrite a terminal commit takes.
      if (terminal) {
        await client.query(
          `delete from ${this.#waitsTableName} where run_id = $1`,
          [String(claim.runId)]
        );
      }
      const consumeSignals = (commit.consumeSignals ?? []).map(String);
      if (consumeSignals.length > 0) {
        await client.query(
          `update ${this.#signalsTableName} set consumed = true where signal_id = any($1::text[])`,
          [consumeSignals]
        );
      }

      const scheduleActivities = commit.scheduleActivities ?? [];
      if (scheduleActivities.length > 0) {
        await this.#insertNormalizedActivityTaskRows(
          client,
          scheduleActivities.map((task) => normalizedActivityTaskRowFromTask(row.namespace, task))
        );
      }

      if (commit.queryProjection !== undefined) {
        if (commit.queryProjection === null) {
          await client.query(
            `delete from ${this.#queryProjectionsTableName} where run_id = $1`,
            [String(claim.runId)]
          );
        } else {
          await client.query(
            `
              insert into ${this.#queryProjectionsTableName}(run_id, namespace, workflow_id, projection)
              values ($1, $2, $3, $4::jsonb)
              on conflict (run_id) do update
              set
                namespace = excluded.namespace,
                workflow_id = excluded.workflow_id,
                projection = excluded.projection
            `,
            [
              String(claim.runId),
              row.namespace,
              row.workflow_id,
              stringifyJson(commit.queryProjection)
            ]
          );
        }
      }

      await this.#appendNormalizedHistoryRows(client, historyRows);

      const signalReady = terminal ? false : await this.#hasReadySignalWait(client, claim.runId);
      await client.query(
        `
          update ${this.#workflowRunsTableName}
          set
            tail_event_id = $2::integer,
            ready_reason = $3,
            ready_at_ms = 0,
            claim_worker_id = null,
            claim_token = null,
            claim_reason = null,
            claim_expires_at_ms = null,
            query_projection = case when $4::boolean then $5::jsonb else query_projection end,
            terminal = $6::boolean
          where run_id = $1
        `,
        [
          String(claim.runId),
          String(nextEventId),
          signalReady ? "SignalReceived" : workflowReadyReason,
          commit.queryProjection !== undefined,
          commit.queryProjection === undefined || commit.queryProjection === null
            ? null
            : stringifyJson(commit.queryProjection),
          terminal || row.terminal
        ]
      );

      if (terminalUpdate !== null && parentLink?.kind === "Child") {
        const parent = await client.query<{
          readonly run_id: string;
          readonly tail_event_id: number | string;
          readonly terminal: boolean;
        }>(
          `
            select run_id, tail_event_id, terminal
            from ${this.#workflowRunsTableName}
            where run_id = $1
            for update
          `,
          [String(parentLink.parentRunId)]
        );
        const parentRow = parent.rows[0];
        if (parentRow !== undefined && !parentRow.terminal) {
          const parentTail = postgresRequiredNumber(parentRow.tail_event_id) + 1;
          await this.#appendNormalizedHistoryRows(client, [
            normalizedHistoryRow(parentLink.parentRunId, makeHistoryEvent(
              eventId(parentTail),
              childTerminalHistoryData(parentLink, terminalUpdate)
            ))
          ]);
          await this.#updateWorkflowTailsAndReasons(client, [{
            run_id: String(parentLink.parentRunId),
            tail_event_id: parentTail,
            ready_reason: childTerminalReadyReason(terminalUpdate)
          }]);
        }
      }
      return { kind: "Committed", newTailEventId: eventId(nextEventId) };
    });
  }

  async claimActivityTask(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    if (opts.registeredActivityNames.length === 0) {
      return null;
    }
    return this.#withSqlTransaction(async (client) => {
      const now = this.#nowMs();
      const selected = await client.query<NormalizedActivityTaskLoadRow>(
        `
          select activities.*
          from ${this.#activityTasksTableName} activities
          left join ${this.#activityMapsTableName} maps
            on maps.command_key = activities.map_command_key
          where activities.namespace = $1
            and activities.task_queue = $2
            and activities.activity_name = any($3::text[])
            and activities.terminal_event_id is null
            and activities.available_at_ms <= $4::bigint
            and (
              activities.claim_token is null
              or (
                activities.claim_expires_at_ms is not null
                and activities.claim_expires_at_ms <= $4::bigint
              )
            )
            and coalesce(maps.terminal, false) = false
          order by activities.available_at_ms asc, activities.activity_id asc
          limit 1
          for update of activities skip locked
        `,
        [
          String(opts.namespace),
          String(opts.taskQueue),
          opts.registeredActivityNames.map(String),
          String(now)
        ]
      );
      const row = selected.rows[0];
      if (row === undefined) {
        return null;
      }
      let task = parsePostgresJson(row.task) as ActivityTask;
      if (postgresOptionalNumber(row.claim_token) !== null) {
        const retry = retryActivityTaskAfterTimeout(task, now);
        if (retry !== null && retry.readyAtMs > now) {
          return null;
        }
        task = retry?.task ?? task;
      }
      const token = await this.#allocateCounterRange(client, "activity_claim", 1);
      const leaseDurationMs = Math.max(0, opts.leaseDurationMs);
      const heartbeatDeadline = activityHeartbeatDeadlineAt(task, now, leaseDurationMs);
      const timeoutDeadline = activityTimeoutDeadlineFromTask(task, now, heartbeatDeadline);
      const claim: ActivityTaskClaim = {
        activityId: row.activity_id,
        workerId,
        token
      };
      await client.query(
        `
          update ${this.#activityTasksTableName}
          set
            claim_worker_id = $2,
            claim_token = $3::bigint,
            claim_started_at_ms = $4::bigint,
            heartbeat_deadline_at_ms = $5::bigint,
            timeout_deadline_at_ms = $6::bigint,
            claim_expires_at_ms = $7::bigint,
            claim_lease_duration_ms = $8::bigint,
            task = $9::jsonb,
            available_at_ms = $4::bigint
          where activity_id = $1
        `,
        [
          row.activity_id,
          String(workerId),
          String(token),
          String(now),
          heartbeatDeadline === null ? null : String(heartbeatDeadline),
          timeoutDeadline === null ? null : String(timeoutDeadline),
          String(this.#leaseExpiresAt(opts.leaseDurationMs)),
          String(leaseDurationMs),
          stringifyJson(task)
        ]
      );
      return { task, claim };
    });
  }

  async claimActivityTasks(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions & { readonly limit: number }
  ): Promise<readonly ClaimedActivityTask[]> {
    if (opts.registeredActivityNames.length === 0) {
      return [];
    }
    const limit = Math.max(1, Math.trunc(opts.limit));
    return this.#withSqlTransaction(async (client) => {
      const now = this.#nowMs();
      const selected = await client.query<NormalizedActivityTaskLoadRow>(
        `
          select activities.*
          from ${this.#activityTasksTableName} activities
          left join ${this.#activityMapsTableName} maps
            on maps.command_key = activities.map_command_key
          where activities.namespace = $1
            and activities.task_queue = $2
            and activities.activity_name = any($3::text[])
            and activities.terminal_event_id is null
            and activities.available_at_ms <= $4::bigint
            and (
              activities.claim_token is null
              or (
                activities.claim_expires_at_ms is not null
                and activities.claim_expires_at_ms <= $4::bigint
              )
            )
            and coalesce(maps.terminal, false) = false
          order by activities.available_at_ms asc, activities.activity_id asc
          limit $5::bigint
          for update of activities skip locked
        `,
        [
          String(opts.namespace),
          String(opts.taskQueue),
          opts.registeredActivityNames.map(String),
          String(now),
          String(limit)
        ]
      );
      if (selected.rows.length === 0) {
        return [];
      }

      const firstToken = await this.#allocateCounterRange(
        client,
        "activity_claim",
        selected.rows.length
      );
      const leaseDurationMs = Math.max(0, opts.leaseDurationMs);
      const claimExpiresAt = this.#leaseExpiresAt(opts.leaseDurationMs);
      const claims = selected.rows.flatMap((row, index) => {
        let task = parsePostgresJson(row.task) as ActivityTask;
        if (postgresOptionalNumber(row.claim_token) !== null) {
          const retry = retryActivityTaskAfterTimeout(task, now);
          if (retry !== null && retry.readyAtMs > now) {
            return [];
          }
          task = retry?.task ?? task;
        }
        const token = firstToken + index;
        const heartbeatDeadline = activityHeartbeatDeadlineAt(task, now, leaseDurationMs);
        const timeoutDeadline = activityTimeoutDeadlineFromTask(task, now, heartbeatDeadline);
        return [{
          activityId: row.activity_id,
          task,
          claim: {
            activityId: row.activity_id,
            workerId,
            token
          } satisfies ActivityTaskClaim,
          heartbeatDeadline,
          timeoutDeadline,
          claimExpiresAt,
          leaseDurationMs
        }];
      });
      if (claims.length === 0) {
        return [];
      }

      await client.query(
        `
          with updates as (
            select *
            from jsonb_to_recordset($2::jsonb) as update_row(
              activity_id text,
              claim_token bigint,
              claim_started_at_ms bigint,
              heartbeat_deadline_at_ms bigint,
              timeout_deadline_at_ms bigint,
              claim_expires_at_ms bigint,
              claim_lease_duration_ms bigint,
              task jsonb
            )
          )
          update ${this.#activityTasksTableName} activities
          set
            claim_worker_id = $1,
            claim_token = updates.claim_token,
            claim_started_at_ms = updates.claim_started_at_ms,
            heartbeat_deadline_at_ms = updates.heartbeat_deadline_at_ms,
            timeout_deadline_at_ms = updates.timeout_deadline_at_ms,
            claim_expires_at_ms = updates.claim_expires_at_ms,
            claim_lease_duration_ms = updates.claim_lease_duration_ms,
            task = updates.task,
            available_at_ms = updates.claim_started_at_ms
          from updates
          where activities.activity_id = updates.activity_id
        `,
        [
          String(workerId),
          stringifyJson(
            claims.map((claim) => ({
              activity_id: claim.activityId,
              claim_token: claim.claim.token,
              claim_started_at_ms: now,
              heartbeat_deadline_at_ms: claim.heartbeatDeadline,
              timeout_deadline_at_ms: claim.timeoutDeadline,
              claim_expires_at_ms: claim.claimExpiresAt,
              claim_lease_duration_ms: claim.leaseDurationMs,
              task: claim.task
            }))
          )
        ]
      );

      return claims.map(({ task, claim }) => ({ task, claim }));
    });
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    const direct = await this.#completeActivitiesSqlNative({ completions: [req] });
    if (direct !== null) {
      const outcome = direct.results[0];
      if (outcome === undefined) {
        throw new Error("activity completion did not return a result");
      }
      if (outcome.kind === "NotFound") {
        throw new Error(`activity task not found: ${req.claim.activityId}`);
      }
      if (outcome.kind === "StaleLease") {
        throw new Error("stale activity task lease");
      }
      return outcome;
    }
    let rewriteScope = scalarActivityMutationRewriteScope;
    return this.#withState(async (client) => {
      const targetedUpdates: TargetedActivityProjectionUpdate[] = [];
      const outcome = this.#completeActivityItem(req, (update) => {
        if (update === "Full") {
          rewriteScope = fullNormalizedRewriteScope;
          return;
        }
        targetedUpdates.push(update);
      });
      if (outcome.kind === "NotFound") {
        throw new Error(`activity task not found: ${req.claim.activityId}`);
      }
      if (outcome.kind === "StaleLease") {
        throw new Error("stale activity task lease");
      }
      if (rewriteScope !== fullNormalizedRewriteScope) {
        await this.#applyTargetedActivityProjectionUpdates(client, targetedUpdates);
      }
      return outcome;
    }, () => rewriteScope);
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    const direct = await this.#completeActivitiesSqlNative(req);
    if (direct !== null) {
      return direct;
    }
    return this.#withState(() => ({
      results: req.completions.map((completion) => this.#completeActivityItem(completion))
    }));
  }

  async #completeActivitiesSqlNative(
    req: CompleteActivitiesRequest
  ): Promise<CompleteActivitiesOutcome | null> {
    if (req.completions.length === 0) {
      return { results: [] };
    }
    return this.#withSqlTransaction(async (client) => {
      const activityIds = [...new Set(req.completions.map((completion) => completion.claim.activityId))];
      const activityRows = await client.query<NormalizedActivityTaskLoadRow>(
        `
          select
            activity_id,
            namespace,
            run_id,
            task,
            input,
            activity_name,
            task_queue,
            available_at_ms,
            claim_worker_id,
            claim_token,
            claim_started_at_ms,
            heartbeat_deadline_at_ms,
            timeout_deadline_at_ms,
            claim_expires_at_ms,
            claim_lease_duration_ms,
            terminal_event_id,
            map_command_key,
            map_item_ordinal
          from ${this.#activityTasksTableName}
          where activity_id = any($1::text[])
          for update
        `,
        [activityIds]
      );
      if (activityRows.rows.some((row) => row.map_command_key !== null)) {
        return null;
      }

      const rowsById = new Map(activityRows.rows.map((row) => [row.activity_id, row]));
      const results: CompleteActivityItemOutcome[] = [];
      const valid: {
        readonly ordinal: number;
        readonly row: NormalizedActivityTaskLoadRow;
        readonly task: ActivityTask;
        readonly result: PayloadRef;
      }[] = [];
      const completedInBatch = new Set<string>();
      const now = this.#nowMs();
      for (const [ordinal, completion] of req.completions.entries()) {
        const row = rowsById.get(completion.claim.activityId);
        if (row === undefined) {
          results[ordinal] = { kind: "NotFound" };
          continue;
        }
        if (row.terminal_event_id !== null || completedInBatch.has(row.activity_id)) {
          results[ordinal] = { kind: "AlreadyCompleted" };
          continue;
        }
        const claimToken = postgresOptionalNumber(row.claim_token);
        const claimExpiresAt = postgresOptionalNumber(row.claim_expires_at_ms);
        if (
          claimToken === null ||
          claimToken !== completion.claim.token ||
          String(row.claim_worker_id ?? "") !== String(completion.claim.workerId) ||
          claimExpiresAt === null ||
          claimExpiresAt <= now
        ) {
          results[ordinal] = { kind: "StaleLease" };
          continue;
        }
        completedInBatch.add(row.activity_id);
        valid.push({
          ordinal,
          row,
          task: parsePostgresJson(row.task) as ActivityTask,
          result: completion.result
        });
      }
      if (valid.length === 0) {
        return { results };
      }

      const workflowIds = [...new Set(valid.map((item) => item.row.run_id))];
      const workflowRows = await client.query<{
        readonly run_id: string;
        readonly tail_event_id: number | string;
      }>(
        `
          select run_id, tail_event_id
          from ${this.#workflowRunsTableName}
          where run_id = any($1::text[])
          for update
        `,
        [workflowIds]
      );
      const workflowTails = new Map(
        workflowRows.rows.map((row) => [row.run_id, postgresRequiredNumber(row.tail_event_id)])
      );
      const historyRows: NormalizedHistoryRow[] = [];
      const activityUpdates: { readonly activity_id: string; readonly terminal_event_id: number }[] = [];
      const workflowUpdates = new Map<string, number>();

      for (const item of valid) {
        const previousTail = workflowTails.get(item.row.run_id);
        if (previousTail === undefined) {
          results[item.ordinal] = { kind: "NotFound" };
          continue;
        }
        const nextTail = previousTail + 1;
        workflowTails.set(item.row.run_id, nextTail);
        workflowUpdates.set(item.row.run_id, nextTail);
        const event = makeHistoryEvent(eventId(nextTail), {
          kind: "ActivityCompleted",
          completed: {
            commandId: item.task.commandId,
            result: item.result
          }
        });
        historyRows.push(normalizedHistoryRow(runId(item.row.run_id), event));
        activityUpdates.push({
          activity_id: item.row.activity_id,
          terminal_event_id: nextTail
        });
        results[item.ordinal] = { kind: "Completed", eventId: event.eventId };
      }

      await this.#appendNormalizedHistoryRows(client, historyRows);
      if (activityUpdates.length > 0) {
        await client.query(
          `
            update ${this.#activityTasksTableName} activities
            set
              terminal_event_id = updates.terminal_event_id,
              claim_worker_id = null,
              claim_token = null,
              claim_started_at_ms = null,
              heartbeat_deadline_at_ms = null,
              timeout_deadline_at_ms = null,
              claim_expires_at_ms = null,
              claim_lease_duration_ms = null
            from jsonb_to_recordset($1::jsonb) as updates(
              activity_id text,
              terminal_event_id integer
            )
            where activities.activity_id = updates.activity_id
          `,
          [stringifyJson(activityUpdates)]
        );
      }
      await this.#updateWorkflowTailsAndReasons(
        client,
        [...workflowUpdates.entries()].map(([runIdValue, tail]) => ({
          run_id: runIdValue,
          tail_event_id: tail,
          ready_reason: "ActivityCompleted"
        }))
      );

      return { results };
    });
  }

  #completeActivityItem(
    req: CompleteActivityRequest,
    recordProjectionUpdate?: (
      update: TargetedActivityProjectionUpdate | "Full"
    ) => void
  ): CompleteActivityItemOutcome {
    const activity = this.#activitiesById.get(req.claim.activityId);
    if (!activity) {
      return { kind: "NotFound" };
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      recordProjectionUpdate?.(
        activity.task.mapItem === null ? { activity, workflow: null } : "Full"
      );
      return { kind: "AlreadyCompleted" };
    }
    if (
      activity.claim === null ||
      !activityLeaseMatches(activity.claim, req.claim)
    ) {
      return { kind: "StaleLease" };
    }

    if (activity.task.mapItem !== null) {
      const eventId = this.#completeActivityMapItem(activity, req.result);
      recordProjectionUpdate?.("Full");
      return { kind: "Completed", eventId };
    }

    const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
      kind: "ActivityCompleted",
      completed: {
        commandId: activity.task.commandId,
        result: req.result
      }
    });
    activity.workflow.history.push(event);
    markWorkflowReady(activity.workflow, "ActivityCompleted");
    activity.terminalEventId = event.eventId;
    activity.claim = null;
    recordProjectionUpdate?.({ activity, workflow: activity.workflow });
    return { kind: "Completed", eventId: event.eventId };
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    let rewriteScope = scalarActivityMutationRewriteScope;
    return this.#withState(async (client) => {
      const targetedUpdates: TargetedActivityProjectionUpdate[] = [];
      const outcome = this.#failActivityItem(req, (update) => {
        if (update === "Full") {
          rewriteScope = fullNormalizedRewriteScope;
          return;
        }
        targetedUpdates.push(update);
      });
      if (rewriteScope !== fullNormalizedRewriteScope) {
        await this.#applyTargetedActivityProjectionUpdates(client, targetedUpdates);
      }
      return outcome;
    }, () => rewriteScope);
  }

  #failActivityItem(
    req: FailActivityRequest,
    recordProjectionUpdate?: (
      update: TargetedActivityProjectionUpdate | "Full"
    ) => void
  ): FailActivityOutcome {
    const activity = this.#activitiesById.get(req.claim.activityId);
    if (!activity) {
      throw new Error(`activity task not found: ${req.claim.activityId}`);
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      recordProjectionUpdate?.(
        activity.task.mapItem === null ? { activity, workflow: null } : "Full"
      );
      return { kind: "AlreadyCompleted" };
    }
    if (
      activity.claim === null ||
      !activityLeaseMatches(activity.claim, req.claim)
    ) {
      throw new Error("stale activity task lease");
    }

    // A map item takes the engine route for *both* verdicts. Deciding
    // retry-versus-exhaust here and only then dispatching to the map path
    // would leave the engine's `ScheduleItemRetry` with no producer, and would
    // reschedule an item onto a map that had already ended.
    if (activity.task.mapItem !== null) {
      const outcome = this.#failActivityMapItem(
        activity,
        req.failure,
        itemRetryDecision(activity.task.attempt, activity.task.retryPolicy, req.failure)
      );
      recordProjectionUpdate?.("Full");
      return outcome;
    }

    const retry = retryActivityAfterFailure(activity, req.failure, this.#nowMs());
    if (retry !== null) {
      activity.task = retry.task;
      activity.availableAtMs = retry.readyAtMs;
      activity.claim = null;
      recordProjectionUpdate?.({ activity, workflow: null });
      return {
        kind: "RetryScheduled",
        attempt: retry.task.attempt,
        readyAtMs: retry.readyAtMs
      };
    }

    const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
      kind: "ActivityFailed",
      failed: {
        commandId: activity.task.commandId,
        failure: req.failure
      }
    });
    activity.workflow.history.push(event);
    markWorkflowReady(activity.workflow, "ActivityFailed");
    activity.terminalEventId = event.eventId;
    activity.claim = null;
    recordProjectionUpdate?.({ activity, workflow: activity.workflow });
    return { kind: "Failed", eventId: event.eventId };
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    return this.#withState(async (client) => {
      const activity = this.#activitiesById.get(req.claim.activityId);
      if (!activity) {
        throw new Error(`activity task not found: ${req.claim.activityId}`);
      }
      this.#restoreExpiredActivityLease(activity);
      if (activity.terminalEventId !== null) {
        return { kind: "AlreadyCompleted" };
      }
      if (
        activity.claim === null ||
        !activityLeaseMatches(activity.claim, req.claim)
      ) {
        throw new Error("stale activity task lease");
      }
      const currentClaim = activity.claim;
      const now = this.#nowMs();
      activity.claim = {
        ...currentClaim,
        heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(
          activity.task,
          now,
          currentClaim.leaseDurationMs
        ),
        expiresAtMs: now + currentClaim.leaseDurationMs
      };
      await this.#upsertNormalizedActivityTaskRow(client, activity);
      return { kind: "Recorded" };
    }, activityTaskRewriteScope);
  }

  #leaseExpiresAt(leaseDurationMs: number): number {
    return this.#nowMs() + Math.max(0, leaseDurationMs);
  }

  #restoreExpiredWorkflowLease(state: WorkflowState): void {
    if (state.claim !== null && state.claim.expiresAtMs <= this.#nowMs()) {
      state.readyReason ??= state.claim.reason;
      state.readyAtMs = 0;
      state.claim = null;
    }
  }

  #restoreExpiredActivityLease(activity: ActivityState): void {
    if (activity.claim !== null && activity.claim.expiresAtMs <= this.#nowMs()) {
      const retry = retryActivityAfterTimeout(activity, this.#nowMs());
      if (retry === null) {
        activity.claim = null;
        return;
      }
      activity.task = retry.task;
      activity.availableAtMs = retry.readyAtMs;
      activity.claim = null;
    }
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    return this.#withSqlTransaction(async (client) => {
      const limit = Math.max(1, Math.trunc(req.limit));
      const due = await client.query<{
        readonly wait_id: string;
        readonly run_id: string;
        readonly command_id: unknown;
        readonly tail_event_id: number | string;
      }>(
        `
          select
            waits.wait_id,
            waits.run_id,
            waits.command_id,
            runs.tail_event_id
          from ${this.#waitsTableName} waits
          join ${this.#workflowRunsTableName} runs
            on runs.run_id = waits.run_id
           and runs.namespace = waits.namespace
          where waits.namespace = $1
            and waits.kind = 'Timer'
            and waits.ready_at_ms is not null
            and waits.ready_at_ms <= $2::bigint
            -- A closed run's history is finished. Firing a stray wait against
            -- it appends TimerFired past the terminal event, which every
            -- replay, audit and terminal-cleanup assumption rests on not
            -- happening. Expressed as a predicate rather than as a per-row
            -- skip so the row is never selected, and therefore never included
            -- in the delete below. Every Rust provider reaches the same end by
            -- the other route, a per-row skip that leaves the row alone; the
            -- trade is recorded in PARITY.md note 22, because the predicate
            -- also keeps a leftover out of the scan's budget and so makes this
            -- provider blind to a terminal cleanup that stopped deleting waits.
            and runs.terminal = false
          order by waits.ready_at_ms asc, waits.wait_id asc
          limit $3::bigint
          for update of waits, runs skip locked
        `,
        [String(req.namespace), String(Number(req.now)), String(limit)]
      );
      if (due.rows.length === 0) {
        return { fired: 0 };
      }

      const tails = new Map<string, number>();
      const historyRows: NormalizedHistoryRow[] = [];
      const workflowUpdates = new Map<string, number>();
      for (const row of due.rows) {
        const previousTail = tails.get(row.run_id) ?? postgresRequiredNumber(row.tail_event_id);
        const nextTail = previousTail + 1;
        tails.set(row.run_id, nextTail);
        workflowUpdates.set(row.run_id, nextTail);
        const commandIdValue = parsePostgresJson(row.command_id) as CommandId;
        historyRows.push(normalizedHistoryRow(runId(row.run_id), makeHistoryEvent(eventId(nextTail), {
          kind: "TimerFired",
          fired: {
            commandId: commandIdValue,
            firedAt: req.now as TimestampMs
          }
        })));
      }

      await this.#appendNormalizedHistoryRows(client, historyRows);
      await client.query(
        `delete from ${this.#waitsTableName} where wait_id = any($1::text[])`,
        [due.rows.map((row) => row.wait_id)]
      );
      await this.#updateWorkflowTailsAndReasons(
        client,
        [...workflowUpdates.entries()].map(([runIdValue, tail]) => ({
          run_id: runIdValue,
          tail_event_id: tail,
          ready_reason: "TimerFired"
        }))
      );
      return { fired: due.rows.length };
    });
  }

  async timeoutDueActivities(req: TimeoutDueActivitiesRequest): Promise<TimeoutDueActivitiesOutcome> {
    let rewriteScope = activityTimeoutRewriteScope;
    return this.#withState(async (client) => {
      const due = await this.#selectTimedOutActivityIds(client, req);
      let timedOut = 0;
      for (const activityId of due) {
        const activity = this.#activitiesById.get(activityId);
        if (
          activity === undefined ||
          activity.namespace !== String(req.namespace) ||
          activity.claim === null ||
          activity.terminalEventId !== null
        ) {
          continue;
        }
        const timeout = activityTimeoutDeadline(activity);
        if (timeout.deadline > Number(req.now)) {
          continue;
        }
        // A map item's lapsed deadline is an engine event, not a local
        // reschedule: only the engine knows whether the map is still running,
        // and it owns both the retried attempt and the map's terminal failure.
        // Its effects reach descriptor and item rows, which the narrow timeout
        // rewrite scope does not cover, so the scope widens for this commit.
        if (activity.task.mapItem !== null) {
          rewriteScope = fullNormalizedRewriteScope;
          this.#failActivityMapItem(
            activity,
            activityTimeoutFailure(activity, timeout.kind),
            // A lapsed deadline is not the attempt's own verdict, so the
            // policy's non-retryable rules do not apply to it.
            itemRetryDecision(activity.task.attempt, activity.task.retryPolicy, null),
            "TimedOut"
          );
          timedOut += 1;
          continue;
        }
        const retry = retryActivityAfterTimeout(activity, Number(req.now));
        if (retry !== null) {
          activity.task = retry.task;
          activity.availableAtMs = retry.readyAtMs;
          activity.claim = null;
          timedOut += 1;
          continue;
        }
        const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
          kind: "ActivityTimedOut",
          timedOut: {
            commandId: activity.task.commandId,
            message: activityTimeoutMessage(activity, timeout.kind)
          }
        });
        activity.workflow.history.push(event);
        markWorkflowReady(activity.workflow, "ActivityTimedOut");
        activity.terminalEventId = event.eventId;
        activity.claim = null;
        timedOut += 1;
      }
      return { timedOut };
    }, () => rewriteScope);
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    return this.#withSqlTransaction(async (client) => {
      const signalKey = String(req.signalId);
      const existing = await client.query(
        `select 1 from ${this.#signalsTableName} where signal_id = $1 limit 1`,
        [signalKey]
      );
      if (existing.rows.length > 0) {
        return { kind: "Duplicate" };
      }

      const selectedRunId = await this.#selectCurrentWorkflowRunId(
        client,
        req.namespace,
        req.workflowId
      );
      if (selectedRunId === null) {
        throw new Error(`workflow not found: ${req.workflowId}`);
      }
      const workflow = await client.query<{ readonly run_id: string }>(
        `
          select run_id
          from ${this.#workflowRunsTableName}
          where run_id = $1
            and namespace = $2
            and workflow_id = $3
          for update
        `,
        [selectedRunId, String(req.namespace), String(req.workflowId)]
      );
      if (workflow.rows[0] === undefined) {
        throw new Error(`workflow not found: ${req.workflowId}`);
      }

      const sequence = await this.#allocateCounterRange(client, "signal_sequence", 1);
      await client.query(
        `
          insert into ${this.#signalsTableName}(
            signal_id,
            run_id,
            signal_name,
            payload,
            received_sequence,
            consumed
          )
          values ($1, $2, $3, $4::jsonb, $5::bigint, false)
        `,
        [
          signalKey,
          selectedRunId,
          String(req.signalName),
          stringifyJson(req.payload),
          String(sequence)
        ]
      );
      if (await this.#hasReadySignalWait(client, runId(selectedRunId))) {
        await client.query(
          `
            update ${this.#workflowRunsTableName}
            set
              ready_reason = 'SignalReceived',
              ready_at_ms = 0
            where run_id = $1
              and terminal = false
          `,
          [selectedRunId]
        );
      }
      return { kind: "Accepted" };
    });
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    await this.#ensureReady();
    const result = await this.#pool.query<NormalizedSignalSelectRow>(
      `
        select signal_id, signal_name, payload
        from ${this.#signalsTableName}
        where run_id = $1
          and signal_name = $2
          and consumed = false
        order by received_sequence asc, signal_id asc
        limit 1
      `,
      [String(req.runId), String(req.signalName)]
    );
    const row = result.rows[0];
    if (row === undefined) {
      return null;
    }
    return {
      signalId: row.signal_id,
      signalName: row.signal_name,
      payload: parseJson<PayloadRef>(JSON.stringify(row.payload))
    };
  }

  async #readSignalInboxesForClaims(
    client: PoolClient,
    runIds: readonly RunId[],
    signalNames: readonly (SignalName | string)[]
  ): Promise<ReadonlyMap<string, readonly SignalInboxRecord[]>> {
    const orderedRunIds = [...new Set(runIds.map(String))];
    const orderedSignalNames = [...new Set(signalNames.map(String))];
    const recordsByRun = new Map<string, SignalInboxRecord[]>(
      orderedRunIds.map((runIdValue) => [runIdValue, []])
    );
    if (orderedRunIds.length === 0 || orderedSignalNames.length === 0) {
      return recordsByRun;
    }
    const result = await client.query<NormalizedClaimSignalSelectRow>(
      `
        select distinct on (run_id, signal_name)
          run_id,
          signal_id,
          signal_name,
          payload
        from ${this.#signalsTableName}
        where run_id = any($1::text[])
          and signal_name = any($2::text[])
          and consumed = false
        order by run_id asc, signal_name asc, received_sequence asc, signal_id asc
      `,
      [orderedRunIds, orderedSignalNames]
    );
    const byRunThenSignal = new Map<string, Map<string, SignalInboxRecord>>();
    for (const row of result.rows) {
      const bySignal = byRunThenSignal.get(row.run_id) ?? new Map<string, SignalInboxRecord>();
      bySignal.set(row.signal_name, {
        signalId: row.signal_id,
        signalName: row.signal_name,
        payload: parseJson<PayloadRef>(JSON.stringify(row.payload))
      });
      byRunThenSignal.set(row.run_id, bySignal);
    }
    for (const runIdValue of orderedRunIds) {
      const bySignal = byRunThenSignal.get(runIdValue);
      recordsByRun.set(
        runIdValue,
        orderedSignalNames.flatMap((signalName) => {
          const record = bySignal?.get(signalName);
          return record === undefined ? [] : [record];
        })
      );
    }
    return recordsByRun;
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    await this.#ensureReady();
    const result = await this.#pool.query<NormalizedQueryProjectionSelectRow>(
      `
        select ids.run_id, projections.projection
        from ${this.#workflowIdsTableName} ids
        left join ${this.#queryProjectionsTableName} projections on projections.run_id = ids.run_id
        where ids.namespace = $1 and ids.workflow_id = $2
        limit 1
      `,
      [String(req.namespace), String(req.workflowId)]
    );
    const row = result.rows[0];
    if (row === undefined) {
      return { kind: "NotFound" };
    }
    if (row.projection === null) {
      return { kind: "NoProjection" };
    }
    return {
      kind: "Found",
      projection: parseJson<PayloadRef>(JSON.stringify(row.projection))
    };
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    await this.#ensureReady();
    const client = await this.#pool.connect();
    try {
      const history = await client.query<NormalizedPayloadRootHistoryRow>(
        `select data from ${this.#historyTableName}`
      );
      const queryProjections = await client.query<NormalizedPayloadRootQueryProjectionRow>(
        `select projection from ${this.#queryProjectionsTableName}`
      );
      const activities = await client.query<NormalizedPayloadRootActivityTaskRow>(
        `select input from ${this.#activityTasksTableName}
         where input is not null`
      );
      const signals = await client.query<NormalizedPayloadRootSignalRow>(
        `select payload from ${this.#signalsTableName}`
      );
      const activityMaps = await client.query<NormalizedPayloadRootActivityMapRow>(
        `select input_manifest from ${this.#activityMapsTableName}`
      );
      const activityMapItems = await client.query<NormalizedPayloadRootActivityMapItemRow>(
        `select input, result from ${this.#activityMapItemsTableName}`
      );
      const childWorkflowMaps = await client.query<NormalizedPayloadRootChildWorkflowMapRow>(
        `select input_manifest from ${this.#childWorkflowMapsTableName}`
      );
      const childWorkflowMapItems =
        await client.query<NormalizedPayloadRootChildWorkflowMapItemRow>(
          `select input, outcome from ${this.#childWorkflowMapItemsTableName}`
      );

      return [
        ...history.rows.map((row) => parsePostgresJson(row.data)),
        ...queryProjections.rows.map((row) => parsePostgresJson(row.projection)),
        ...activities.rows.map((row) => parsePostgresJson(row.input)),
        ...signals.rows.map((row) => parsePostgresJson(row.payload)),
        ...activityMaps.rows.map((row) => parsePostgresJson(row.input_manifest)),
        ...activityMapItems.rows.flatMap((row) => [
          parsePostgresJson(row.input),
          parsePostgresJson(row.result)
        ]),
        ...childWorkflowMaps.rows.map((row) => parsePostgresJson(row.input_manifest)),
        ...childWorkflowMapItems.rows.flatMap((row) => [
          parsePostgresJson(row.input),
          parsePostgresJson(row.outcome)
        ])
      ];
    } finally {
      client.release();
    }
  }

  async #initialize(): Promise<void> {
    this.#statementStatsSetupFailure = await ensureOptionalPostgresStatementStats(this.#pool);
    await this.#pool.query(`
      create table if not exists ${this.#countersTableName}(
        name text primary key,
        next_value bigint not null
      )
    `);
    await this.#pool.query(
      `insert into ${this.#countersTableName}(name, next_value)
       values
         ('run', 1),
         ('workflow_claim', 1),
         ('activity_claim', 1),
         ('signal_sequence', 1)
       on conflict (name) do nothing`
    );
    await this.#pool.query(`
      create table if not exists ${this.#historyTableName}(
        run_id text not null,
        event_id integer not null,
        event_type text not null,
        data jsonb not null,
        primary key (run_id, event_id)
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#workflowIdsTableName}(
        namespace text not null,
        workflow_id text not null,
        run_id text not null,
        primary key(namespace, workflow_id)
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#workflowRunsTableName}(
        run_id text primary key,
        namespace text not null,
        workflow_id text not null,
        workflow_type_name text not null,
        workflow_type_version integer not null,
        workflow_type jsonb not null,
        task_queue text not null,
        tail_event_id integer not null,
        ready_reason text,
        ready_at_ms bigint not null default 0,
        claim_worker_id text,
        claim_token bigint,
        claim_reason text,
        claim_expires_at_ms bigint,
        query_projection jsonb,
        terminal boolean not null,
        parent jsonb
      )
    `);
    await this.#pool.query(
      `alter table ${this.#workflowRunsTableName}
       add column if not exists ready_at_ms bigint not null default 0`
    );
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "workflow_runs_ready_idx")}
      on ${this.#workflowRunsTableName}(
        namespace,
        task_queue,
        terminal,
        ready_reason,
        ready_at_ms,
        workflow_type_name,
        workflow_type_version
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "workflow_runs_unclaimed_ready_idx")}
      on ${this.#workflowRunsTableName}(
        namespace,
        task_queue,
        workflow_type_name,
        workflow_type_version,
        ready_at_ms,
        run_id
      )
      where terminal = false
        and ready_reason is not null
        and claim_token is null
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "workflow_runs_expired_claim_idx")}
      on ${this.#workflowRunsTableName}(
        namespace,
        task_queue,
        workflow_type_name,
        workflow_type_version,
        claim_expires_at_ms,
        run_id
      )
      where terminal = false
        and claim_token is not null
        and claim_reason is not null
        and claim_expires_at_ms is not null
    `);
    await this.#pool.query(`
      create table if not exists ${this.#queryProjectionsTableName}(
        run_id text primary key,
        namespace text not null,
        workflow_id text not null,
        projection jsonb not null
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "query_projections_lookup_idx")}
      on ${this.#queryProjectionsTableName}(
        namespace,
        workflow_id,
        run_id
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#waitsTableName}(
        wait_id text primary key,
        run_id text not null,
        namespace text not null,
        command_id jsonb not null,
        kind text not null,
        key text not null,
        ready_at_ms bigint
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "waits_due_timer_idx")}
      on ${this.#waitsTableName}(
        namespace,
        kind,
        ready_at_ms,
        wait_id
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "waits_due_timer_partial_idx")}
      on ${this.#waitsTableName}(
        namespace,
        ready_at_ms,
        wait_id
      )
      where kind = 'Timer'
        and ready_at_ms is not null
    `);
    await this.#pool.query(`
      create table if not exists ${this.#signalsTableName}(
        signal_id text primary key,
        run_id text not null,
        signal_name text not null,
        payload jsonb not null,
        received_sequence bigint not null,
        consumed boolean not null
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "signals_inbox_idx")}
      on ${this.#signalsTableName}(
        run_id,
        signal_name,
        consumed,
        received_sequence
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "signals_unconsumed_inbox_idx")}
      on ${this.#signalsTableName}(
        run_id,
        signal_name,
        received_sequence,
        signal_id
      )
      where consumed = false
    `);
    await this.#pool.query(`
      create table if not exists ${this.#activityTasksTableName}(
        activity_id text primary key,
        namespace text not null,
        run_id text not null,
        task jsonb not null,
        input jsonb,
        activity_name text not null,
        task_queue text not null,
        available_at_ms bigint not null,
        claim_worker_id text,
        claim_token bigint,
        claim_started_at_ms bigint,
        heartbeat_deadline_at_ms bigint,
        timeout_deadline_at_ms bigint,
        claim_expires_at_ms bigint,
        claim_lease_duration_ms bigint,
        terminal_event_id integer,
        map_command_key text,
        map_item_ordinal integer
      )
    `);
    await this.#pool.query(`
      alter table ${this.#activityTasksTableName}
      add column if not exists claim_lease_duration_ms bigint
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_tasks_claim_idx")}
      on ${this.#activityTasksTableName}(
        namespace,
        task_queue,
        activity_name,
        terminal_event_id,
        available_at_ms,
        activity_id
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_tasks_unclaimed_claim_idx")}
      on ${this.#activityTasksTableName}(
        namespace,
        task_queue,
        activity_name,
        available_at_ms,
        activity_id
      )
      where terminal_event_id is null
        and claim_token is null
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_tasks_expired_claim_idx")}
      on ${this.#activityTasksTableName}(
        namespace,
        task_queue,
        activity_name,
        claim_expires_at_ms,
        available_at_ms,
        activity_id
      )
      where terminal_event_id is null
        and claim_token is not null
        and claim_expires_at_ms is not null
    `);
    // The timeout scanner used to skip map items, so its index excluded them
    // too (`... and map_command_key is null`). Now that a map item's lapsed
    // deadline is an engine event the predicate has to cover them, and a
    // partial index's predicate cannot be altered in place — `create index if
    // not exists` under the old name would silently keep the narrower one and
    // leave the scan unindexed. So the index is *renamed*.
    //
    // The superseded index is deliberately **not dropped**. Dropping it makes
    // the migration destructive under a rolling deploy: old pods still run
    // `create index if not exists <old name>` while new pods drop it, so the
    // index flaps, and every recreation takes a build-length lock on the
    // hottest table in the schema. Additive is the only shape that is safe to
    // run concurrently with the previous version. What is left behind is one
    // redundant partial index costing write maintenance; an operator can drop
    // it out of band once no old pod remains.
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_tasks_timeout_due_all_idx")}
      on ${this.#activityTasksTableName}(
        namespace,
        timeout_deadline_at_ms,
        activity_id
      )
      where terminal_event_id is null
        and claim_token is not null
        and timeout_deadline_at_ms is not null
    `);
    await this.#pool.query(`
      create table if not exists ${this.#activityMapsTableName}(
        command_key text primary key,
        namespace text not null,
        run_id text not null,
        task jsonb not null,
        input_manifest jsonb,
        input_count integer not null,
        inputs jsonb not null,
        results jsonb not null,
        in_flight jsonb not null,
        next_ordinal integer not null,
        terminal boolean not null
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_maps_run_idx")}
      on ${this.#activityMapsTableName}(
        namespace,
        run_id,
        terminal
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#activityMapItemsTableName}(
        command_key text not null,
        namespace text not null,
        run_id text not null,
        item_ordinal integer not null,
        input jsonb not null,
        result jsonb,
        in_flight boolean not null,
        terminal boolean not null,
        primary key(command_key, item_ordinal)
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "activity_map_items_run_idx")}
      on ${this.#activityMapItemsTableName}(
        namespace,
        run_id,
        command_key,
        item_ordinal
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#childWorkflowMapsTableName}(
        command_key text primary key,
        namespace text not null,
        run_id text not null,
        task jsonb not null,
        input_manifest jsonb,
        input_count integer not null,
        inputs jsonb not null,
        outcomes jsonb not null,
        in_flight jsonb not null,
        next_ordinal integer not null,
        terminal boolean not null
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "child_workflow_maps_run_idx")}
      on ${this.#childWorkflowMapsTableName}(
        namespace,
        run_id,
        terminal
      )
    `);
    await this.#pool.query(`
      create table if not exists ${this.#childWorkflowMapItemsTableName}(
        command_key text not null,
        namespace text not null,
        run_id text not null,
        item_ordinal integer not null,
        input jsonb not null,
        outcome jsonb,
        in_flight boolean not null,
        terminal boolean not null,
        primary key(command_key, item_ordinal)
      )
    `);
    await this.#pool.query(`
      create index if not exists ${derivedSqlIdentifier(this.#rawTableName, "child_workflow_map_items_run_idx")}
      on ${this.#childWorkflowMapItemsTableName}(
        namespace,
        run_id,
        command_key,
        item_ordinal
      )
    `);
  }

  async #ensureReady(): Promise<void> {
    this.#ready ??= this.#initialize().then(() => this.#repairWorkOfTerminalRuns());
    await this.#ready;
  }

  /**
   * One-time upgrade repair: abandon every scrap of live work still attached to
   * a run that is already closed — a map descriptor that never went terminal,
   * and any pending plain activity of the same run.
   *
   * No code path can create that state any more; every transition that closes a
   * run abandons its work in the same transaction. But a database written
   * before that landed can hold it, and for a map descriptor it is
   * *unrecoverable*: every item-terminal path asks the engine, gets
   * `TerminalParent`, and raises, so `completeActivity`, `failActivity` and
   * `timeoutDueActivities` all fail permanently for that map. The scanner is
   * the damaging one — it processes a batch as one unit, so a single poisoned
   * map item stops *every* activity in the namespace from ever timing out
   * again. Before the guard the same call appended past the run's terminal
   * event, which was wrong but at least made progress.
   *
   * The probe is read-only and runs first, so a database with nothing to repair
   * never pays for the full-state load `#withState` performs. Nothing can add
   * to the probe's answer between the two statements, and the repair is
   * idempotent regardless.
   */
  async #repairWorkOfTerminalRuns(): Promise<void> {
    const orphaned = await this.#pool.query<{ readonly run_id: string }>(
      `
        select runs.run_id as run_id
        from ${this.#workflowRunsTableName} runs
        where runs.terminal = true
          and (
            exists (
              select 1 from ${this.#activityMapsTableName} m
              where m.run_id = runs.run_id and m.terminal = false
            )
            or exists (
              select 1 from ${this.#childWorkflowMapsTableName} m
              where m.run_id = runs.run_id and m.terminal = false
            )
            or exists (
              select 1 from ${this.#activityTasksTableName} a
              where a.run_id = runs.run_id
                and a.map_command_key is null
                and a.terminal_event_id is null
            )
          )
      `
    );
    if (orphaned.rows.length === 0) {
      return;
    }
    const runIds = new Set(orphaned.rows.map((row) => row.run_id));
    await this.#withLoadedState(() => {
      for (const runIdValue of runIds) {
        const state = this.#workflowsByRun.get(runIdValue);
        if (state !== undefined) {
          this.#abandonWorkForClosedRun(state);
        }
      }
    });
  }

  async #withSqlTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    await this.#ensureReady();
    const client = await this.#pool.connect();
    try {
      await client.query("begin");
      const result = await fn(client);
      await client.query("commit");
      return result;
    } catch (error) {
      await client.query("rollback").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async #allocateCounterRange(
    client: PoolClient,
    name: string,
    count: number
  ): Promise<number> {
    if (count <= 0) {
      throw new Error("counter range count must be positive");
    }
    const result = await client.query<{ readonly first_value: number | string }>(
      `
        update ${this.#countersTableName}
        set next_value = next_value + $2::bigint
        where name = $1
        returning next_value - $2::bigint as first_value
      `,
      [name, String(count)]
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`counter not initialized: ${name}`);
    }
    return postgresRequiredNumber(row.first_value);
  }

  async #withState<T>(
    fn: (client: PoolClient) => T | Promise<T>,
    rewriteScope: NormalizedRewriteScopeSelector<T> = fullNormalizedRewriteScope
  ): Promise<T> {
    await this.#ensureReady();
    return this.#withLoadedState(fn, rewriteScope);
  }

  /**
   * `#withState` without the readiness await, for the one caller that runs
   * *inside* the readiness chain.
   *
   * The upgrade repair is part of `#ready`, so calling `#withState` from it
   * awaits the promise it is itself resolving and deadlocks — silently, because
   * nothing rejects. That is not hypothetical: the split exists because the
   * Postgres repair shipped with the deadlock in it and no test reached the
   * path, which is the whole reason the repair now has a test per provider
   * rather than one.
   */
  async #withLoadedState<T>(
    fn: (client: PoolClient) => T | Promise<T>,
    rewriteScope: NormalizedRewriteScopeSelector<T> = fullNormalizedRewriteScope
  ): Promise<T> {
    const client = await this.#pool.connect();
    try {
      await client.query("begin");
      await this.#loadNormalizedState(client);
      const historyTailsBefore = this.#historyTailsByRun();
      const result = await fn(client);
      const selectedRewriteScope =
        typeof rewriteScope === "function" ? rewriteScope(result) : rewriteScope;
      if (selectedRewriteScope.history) {
        await this.#appendNormalizedHistoryRows(
          client,
          this.#newNormalizedHistoryRowsAfter(historyTailsBefore)
        );
      }
      if (selectedRewriteScope.workflowIds) {
        await this.#replaceNormalizedWorkflowIdRows(client);
      }
      if (selectedRewriteScope.workflows) {
        await this.#replaceNormalizedWorkflowRows(client);
      }
      if (selectedRewriteScope.queryProjections) {
        await this.#replaceNormalizedQueryProjectionRows(client);
      }
      if (selectedRewriteScope.waits) {
        await this.#replaceNormalizedWaitRows(client);
      }
      if (selectedRewriteScope.signals) {
        await this.#replaceNormalizedSignalRows(client);
      }
      if (selectedRewriteScope.activityTasks) {
        await this.#replaceNormalizedActivityTaskRows(client);
      }
      if (selectedRewriteScope.mapStates) {
        await this.#replaceNormalizedActivityMapRows(client);
        await this.#replaceNormalizedActivityMapItemRows(client);
        await this.#replaceNormalizedChildWorkflowMapRows(client);
        await this.#replaceNormalizedChildWorkflowMapItemRows(client);
      }
      await this.#writeNormalizedCounters(client);
      await client.query("commit");
      return result;
    } catch (error) {
      await client.query("rollback").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async #loadNormalizedState(client: PoolClient): Promise<void> {
    this.#workflowsById.clear();
    this.#workflowsByRun.clear();
    this.#activitiesById.clear();
    this.#activityMapsByCommand.clear();
    this.#childWorkflowMapsByCommand.clear();
    this.#waitsById.clear();
    this.#signalsById.clear();

    const counters = await client.query<NormalizedCounterRow>(
      `select name, next_value from ${this.#countersTableName} for update`
    );
    for (const row of counters.rows) {
      const value = postgresRequiredNumber(row.next_value);
      switch (row.name) {
        case "run":
          this.#nextRun = value;
          break;
        case "workflow_claim":
          this.#nextClaimToken = value;
          break;
        case "activity_claim":
          this.#nextActivityClaimToken = value;
          break;
        case "signal_sequence":
          this.#nextSignalSequence = value;
          break;
        default:
          break;
      }
    }

    const histories = await client.query<NormalizedHistorySelectRow & { readonly run_id: string }>(
      `
        select run_id, event_id, event_type, data
        from ${this.#historyTableName}
        order by run_id asc, event_id asc
      `
    );
    const historiesByRun = new Map<string, HistoryEvent[]>();
    for (const row of histories.rows) {
      const events = historiesByRun.get(row.run_id) ?? [];
      events.push(historyEventFromNormalizedRow(row));
      historiesByRun.set(row.run_id, events);
    }

    const workflows = await client.query<NormalizedWorkflowRunLoadRow>(
      `
        select
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
            ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        from ${this.#workflowRunsTableName}
        order by run_id asc
      `
    );
    for (const row of workflows.rows) {
      const claimToken = postgresOptionalNumber(row.claim_token);
      const claim =
        claimToken === null
          ? null
          : {
              claim: {
                runId: runId(row.run_id),
                workerId: String(row.claim_worker_id ?? ""),
                token: claimToken
              },
              reason: row.claim_reason as WorkflowTaskReason,
              expiresAtMs: postgresRequiredNumber(row.claim_expires_at_ms)
            };
      const state: WorkflowState = {
        namespace: row.namespace,
        workflowId: row.workflow_id,
        workflowType: parsePostgresJson(row.workflow_type) as WorkflowType,
        taskQueue: row.task_queue,
        runId: runId(row.run_id),
        history: historiesByRun.get(row.run_id) ?? [],
        readyReason: row.ready_reason as WorkflowTaskReason | null,
            readyAtMs: postgresRequiredNumber(row.ready_at_ms),
        claim,
        queryProjection:
          row.query_projection === null
            ? null
            : (parsePostgresJson(row.query_projection) as PayloadRef),
        terminal: row.terminal,
        parent:
          row.parent === null
            ? null
            : (parsePostgresJson(row.parent) as ParentWorkflowLink)
      };
      this.#workflowsByRun.set(String(state.runId), state);
    }

    const workflowIds = await client.query<{
      readonly namespace: string;
      readonly workflow_id: string;
      readonly run_id: string;
    }>(
      `select namespace, workflow_id, run_id from ${this.#workflowIdsTableName}`
    );
    for (const row of workflowIds.rows) {
      const workflow = this.#workflowsByRun.get(row.run_id);
      if (workflow !== undefined) {
        this.#workflowsById.set(workflowKey(row.namespace, row.workflow_id), workflow);
      }
    }
    for (const workflow of this.#workflowsByRun.values()) {
      const key = workflowKey(workflow.namespace, workflow.workflowId);
      if (!this.#workflowsById.has(key)) {
        this.#workflowsById.set(key, workflow);
      }
    }

    const waits = await client.query<NormalizedWaitLoadRow>(
      `
        select wait_id, run_id, namespace, command_id, kind, key, ready_at_ms
        from ${this.#waitsTableName}
      `
    );
    for (const row of waits.rows) {
      this.#waitsById.set(row.wait_id, {
        waitId: row.wait_id,
        runId: runId(row.run_id),
        commandId: parsePostgresJson(row.command_id) as CommandId,
        kind: row.kind,
        key: row.key,
        readyAt:
          row.ready_at_ms === null ? null : (postgresRequiredNumber(row.ready_at_ms) as TimestampMs)
      });
    }

    const signals = await client.query<NormalizedSignalLoadRow>(
      `
        select signal_id, run_id, signal_name, payload, received_sequence, consumed
        from ${this.#signalsTableName}
      `
    );
    for (const row of signals.rows) {
      this.#signalsById.set(row.signal_id, {
        runId: runId(row.run_id),
        signalName: row.signal_name,
        payload: parsePostgresJson(row.payload) as PayloadRef,
        receivedSequence: postgresRequiredNumber(row.received_sequence),
        consumed: row.consumed
      });
    }

    const activities = await client.query<NormalizedActivityTaskLoadRow>(
      `
        select
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        from ${this.#activityTasksTableName}
      `
    );
    for (const row of activities.rows) {
      const workflow = this.#workflowsByRun.get(row.run_id);
      if (workflow === undefined) {
        continue;
      }
      const claimToken = postgresOptionalNumber(row.claim_token);
      this.#activitiesById.set(row.activity_id, {
        namespace: row.namespace,
        workflow,
        task: parsePostgresJson(row.task) as ActivityTask,
        claim:
          claimToken === null
            ? null
            : {
                claim: {
                  activityId: row.activity_id,
                  workerId: String(row.claim_worker_id ?? ""),
                  token: claimToken
                },
                startedAtMs: postgresRequiredNumber(row.claim_started_at_ms),
                heartbeatDeadlineAtMs: postgresOptionalNumber(row.heartbeat_deadline_at_ms),
                expiresAtMs: postgresRequiredNumber(row.claim_expires_at_ms),
                leaseDurationMs:
                  postgresOptionalNumber(row.claim_lease_duration_ms) ??
                  Math.max(
                    0,
                    postgresRequiredNumber(row.claim_expires_at_ms) -
                      postgresRequiredNumber(row.claim_started_at_ms)
                  )
              },
        availableAtMs: postgresRequiredNumber(row.available_at_ms),
        terminalEventId:
          row.terminal_event_id === null
            ? null
            : eventId(postgresRequiredNumber(row.terminal_event_id))
      });
    }

    const activityMaps = await client.query<NormalizedActivityMapRow>(
      `
        select
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          results,
          in_flight,
          next_ordinal,
          terminal
        from ${this.#activityMapsTableName}
      `
    );
    for (const row of activityMaps.rows) {
      const workflow = this.#workflowsByRun.get(row.run_id);
      if (workflow === undefined) {
        continue;
      }
      this.#activityMapsByCommand.set(row.command_key, {
        namespace: row.namespace,
        workflow,
        task: parsePostgresJson(row.task) as ActivityMapTask,
        inputs: parsePostgresJson(row.inputs) as PayloadRef[],
        results: parsePostgresJson(row.results) as (PayloadRef | null)[],
        inFlight: parseInFlight(parsePostgresJson(row.in_flight)),
        nextOrdinal: row.next_ordinal,
        terminal: row.terminal
      });
    }

    const childWorkflowMaps = await client.query<NormalizedChildWorkflowMapRow>(
      `
        select
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          outcomes,
          in_flight,
          next_ordinal,
          terminal
        from ${this.#childWorkflowMapsTableName}
      `
    );
    for (const row of childWorkflowMaps.rows) {
      const workflow = this.#workflowsByRun.get(row.run_id);
      if (workflow === undefined) {
        continue;
      }
      this.#childWorkflowMapsByCommand.set(row.command_key, {
        namespace: row.namespace,
        workflow,
        task: parsePostgresJson(row.task) as ChildWorkflowMapTask,
        inputs: parsePostgresJson(row.inputs) as PayloadRef[],
        outcomes: parsePostgresJson(row.outcomes) as (ChildWorkflowMapItemOutcome<unknown> | null)[],
        inFlight: parseInFlight(parsePostgresJson(row.in_flight)),
        nextOrdinal: row.next_ordinal,
        terminal: row.terminal
      });
    }
  }

  async #writeNormalizedCounters(client: PoolClient): Promise<void> {
    const rows = [
      { name: "run", next_value: this.#nextRun },
      { name: "workflow_claim", next_value: this.#nextClaimToken },
      { name: "activity_claim", next_value: this.#nextActivityClaimToken },
      { name: "signal_sequence", next_value: this.#nextSignalSequence }
    ];
    await client.query(
      `
        insert into ${this.#countersTableName}(name, next_value)
        select name, next_value
        from jsonb_to_recordset($1::jsonb) as rows(name text, next_value bigint)
        on conflict (name) do update
        set next_value = excluded.next_value
      `,
      [stringifyJson(rows)]
    );
  }

  #historyTailsByRun(): Map<string, number> {
    return new Map(
      [...this.#workflowsByRun.entries()].map(([id, workflow]) => [id, Number(tailEventId(workflow))])
    );
  }

  #newNormalizedHistoryRowsAfter(
    tailsByRun: ReadonlyMap<string, number>
  ): readonly NormalizedHistoryRow[] {
    return [...this.#workflowsByRun.entries()].flatMap(([id, workflow]) => {
      const previousTail = tailsByRun.get(id) ?? 0;
      return workflow.history
        .filter((event) => Number(event.eventId) > previousTail)
        .map((event) => normalizedHistoryRow(workflow.runId, event));
    });
  }

  async #appendNormalizedHistoryRows(
    client: PoolClient,
    rows: readonly NormalizedHistoryRow[]
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#historyTableName}(run_id, event_id, event_type, data)
        select run_id, event_id, event_type, data
        from jsonb_to_recordset($1::jsonb) as rows(
          run_id text,
          event_id integer,
          event_type text,
          data jsonb
        )
        on conflict (run_id, event_id) do nothing
      `,
      [stringifyJson(rows)]
    );
  }

  async #readNormalizedHistoriesForClaims(
    client: PoolClient,
    targets: readonly {
      readonly runId: RunId;
      readonly replayTargetEventId: EventId;
    }[]
  ): Promise<ReadonlyMap<string, readonly HistoryEvent[]>> {
    const histories = new Map<string, HistoryEvent[]>(
      targets.map((target) => [String(target.runId), []])
    );
    if (targets.length === 0) {
      return histories;
    }
    const result = await client.query<NormalizedClaimHistorySelectRow>(
      `
        with targets as (
          select *
          from jsonb_to_recordset($1::jsonb) as target(
            run_id text,
            replay_target_event_id bigint
          )
        )
        select
          history.run_id,
          history.event_id,
          history.event_type,
          history.data
        from ${this.#historyTableName} history
        join targets
          on targets.run_id = history.run_id
         and history.event_id::bigint <= targets.replay_target_event_id
        where history.event_id::bigint > 0
        order by history.run_id asc, history.event_id asc
      `,
      [
        stringifyJson(
          targets.map((target) => ({
            run_id: String(target.runId),
            replay_target_event_id: Number(target.replayTargetEventId)
          }))
        )
      ]
    );
    for (const row of result.rows) {
      const history = histories.get(row.run_id);
      if (history !== undefined) {
        history.push(historyEventFromNormalizedRow(row));
      }
    }
    return histories;
  }

  async #selectCurrentWorkflowRunId(
    client: PoolClient,
    namespace: Namespace | string,
    workflowId: WorkflowId | string
  ): Promise<string | null> {
    const result = await client.query<{ readonly run_id: string }>(
      `
        select run_id
        from ${this.#workflowIdsTableName}
        where namespace = $1 and workflow_id = $2
        limit 1
      `,
      [String(namespace), String(workflowId)]
    );
    return result.rows[0]?.run_id ?? null;
  }

  async #hasCancelableOpenChildren(client: PoolClient, parentRunId: RunId): Promise<boolean> {
    const result = await client.query<{ readonly exists: boolean }>(
      `
        select exists(
          select 1
          from ${this.#workflowRunsTableName}
          where terminal = false
            and parent is not null
            and parent->>'parentRunId' = $1
            and parent->>'parentClosePolicy' = 'Cancel'
          limit 1
        ) as exists
      `,
      [String(parentRunId)]
    );
    return result.rows[0]?.exists ?? false;
  }

  /**
   * Whether the run still owns a map descriptor that has not ended.
   *
   * A commit that closes a run has to abandon that run's live fanout, which
   * means stepping each descriptor through the map engine and rewriting its
   * item rows. The SQL-native fast path cannot express that, so it hands the
   * commit back to the general path rather than closing the run and leaving
   * the map spawning items behind it.
   */
  async #hasOpenMapDescriptors(client: PoolClient, runIdValue: RunId): Promise<boolean> {
    const result = await client.query<{ readonly exists: boolean }>(
      `
        select
          exists(
            select 1 from ${this.#activityMapsTableName}
            where run_id = $1 and terminal = false
            limit 1
          )
          or exists(
            select 1 from ${this.#childWorkflowMapsTableName}
            where run_id = $1 and terminal = false
            limit 1
          ) as exists
      `,
      [String(runIdValue)]
    );
    return result.rows[0]?.exists ?? false;
  }

  async #hasReadySignalWait(client: PoolClient, runIdValue: RunId): Promise<boolean> {
    const result = await client.query<{ readonly exists: boolean }>(
      `
        select exists(
          select 1
          from ${this.#waitsTableName} waits
          join ${this.#signalsTableName} signals
            on signals.run_id = waits.run_id
           and signals.signal_name = waits.key
           and signals.consumed = false
          where waits.run_id = $1
            and waits.kind = 'Signal'
          limit 1
        ) as exists
      `,
      [String(runIdValue)]
    );
    return result.rows[0]?.exists ?? false;
  }

  async #replaceNormalizedWorkflowIdRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#workflowIdsTableName}`);
    const rows = [...this.#workflowsById.values()].map((workflow) => ({
      namespace: workflow.namespace,
      workflow_id: workflow.workflowId,
      run_id: String(workflow.runId)
    }));
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#workflowIdsTableName}(namespace, workflow_id, run_id)
        select namespace, workflow_id, run_id
        from jsonb_to_recordset($1::jsonb) as rows(
          namespace text,
          workflow_id text,
          run_id text
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #replaceNormalizedWorkflowRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#workflowRunsTableName}`);
    const rows = [...this.#workflowsByRun.values()].map(normalizedWorkflowRunRow);
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#workflowRunsTableName}(
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        )
        select
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        from jsonb_to_recordset($1::jsonb) as rows(
          run_id text,
          namespace text,
          workflow_id text,
          workflow_type_name text,
          workflow_type_version integer,
          workflow_type jsonb,
          task_queue text,
          tail_event_id integer,
          ready_reason text,
          ready_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_reason text,
          claim_expires_at_ms bigint,
          query_projection jsonb,
          terminal boolean,
          parent jsonb
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #insertNormalizedWorkflowRows(
    client: PoolClient,
    rows: readonly NormalizedWorkflowRunRow[]
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#workflowRunsTableName}(
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        )
        select
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        from jsonb_to_recordset($1::jsonb) as rows(
          run_id text,
          namespace text,
          workflow_id text,
          workflow_type_name text,
          workflow_type_version integer,
          workflow_type jsonb,
          task_queue text,
          tail_event_id integer,
          ready_reason text,
          ready_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_reason text,
          claim_expires_at_ms bigint,
          query_projection jsonb,
          terminal boolean,
          parent jsonb
        )
        on conflict (run_id) do nothing
      `,
      [stringifyJson(rows)]
    );
  }

  async #upsertNormalizedWorkflowRow(
    client: PoolClient,
    workflow: WorkflowState
  ): Promise<void> {
    const row = normalizedWorkflowRunRow(workflow);
    await client.query(
      `
        insert into ${this.#workflowRunsTableName}(
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        )
        select
          run_id,
          namespace,
          workflow_id,
          workflow_type_name,
          workflow_type_version,
          workflow_type,
          task_queue,
          tail_event_id,
          ready_reason,
          ready_at_ms,
          claim_worker_id,
          claim_token,
          claim_reason,
          claim_expires_at_ms,
          query_projection,
          terminal,
          parent
        from jsonb_to_record($1::jsonb) as row(
          run_id text,
          namespace text,
          workflow_id text,
          workflow_type_name text,
          workflow_type_version integer,
          workflow_type jsonb,
          task_queue text,
          tail_event_id integer,
          ready_reason text,
          ready_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_reason text,
          claim_expires_at_ms bigint,
          query_projection jsonb,
          terminal boolean,
          parent jsonb
        )
        on conflict (run_id) do update
        set
          namespace = excluded.namespace,
          workflow_id = excluded.workflow_id,
          workflow_type_name = excluded.workflow_type_name,
          workflow_type_version = excluded.workflow_type_version,
          workflow_type = excluded.workflow_type,
          task_queue = excluded.task_queue,
          tail_event_id = excluded.tail_event_id,
          ready_reason = excluded.ready_reason,
          ready_at_ms = excluded.ready_at_ms,
          claim_worker_id = excluded.claim_worker_id,
          claim_token = excluded.claim_token,
          claim_reason = excluded.claim_reason,
          claim_expires_at_ms = excluded.claim_expires_at_ms,
          query_projection = excluded.query_projection,
          terminal = excluded.terminal,
          parent = excluded.parent
      `,
      [stringifyJson(row)]
    );
  }

  async #updateWorkflowTailsAndReasons(
    client: PoolClient,
    rows: readonly {
      readonly run_id: string;
      readonly tail_event_id: number;
      readonly ready_reason: WorkflowTaskReason | null;
    }[]
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        update ${this.#workflowRunsTableName} runs
        set
          tail_event_id = updates.tail_event_id,
          ready_reason = updates.ready_reason,
          ready_at_ms = 0
        from jsonb_to_recordset($1::jsonb) as updates(
          run_id text,
          tail_event_id integer,
          ready_reason text
        )
        where runs.run_id = updates.run_id
      `,
      [stringifyJson(rows)]
    );
  }

  async #replaceNormalizedQueryProjectionRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#queryProjectionsTableName}`);
    const rows = [...this.#workflowsByRun.values()]
      .filter((workflow) => workflow.queryProjection !== null)
      .map(normalizedQueryProjectionRow);
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#queryProjectionsTableName}(
          run_id,
          namespace,
          workflow_id,
          projection
        )
        select
          run_id,
          namespace,
          workflow_id,
          projection
        from jsonb_to_recordset($1::jsonb) as rows(
          run_id text,
          namespace text,
          workflow_id text,
          projection jsonb
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #upsertNormalizedQueryProjectionRow(
    client: PoolClient,
    workflow: WorkflowState
  ): Promise<void> {
    if (workflow.queryProjection === null) {
      await client.query(
        `delete from ${this.#queryProjectionsTableName}
         where run_id = $1`,
        [String(workflow.runId)]
      );
      return;
    }
    const row = normalizedQueryProjectionRow(workflow);
    await client.query(
      `
        insert into ${this.#queryProjectionsTableName}(
          run_id,
          namespace,
          workflow_id,
          projection
        )
        select run_id, namespace, workflow_id, projection
        from jsonb_to_record($1::jsonb) as row(
          run_id text,
          namespace text,
          workflow_id text,
          projection jsonb
        )
        on conflict (run_id) do update
        set
          namespace = excluded.namespace,
          workflow_id = excluded.workflow_id,
          projection = excluded.projection
      `,
      [stringifyJson(row)]
    );
  }

  async #selectTimedOutActivityIds(
    client: PoolClient,
    req: TimeoutDueActivitiesRequest
  ): Promise<readonly string[]> {
    const limit = Math.max(1, Math.trunc(req.limit));
    const result = await client.query<{ readonly activity_id: string }>(
      `
        select activity_id
        from ${this.#activityTasksTableName}
        where namespace = $1
          and terminal_event_id is null
          and claim_token is not null
          and timeout_deadline_at_ms is not null
          and timeout_deadline_at_ms <= $2::bigint
        order by timeout_deadline_at_ms asc, activity_id asc
        limit $3::bigint
      `,
      [String(req.namespace), String(Number(req.now)), String(limit)]
    );
    return result.rows.map((row) => row.activity_id);
  }

  async #replaceNormalizedWaitRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#waitsTableName}`);
    const rows = [...this.#waitsById.entries()].flatMap(([id, wait]) => {
      const workflow = this.#workflowsByRun.get(wait.runId);
      if (workflow === undefined) {
        return [];
      }
      return [normalizedWaitRow(id, workflow.namespace, wait)];
    });
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#waitsTableName}(
          wait_id,
          run_id,
          namespace,
          command_id,
          kind,
          key,
          ready_at_ms
        )
        select wait_id, run_id, namespace, command_id, kind, key, ready_at_ms
        from jsonb_to_recordset($1::jsonb) as rows(
          wait_id text,
          run_id text,
          namespace text,
          command_id jsonb,
          kind text,
          key text,
          ready_at_ms bigint
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #upsertNormalizedWaitRow(
    client: PoolClient,
    waitId: string,
    wait: WaitRecord
  ): Promise<void> {
    const workflow = this.#workflowsByRun.get(wait.runId);
    if (workflow === undefined) {
      await this.#deleteNormalizedWaitRow(client, waitId);
      return;
    }
    const row = normalizedWaitRow(waitId, workflow.namespace, wait);
    await client.query(
      `
        insert into ${this.#waitsTableName}(
          wait_id,
          run_id,
          namespace,
          command_id,
          kind,
          key,
          ready_at_ms
        )
        select wait_id, run_id, namespace, command_id, kind, key, ready_at_ms
        from jsonb_to_record($1::jsonb) as row(
          wait_id text,
          run_id text,
          namespace text,
          command_id jsonb,
          kind text,
          key text,
          ready_at_ms bigint
        )
        on conflict (wait_id) do update
        set
          run_id = excluded.run_id,
          namespace = excluded.namespace,
          command_id = excluded.command_id,
          kind = excluded.kind,
          key = excluded.key,
          ready_at_ms = excluded.ready_at_ms
      `,
      [stringifyJson(row)]
    );
  }

  async #upsertNormalizedWaitRowsForNamespace(
    client: PoolClient,
    namespace: string,
    waits: readonly WaitRecord[]
  ): Promise<void> {
    if (waits.length === 0) {
      return;
    }
    const rows = waits.map((wait) => normalizedWaitRow(String(wait.waitId), namespace, wait));
    await client.query(
      `
        insert into ${this.#waitsTableName}(
          wait_id,
          run_id,
          namespace,
          command_id,
          kind,
          key,
          ready_at_ms
        )
        select wait_id, run_id, namespace, command_id, kind, key, ready_at_ms
        from jsonb_to_recordset($1::jsonb) as rows(
          wait_id text,
          run_id text,
          namespace text,
          command_id jsonb,
          kind text,
          key text,
          ready_at_ms bigint
        )
        on conflict (wait_id) do update
        set
          run_id = excluded.run_id,
          namespace = excluded.namespace,
          command_id = excluded.command_id,
          kind = excluded.kind,
          key = excluded.key,
          ready_at_ms = excluded.ready_at_ms
      `,
      [stringifyJson(rows)]
    );
  }

  async #deleteNormalizedWaitRow(client: PoolClient, waitId: string): Promise<void> {
    await client.query(
      `delete from ${this.#waitsTableName}
       where wait_id = $1`,
      [waitId]
    );
  }

  async #replaceNormalizedSignalRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#signalsTableName}`);
    const rows = [...this.#signalsById.entries()].map(([id, signal]) =>
      normalizedSignalRow(id, signal)
    );
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#signalsTableName}(
          signal_id,
          run_id,
          signal_name,
          payload,
          received_sequence,
          consumed
        )
        select signal_id, run_id, signal_name, payload, received_sequence, consumed
        from jsonb_to_recordset($1::jsonb) as rows(
          signal_id text,
          run_id text,
          signal_name text,
          payload jsonb,
          received_sequence bigint,
          consumed boolean
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #upsertNormalizedSignalRow(
    client: PoolClient,
    signalId: string,
    signal: SignalState
  ): Promise<void> {
    const row = normalizedSignalRow(signalId, signal);
    await client.query(
      `
        insert into ${this.#signalsTableName}(
          signal_id,
          run_id,
          signal_name,
          payload,
          received_sequence,
          consumed
        )
        select signal_id, run_id, signal_name, payload, received_sequence, consumed
        from jsonb_to_record($1::jsonb) as row(
          signal_id text,
          run_id text,
          signal_name text,
          payload jsonb,
          received_sequence bigint,
          consumed boolean
        )
        on conflict (signal_id) do update
        set
          run_id = excluded.run_id,
          signal_name = excluded.signal_name,
          payload = excluded.payload,
          received_sequence = excluded.received_sequence,
          consumed = excluded.consumed
      `,
      [stringifyJson(row)]
    );
  }

  async #replaceNormalizedActivityTaskRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#activityTasksTableName}`);
    const rows = [...this.#activitiesById.values()].map(normalizedActivityTaskRow);
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#activityTasksTableName}(
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        )
        select
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        from jsonb_to_recordset($1::jsonb) as rows(
          activity_id text,
          namespace text,
          run_id text,
          task jsonb,
          input jsonb,
          activity_name text,
          task_queue text,
          available_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_started_at_ms bigint,
          heartbeat_deadline_at_ms bigint,
          timeout_deadline_at_ms bigint,
          claim_expires_at_ms bigint,
          claim_lease_duration_ms bigint,
          terminal_event_id integer,
          map_command_key text,
          map_item_ordinal integer
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #upsertNormalizedActivityTaskRow(
    client: PoolClient,
    activity: ActivityState
  ): Promise<void> {
    const row = normalizedActivityTaskRow(activity);
    await client.query(
      `
        insert into ${this.#activityTasksTableName}(
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        )
        select
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        from jsonb_to_record($1::jsonb) as row(
          activity_id text,
          namespace text,
          run_id text,
          task jsonb,
          input jsonb,
          activity_name text,
          task_queue text,
          available_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_started_at_ms bigint,
          heartbeat_deadline_at_ms bigint,
          timeout_deadline_at_ms bigint,
          claim_expires_at_ms bigint,
          claim_lease_duration_ms bigint,
          terminal_event_id integer,
          map_command_key text,
          map_item_ordinal integer
        )
        on conflict (activity_id) do update
        set
          namespace = excluded.namespace,
          run_id = excluded.run_id,
          task = excluded.task,
          input = excluded.input,
          activity_name = excluded.activity_name,
          task_queue = excluded.task_queue,
          available_at_ms = excluded.available_at_ms,
          claim_worker_id = excluded.claim_worker_id,
          claim_token = excluded.claim_token,
          claim_started_at_ms = excluded.claim_started_at_ms,
          heartbeat_deadline_at_ms = excluded.heartbeat_deadline_at_ms,
          timeout_deadline_at_ms = excluded.timeout_deadline_at_ms,
          claim_expires_at_ms = excluded.claim_expires_at_ms,
          claim_lease_duration_ms = excluded.claim_lease_duration_ms,
          terminal_event_id = excluded.terminal_event_id,
          map_command_key = excluded.map_command_key,
          map_item_ordinal = excluded.map_item_ordinal
      `,
      [stringifyJson(row)]
    );
  }

  async #insertNormalizedActivityTaskRows(
    client: PoolClient,
    rows: readonly NormalizedActivityTaskRow[]
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#activityTasksTableName}(
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        )
        select
          activity_id,
          namespace,
          run_id,
          task,
          input,
          activity_name,
          task_queue,
          available_at_ms,
          claim_worker_id,
          claim_token,
          claim_started_at_ms,
          heartbeat_deadline_at_ms,
          timeout_deadline_at_ms,
          claim_expires_at_ms,
          claim_lease_duration_ms,
          terminal_event_id,
          map_command_key,
          map_item_ordinal
        from jsonb_to_recordset($1::jsonb) as rows(
          activity_id text,
          namespace text,
          run_id text,
          task jsonb,
          input jsonb,
          activity_name text,
          task_queue text,
          available_at_ms bigint,
          claim_worker_id text,
          claim_token bigint,
          claim_started_at_ms bigint,
          heartbeat_deadline_at_ms bigint,
          timeout_deadline_at_ms bigint,
          claim_expires_at_ms bigint,
          claim_lease_duration_ms bigint,
          terminal_event_id integer,
          map_command_key text,
          map_item_ordinal integer
        )
        on conflict (activity_id) do update
        set
          namespace = excluded.namespace,
          run_id = excluded.run_id,
          task = excluded.task,
          input = excluded.input,
          activity_name = excluded.activity_name,
          task_queue = excluded.task_queue,
          available_at_ms = excluded.available_at_ms,
          claim_worker_id = excluded.claim_worker_id,
          claim_token = excluded.claim_token,
          claim_started_at_ms = excluded.claim_started_at_ms,
          heartbeat_deadline_at_ms = excluded.heartbeat_deadline_at_ms,
          timeout_deadline_at_ms = excluded.timeout_deadline_at_ms,
          claim_expires_at_ms = excluded.claim_expires_at_ms,
          claim_lease_duration_ms = excluded.claim_lease_duration_ms,
          terminal_event_id = excluded.terminal_event_id,
          map_command_key = excluded.map_command_key,
          map_item_ordinal = excluded.map_item_ordinal
      `,
      [stringifyJson(rows)]
    );
  }

  async #applyTargetedActivityProjectionUpdates(
    client: PoolClient,
    updates: readonly TargetedActivityProjectionUpdate[]
  ): Promise<void> {
    const workflows = new Map<string, WorkflowState>();
    const activities = new Map<string, ActivityState>();
    for (const update of updates) {
      activities.set(update.activity.task.activityId, update.activity);
      if (update.workflow !== null) {
        workflows.set(String(update.workflow.runId), update.workflow);
      }
    }
    for (const workflow of [...workflows.values()].sort((left, right) =>
      String(left.runId).localeCompare(String(right.runId))
    )) {
      await this.#upsertNormalizedWorkflowRow(client, workflow);
    }
    for (const activity of [...activities.values()].sort((left, right) =>
      left.task.activityId.localeCompare(right.task.activityId)
    )) {
      await this.#upsertNormalizedActivityTaskRow(client, activity);
    }
  }

  async #replaceNormalizedActivityMapRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#activityMapsTableName}`);
    const rows = [...this.#activityMapsByCommand.entries()].map(([key, map]) =>
      normalizedActivityMapRow(key, map)
    );
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#activityMapsTableName}(
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          results,
          in_flight,
          next_ordinal,
          terminal
        )
        select
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          results,
          in_flight,
          next_ordinal,
          terminal
        from jsonb_to_recordset($1::jsonb) as rows(
          command_key text,
          namespace text,
          run_id text,
          task jsonb,
          input_manifest jsonb,
          input_count integer,
          inputs jsonb,
          results jsonb,
          in_flight jsonb,
          next_ordinal integer,
          terminal boolean
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #replaceNormalizedActivityMapItemRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#activityMapItemsTableName}`);
    const rows = [...this.#activityMapsByCommand.entries()].flatMap(([key, map]) =>
      normalizedActivityMapItemRows(key, map)
    );
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#activityMapItemsTableName}(
          command_key,
          namespace,
          run_id,
          item_ordinal,
          input,
          result,
          in_flight,
          terminal
        )
        select
          command_key,
          namespace,
          run_id,
          item_ordinal,
          input,
          result,
          in_flight,
          terminal
        from jsonb_to_recordset($1::jsonb) as rows(
          command_key text,
          namespace text,
          run_id text,
          item_ordinal integer,
          input jsonb,
          result jsonb,
          in_flight boolean,
          terminal boolean
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #replaceNormalizedChildWorkflowMapRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#childWorkflowMapsTableName}`);
    const rows = [...this.#childWorkflowMapsByCommand.entries()].map(([key, map]) =>
      normalizedChildWorkflowMapRow(key, map)
    );
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#childWorkflowMapsTableName}(
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          outcomes,
          in_flight,
          next_ordinal,
          terminal
        )
        select
          command_key,
          namespace,
          run_id,
          task,
          input_manifest,
          input_count,
          inputs,
          outcomes,
          in_flight,
          next_ordinal,
          terminal
        from jsonb_to_recordset($1::jsonb) as rows(
          command_key text,
          namespace text,
          run_id text,
          task jsonb,
          input_manifest jsonb,
          input_count integer,
          inputs jsonb,
          outcomes jsonb,
          in_flight jsonb,
          next_ordinal integer,
          terminal boolean
        )
      `,
      [stringifyJson(rows)]
    );
  }

  async #replaceNormalizedChildWorkflowMapItemRows(client: PoolClient): Promise<void> {
    await client.query(`delete from ${this.#childWorkflowMapItemsTableName}`);
    const rows = [...this.#childWorkflowMapsByCommand.entries()].flatMap(([key, map]) =>
      normalizedChildWorkflowMapItemRows(key, map)
    );
    if (rows.length === 0) {
      return;
    }
    await client.query(
      `
        insert into ${this.#childWorkflowMapItemsTableName}(
          command_key,
          namespace,
          run_id,
          item_ordinal,
          input,
          outcome,
          in_flight,
          terminal
        )
        select
          command_key,
          namespace,
          run_id,
          item_ordinal,
          input,
          outcome,
          in_flight,
          terminal
        from jsonb_to_recordset($1::jsonb) as rows(
          command_key text,
          namespace text,
          run_id text,
          item_ordinal integer,
          input jsonb,
          outcome jsonb,
          in_flight boolean,
          terminal boolean
        )
      `,
      [stringifyJson(rows)]
    );
  }

  #stateForRun(id: RunId): WorkflowState {
    const state = this.#workflowsByRun.get(id);
    if (!state) {
      throw new Error(`workflow run not found: ${id}`);
    }
    return state;
  }

  #wakeIfSignalWaitReady(state: WorkflowState): void {
    const ready = [...this.#waitsById.values()].some(
      (wait) =>
        wait.runId === state.runId &&
        wait.kind === "Signal" &&
        [...this.#signalsById.values()].some(
          (signal) =>
            signal.runId === state.runId &&
            String(signal.signalName) === wait.key &&
            !signal.consumed
        )
    );
    if (ready) {
      markWorkflowReady(state, "SignalReceived");
    }
  }

  // ---------------------------------------------------------------------
  // Map fanout. Every decision below — what to admit, when the bound is
  // reached, whether a failed attempt retries, which failure mode ends the
  // map, when the parent is notified — belongs to `map-engine.ts`. What is
  // left here is storage: insert item tasks, start item children, write the
  // descriptor cursor, append a parent event, tombstone leftovers. The
  // normalized projection rows are rewritten from this state by the commit's
  // rewrite scope, which every map path already widens to `full`.
  // ---------------------------------------------------------------------

  /**
   * `WorkflowTaskCommit.cancelCommands`: withdraw one command's operational
   * state. A plain activity is tombstoned; a map is handed to the engine as
   * `ParentCancelled`, which tombstones its pending work and closes the
   * descriptor. Routing the map through the engine rather than deleting the
   * descriptor here is what keeps the cancellation path from drifting away
   * from the fail-fast one.
   */
  #cancelCommandOperationalState(cancelled: CommandId): void {
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null &&
        sameCommandId(activity.task.commandId, cancelled) &&
        activity.terminalEventId === null
      ) {
        activity.terminalEventId = tailEventId(activity.workflow);
        activity.claim = null;
      }
    }
    const activityMap = this.#activityMapsByCommand.get(commandKey(cancelled));
    if (activityMap !== undefined) {
      this.#stepActivityMap(activityMap, { kind: "ParentCancelled" });
    }
    const childMap = this.#childWorkflowMapsByCommand.get(commandKey(cancelled));
    if (childMap !== undefined && !childMap.terminal) {
      // Withdrawing the command on a *live* run means the `parentClosePolicy`
      // path never runs, so without this the children the map already started
      // keep running with nothing waiting for them.
      //
      // Children first, descriptor last, matching the fail-fast effect order.
      // That is a consistency choice and not a constraint: the reverse order
      // was measured green across the whole memory and SQLite conformance
      // suite, because child cancellation filters on the child's parent link
      // and its own terminal flag and never reads the descriptor's.
      this.#cancelRunningChildWorkflowMapItems(
        childMap,
        mapCommandCancelledReason(childMap.task.mapCommandId)
      );
      this.#stepChildWorkflowMap(childMap, { kind: "ParentCancelled" });
    }
  }

  /**
   * Abandon-on-close: a run that has just reached a terminal event owns no
   * live work any more. Every map of that run is handed to the engine as
   * `ParentCancelled`, and every plain activity of it is tombstoned.
   *
   * Without this a closed workflow keeps spawning and appending: completing an
   * item of its map materialized the next ordinal, and both a map's terminal
   * fact and a plain activity's `ActivityCompleted` were appended to the run's
   * history *after* its terminal event — the exact thing the terminal-commit
   * guard exists to prevent. Rust reaches the same end by deleting the run's
   * activities and descriptors during terminal cleanup.
   *
   * Children are deliberately left alone here. `ParentCancelled` emits only
   * `AbandonPendingItems` and `MarkDescriptorTerminal` — in both runtimes — so
   * this cannot cascade, and a closed run's children belong to the
   * `parentClosePolicy` path, which must stay free to *abandon* them rather
   * than cancel them. The other `ParentCancelled` producer, a withdrawn
   * command on a still-live run, has no such path and cancels the map's
   * children itself; see `#cancelCommandOperationalState`.
   */
  /**
   * See `MemoryBackend.#abandonWorkForClosedRun` for why each half is here.
   *
   * This half runs on the loaded-state path, whose rewrite scope for a terminal
   * commit is `fullNormalizedRewriteScope` — `workflowCommitEventRequiresFullProjectionRewrite`
   * names every terminal event — so mutating `#waitsById` is enough to remove
   * the rows. The SQL-native commit path does the same deletion as a statement;
   * see `#commitWorkflowTaskSqlNative`.
   */
  #abandonWorkForClosedRun(state: WorkflowState): void {
    for (const [waitIdValue, wait] of this.#waitsById) {
      if (wait.runId === state.runId) {
        this.#waitsById.delete(waitIdValue);
      }
    }
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null &&
        activity.workflow.runId === state.runId &&
        activity.terminalEventId === null
      ) {
        activity.terminalEventId = tailEventId(state);
        activity.claim = null;
      }
    }
    for (const map of this.#activityMapsByCommand.values()) {
      if (map.workflow.runId === state.runId && !map.terminal) {
        this.#stepActivityMap(map, { kind: "ParentCancelled" });
      }
    }
    for (const map of this.#childWorkflowMapsByCommand.values()) {
      if (map.workflow.runId === state.runId && !map.terminal) {
        this.#stepChildWorkflowMap(map, { kind: "ParentCancelled" });
      }
    }
  }

  #createActivityMap(workflow: WorkflowState, task: ActivityMapTask): void {
    // Validation runs before the engine is consulted. The engine clamps a
    // degenerate bound so an already-persisted descriptor cannot stall, but a
    // caller typo must still be rejected at the scheduling boundary rather
    // than silently reinterpreted as one.
    if (task.maxInFlight <= 0 || !Number.isInteger(task.maxInFlight)) {
      throw new Error("activity map maxInFlight must be a positive integer");
    }
    const inputs = decodeActivityMapInputs(task.inputManifest);
    const map: ActivityMapState = {
      namespace: workflow.namespace,
      workflow,
      task,
      inputs,
      results: Array.from({ length: inputs.length }, () => null),
      inFlight: 0,
      nextOrdinal: 0,
      terminal: false
    };
    this.#activityMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#stepActivityMap(map, {
      kind: "DescriptorCreated",
      parentTerminal: workflow.terminal
    });
  }

  /** The activity-map descriptor as `map-engine.ts` sees it. */
  #activityMapEngineState(map: ActivityMapState): MapState {
    return {
      mapCommandId: map.task.mapCommandId,
      kind: "Activity",
      // An activity map is always fail-fast; the field is inert for it.
      failureMode: "FailFast",
      itemCount: map.inputs.length,
      nextOrdinal: map.nextOrdinal,
      inFlight: map.inFlight,
      maxInFlight: map.task.maxInFlight,
      recordedOutcomes: recordedOutcomeCount(map.results),
      completed: map.terminal
    };
  }

  /**
   * Run one activity-map transition: project the descriptor, ask the engine,
   * apply the effects it returns. Returns the parent event the transition
   * appended, if any.
   */
  #stepActivityMap(map: ActivityMapState, event: MapEvent): EventId | null {
    const transition = stepMap(this.#activityMapEngineState(map), event);
    if (transition.kind === "Reject") {
      throw new Error(mapRejectMessage("Activity", transition.reject));
    }
    return this.#applyActivityMapEffects(map, transition.effects);
  }

  /**
   * Apply an engine effect list in order. Each arm is one storage primitive;
   * no arm decides anything the engine already decided.
   */
  #applyActivityMapEffects(
    map: ActivityMapState,
    effects: readonly MapEffect[]
  ): EventId | null {
    let appended: EventId | null = null;
    for (const effect of effects) {
      switch (effect.kind) {
        case "MaterializeItems":
          this.#insertActivityMapItemBatch(map, effect.firstOrdinal, effect.count);
          break;
        case "AdvanceDescriptor":
          map.nextOrdinal = effect.nextOrdinal;
          map.inFlight = effect.inFlight;
          break;
        case "ScheduleItemRetry": {
          const activity = this.#activitiesById.get(
            activityMapItemId(map.task.mapCommandId, effect.ordinal)
          );
          if (activity !== undefined) {
            activity.task = { ...activity.task, attempt: effect.nextAttempt };
            // `null` means immediately claimable, which this provider spells
            // as an availability instant already in the past.
            activity.availableAtMs = effect.visibleAtMs ?? 0;
            activity.claim = null;
          }
          // `effect.timeoutAtMs` has no consumer here. Not because map items
          // are exempt from the timeout scanners — they are not; see
          // `activityTimeoutDeadline`, which covers them on the same terms as
          // any other activity. It is unused because the deadline is derived
          // at claim time — `activityTimeoutDeadlineFromTask` stamps
          // `timeout_deadline_at_ms` on the claiming update — and this effect
          // hands the item back unclaimed: anything stamped here would be
          // recomputed by the claim that follows.
          break;
        }
        case "CompleteMap":
          appended = this.#appendActivityMapCompleted(map, effect.itemCount);
          break;
        case "FailMap":
          appended = this.#appendActivityMapFailed(map, effect.failure);
          break;
        case "AbandonPendingItems":
          this.#abandonPendingActivityMapItems(map);
          break;
        case "MarkDescriptorTerminal":
          map.terminal = true;
          map.inFlight = 0;
          break;
        case "RecordItemOutcome":
        case "CancelChildren":
          throw new Error(`activity maps never receive ${effect.kind}`);
      }
    }
    return appended;
  }

  /**
   * `MaterializeItems`: insert `[firstOrdinal, firstOrdinal + count)` as
   * claimable item activity tasks.
   */
  #insertActivityMapItemBatch(
    map: ActivityMapState,
    firstOrdinal: number,
    count: number
  ): void {
    for (let ordinal = firstOrdinal; ordinal < firstOrdinal + count; ordinal += 1) {
      const activityId = activityMapItemId(map.task.mapCommandId, ordinal);
      this.#activitiesById.set(activityId, {
        namespace: map.namespace,
        workflow: map.workflow,
        task: {
          activityId,
          runId: map.task.mapCommandId.runId,
          commandId: map.task.mapCommandId,
          activityName: map.task.activityName,
          taskQueue: map.task.taskQueue,
          retryPolicy: map.task.retryPolicy,
          startToCloseTimeoutMs: map.task.startToCloseTimeoutMs,
          heartbeatTimeoutMs: map.task.heartbeatTimeoutMs,
          attempt: 1,
          input: map.inputs[ordinal] as PayloadRef,
          mapItem: {
            mapCommandId: map.task.mapCommandId,
            itemOrdinal: ordinal
          }
        },
        claim: null,
        availableAtMs: 0,
        terminalEventId: null
      });
    }
  }

  /**
   * `AbandonPendingItems`: tombstone every not-yet-terminal item task of a map
   * that is over, so neither the claim path nor a late completion can
   * resurrect it. The claim-time guard that skips items of a terminal map
   * stays as well: a claim already in flight when the map ended must not
   * change the answer.
   */
  #abandonPendingActivityMapItems(map: ActivityMapState): void {
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null ||
        !sameCommandId(activity.task.mapItem.mapCommandId, map.task.mapCommandId) ||
        activity.terminalEventId !== null
      ) {
        continue;
      }
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
    }
  }

  /**
   * `CompleteMap`: assemble the result manifest in ascending ordinal order,
   * append the terminal success fact to the parent, and wake it.
   */
  #appendActivityMapCompleted(map: ActivityMapState, itemCount: number): EventId {
    const counts = activityOutcomeCounts(itemCount);
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest: encodeActivityMapResultManifest(
          map.task.resultManifestName,
          completeMapItems("activity map result manifest", itemCount, map.results)
        ),
        itemCount,
        successCount: counts.successCount,
        failureCount: counts.failureCount
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ActivityMapCompleted");
    return event.eventId;
  }

  /** `FailMap`: append the terminal failure fact to the parent and wake it. */
  #appendActivityMapFailed(map: ActivityMapState, failure: DurableFailure): EventId {
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ActivityMapFailed");
    return event.eventId;
  }

  #completeActivityMapItem(activity: ActivityState, result: PayloadRef): EventId {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
      return tailEventId(map.workflow);
    }
    const ordinal = activity.task.mapItem.itemOrdinal;
    const alreadyRecorded = (map.results[ordinal] ?? null) !== null;
    // Ask before writing. An activity map's result slot is a storage primitive
    // rather than an effect, so it must not be written for a transition the
    // engine rejects.
    const transition = stepMap(this.#activityMapEngineState(map), {
      kind: "ItemCompleted",
      ordinal,
      // The result payload rides the descriptor's result slot, not the
      // outcome; the engine only needs to know this was a success.
      outcome: { kind: "Succeeded", result },
      alreadyRecorded,
      parentTerminal: map.workflow.terminal
    });
    if (transition.kind === "Reject") {
      throw new Error(mapRejectMessage("Activity", transition.reject));
    }
    if (!alreadyRecorded) {
      map.results[ordinal] = result;
    }
    activity.terminalEventId = tailEventId(map.workflow);
    activity.claim = null;
    return this.#applyActivityMapEffects(map, transition.effects) ?? tailEventId(map.workflow);
  }

  /**
   * One activity-map item attempt ended. `decision` is the shared retry
   * verdict, computed but not applied: the engine turns it into either a
   * rescheduled attempt or the map's terminal failure, because only the engine
   * knows whether the map is still running.
   */
  #failActivityMapItem(
    activity: ActivityState,
    failure: DurableFailure,
    decision: ItemRetryDecision,
    attemptFailure: ItemAttemptFailureKind = "Failed"
  ): FailActivityOutcome {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
      return { kind: "Failed", eventId: tailEventId(map.workflow) };
    }
    const ordinal = activity.task.mapItem.itemOrdinal;
    const appended = this.#stepActivityMap(map, {
      kind: "ItemAttemptFailed",
      ordinal,
      failure,
      attemptFailure,
      decision,
      failedAttempt: activity.task.attempt,
      retryPolicy: activity.task.retryPolicy,
      startToCloseTimeoutMs: activity.task.startToCloseTimeoutMs,
      nowMs: this.#nowMs(),
      alreadyRecorded: (map.results[ordinal] ?? null) !== null,
      parentTerminal: map.workflow.terminal
    });
    if (decision.kind === "Retry") {
      return {
        kind: "RetryScheduled",
        attempt: decision.nextAttempt,
        readyAtMs: activity.availableAtMs
      };
    }
    return appended === null
      ? { kind: "AlreadyCompleted" }
      : { kind: "Failed", eventId: appended };
  }

  #activityMapForTask(task: ActivityTask): ActivityMapState | undefined {
    if (task.mapItem === null) {
      return undefined;
    }
    return this.#activityMapsByCommand.get(commandKey(task.mapItem.mapCommandId));
  }

  #createChildWorkflowMap(workflow: WorkflowState, task: ChildWorkflowMapTask): void {
    if (task.maxInFlight <= 0 || !Number.isInteger(task.maxInFlight)) {
      throw new Error("child workflow map maxInFlight must be a positive integer");
    }
    if (task.workflowIdPrefix.length === 0) {
      throw new Error("child workflow map workflowIdPrefix must not be empty");
    }
    const inputs = decodeActivityMapInputs(task.inputManifest);
    const map: ChildWorkflowMapState = {
      namespace: workflow.namespace,
      workflow,
      task,
      inputs,
      outcomes: Array.from({ length: inputs.length }, () => null),
      inFlight: 0,
      nextOrdinal: 0,
      terminal: false
    };
    this.#childWorkflowMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#stepChildWorkflowMap(map, {
      kind: "DescriptorCreated",
      parentTerminal: workflow.terminal
    });
  }

  /** The child-workflow-map descriptor as `map-engine.ts` sees it. */
  #childWorkflowMapEngineState(map: ChildWorkflowMapState): MapState {
    return {
      mapCommandId: map.task.mapCommandId,
      kind: "ChildWorkflow",
      failureMode: map.task.failureMode,
      itemCount: map.inputs.length,
      nextOrdinal: map.nextOrdinal,
      inFlight: map.inFlight,
      maxInFlight: map.task.maxInFlight,
      recordedOutcomes: recordedOutcomeCount(map.outcomes),
      completed: map.terminal
    };
  }

  /**
   * Run child-map transitions until the queue drains. Starting an item child
   * can fail on an id conflict, which is itself an item outcome; rather than
   * deciding what that means, the batch primitive queues an `ItemCompleted`
   * and the loop feeds it back through the engine.
   */
  #stepChildWorkflowMap(map: ChildWorkflowMapState, event: MapEvent): EventId | null {
    const queue: MapEvent[] = [event];
    let appended: EventId | null = null;
    while (queue.length > 0) {
      const next = queue.shift() as MapEvent;
      const transition = stepMap(this.#childWorkflowMapEngineState(map), next);
      if (transition.kind === "Reject") {
        throw new Error(mapRejectMessage("ChildWorkflow", transition.reject));
      }
      appended = this.#applyChildWorkflowMapEffects(map, transition.effects, queue) ?? appended;
    }
    return appended;
  }

  #applyChildWorkflowMapEffects(
    map: ChildWorkflowMapState,
    effects: readonly MapEffect[],
    queue: MapEvent[]
  ): EventId | null {
    let appended: EventId | null = null;
    for (const effect of effects) {
      switch (effect.kind) {
        case "RecordItemOutcome":
          map.outcomes[effect.ordinal] = effect.outcome;
          break;
        case "MaterializeItems":
          this.#startChildWorkflowMapItemBatch(map, effect.firstOrdinal, effect.count, queue);
          break;
        case "AdvanceDescriptor":
          map.nextOrdinal = effect.nextOrdinal;
          map.inFlight = effect.inFlight;
          break;
        case "CompleteMap":
          appended = this.#appendChildWorkflowMapCompleted(map, effect.itemCount);
          break;
        case "FailMap":
          appended = this.#appendChildWorkflowMapFailed(map, effect.failure);
          break;
        case "AbandonPendingItems":
          // Nothing to tombstone: this provider starts an item's child inside
          // `MaterializeItems`, so a map that is over has no undispatched item
          // start, and ordinals at or past the cursor are never admitted
          // again. `CancelChildren` covers the children that did start.
          break;
        case "CancelChildren":
          this.#cancelRunningChildWorkflowMapItems(map, effect.reason);
          break;
        case "MarkDescriptorTerminal":
          map.terminal = true;
          map.inFlight = 0;
          break;
        case "ScheduleItemRetry":
          throw new Error("child workflow maps never receive ScheduleItemRetry");
      }
    }
    return appended;
  }

  /**
   * `MaterializeItems`: start `[firstOrdinal, firstOrdinal + count)` as item
   * children. An id conflict is not a start; it is the item's terminal
   * outcome, queued for the engine to route through the map's failure mode.
   */
  #startChildWorkflowMapItemBatch(
    map: ChildWorkflowMapState,
    firstOrdinal: number,
    count: number,
    queue: MapEvent[]
  ): void {
    for (let ordinal = firstOrdinal; ordinal < firstOrdinal + count; ordinal += 1) {
      const childWorkflowId = `${map.task.workflowIdPrefix}/${ordinal}`;
      const key = workflowKey(map.namespace, childWorkflowId);
      if (this.#workflowsById.has(key)) {
        queue.push({
          kind: "ItemCompleted",
          ordinal,
          outcome: {
            kind: "Failed",
            failure: {
              errorType: "durust.child_workflow_id_conflict",
              message: `child workflow id already exists: ${childWorkflowId}`,
              nonRetryable: true
            }
          },
          alreadyRecorded: false,
          parentTerminal: map.workflow.terminal
        });
        continue;
      }

      const childRunId = runId(`run-${this.#nextRun++}`);
      const child: WorkflowState = {
        namespace: map.namespace,
        workflowId: childWorkflowId,
        workflowType: map.task.workflowType,
        taskQueue: String(map.task.taskQueue),
        runId: childRunId,
        history: [
          makeHistoryEvent(eventId(1), {
            kind: "WorkflowStarted",
            workflowType: map.task.workflowType,
            input: map.inputs[ordinal] as PayloadRef
          })
        ],
        readyReason: "WorkflowStarted",
        readyAtMs: 0,
        claim: null,
        queryProjection: null,
        terminal: false,
        parent: {
          kind: "ChildWorkflowMap",
          parentRunId: map.workflow.runId,
          mapCommandId: map.task.mapCommandId,
          itemOrdinal: ordinal,
          parentClosePolicy: map.task.parentClosePolicy
        }
      };
      this.#workflowsById.set(key, child);
      this.#workflowsByRun.set(childRunId, child);
    }
  }

  #completeChildWorkflowMapItem(
    parentLink: ChildWorkflowMapParentLink,
    terminal: ChildTerminalUpdate
  ): void {
    const map = this.#childWorkflowMapsByCommand.get(commandKey(parentLink.mapCommandId));
    if (!map || map.terminal) {
      return;
    }
    this.#stepChildWorkflowMap(map, {
      kind: "ItemCompleted",
      ordinal: parentLink.itemOrdinal,
      outcome: childWorkflowMapItemOutcome(terminal),
      alreadyRecorded: (map.outcomes[parentLink.itemOrdinal] ?? null) !== null,
      parentTerminal: map.workflow.terminal
    });
  }

  /**
   * `CompleteMap`: assemble the outcome manifest in ascending ordinal order,
   * append the terminal success fact to the parent, and wake it.
   */
  #appendChildWorkflowMapCompleted(map: ChildWorkflowMapState, itemCount: number): EventId {
    const outcomes = completeMapItems(
      "child workflow map result manifest",
      itemCount,
      map.outcomes
    );
    const counts = outcomeCounts(outcomes);
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest: encodeChildWorkflowMapResultManifest(
          map.task.resultManifestName,
          outcomes
        ),
        itemCount,
        successCount: counts.successCount,
        failureCount: counts.failureCount,
        cancellationCount: counts.cancellationCount
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ChildWorkflowMapCompleted");
    return event.eventId;
  }

  /** `FailMap`: append the terminal failure fact to the parent and wake it. */
  #appendChildWorkflowMapFailed(
    map: ChildWorkflowMapState,
    failure: DurableFailure
  ): EventId {
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ChildWorkflowMapFailed");
    return event.eventId;
  }

  /**
   * `CancelChildren`: cancel every already-running, not-yet-terminal child of
   * this map with the engine's reason.
   */
  #cancelRunningChildWorkflowMapItems(map: ChildWorkflowMapState, reason: string): void {
    for (const child of this.#workflowsByRun.values()) {
      if (
        child.parent?.kind !== "ChildWorkflowMap" ||
        child.parent.parentRunId !== map.workflow.runId ||
        !sameCommandId(child.parent.mapCommandId, map.task.mapCommandId) ||
        child.terminal
      ) {
        continue;
      }
      child.history.push(makeHistoryEvent(eventId(Number(tailEventId(child)) + 1), {
        kind: "WorkflowCancelled",
        reason
      }));
      child.terminal = true;
      child.readyReason = null;
      child.readyAtMs = 0;
      child.claim = null;
      this.#abandonWorkForClosedRun(child);
    }
  }

  #startContinuedRun(previous: WorkflowState, input: PayloadRef): void {
    const newRunId = runId(`run-${this.#nextRun++}`);
    const started = makeHistoryEvent(eventId(1), {
      kind: "WorkflowStarted",
      workflowType: previous.workflowType,
      input
    });
    const next: WorkflowState = {
      namespace: previous.namespace,
      workflowId: previous.workflowId,
      workflowType: previous.workflowType,
      taskQueue: previous.taskQueue,
      runId: newRunId,
      history: [started],
      readyReason: "WorkflowStarted",
      readyAtMs: 0,
      claim: null,
      queryProjection: null,
      terminal: false,
      parent: null
    };
    this.#workflowsById.set(workflowKey(previous.namespace, previous.workflowId), next);
    this.#workflowsByRun.set(newRunId, next);
  }

  #startChildWorkflow(parent: WorkflowState, requested: ChildWorkflowStartRequested): void {
    const key = workflowKey(parent.namespace, requested.workflowId);
    if (this.#workflowsById.has(key)) {
      parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), {
        kind: "ChildWorkflowFailed",
        failed: {
          commandId: requested.commandId,
          failure: {
            errorType: "durust.child_workflow_id_conflict",
            message: `child workflow id already exists: ${requested.workflowId}`,
            nonRetryable: true
          }
        }
      }));
      markWorkflowReady(parent, "ChildWorkflowFailed");
      return;
    }

    const childRunId = runId(`run-${this.#nextRun++}`);
    const child: WorkflowState = {
      namespace: parent.namespace,
      workflowId: String(requested.workflowId),
      workflowType: requested.workflowType,
      taskQueue: String(requested.taskQueue),
      runId: childRunId,
      history: [
        makeHistoryEvent(eventId(1), {
          kind: "WorkflowStarted",
          workflowType: requested.workflowType,
          input: requested.input
        })
      ],
      readyReason: "WorkflowStarted",
      readyAtMs: 0,
      claim: null,
      queryProjection: null,
      terminal: false,
      parent: {
        kind: "Child",
        parentRunId: parent.runId,
        commandId: requested.commandId,
        parentClosePolicy: requested.parentClosePolicy
      }
    };
    this.#workflowsById.set(key, child);
    this.#workflowsByRun.set(childRunId, child);
    parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), {
      kind: "ChildWorkflowStarted",
      started: {
        commandId: requested.commandId,
        workflowId: requested.workflowId,
        runId: childRunId
      }
    }));
    markWorkflowReady(parent, "ChildWorkflowStarted");
  }

  #notifyParentOfChildTerminal(parentLink: ParentWorkflowLink, terminal: ChildTerminalUpdate): void {
    if (parentLink.kind === "ChildWorkflowMap") {
      this.#completeChildWorkflowMapItem(parentLink, terminal);
      return;
    }
    const parent = this.#workflowsByRun.get(parentLink.parentRunId);
    if (!parent || parent.terminal) {
      return;
    }
    const data: HistoryEventData =
      terminal.kind === "Completed"
        ? {
            kind: "ChildWorkflowCompleted",
            completed: {
              commandId: parentLink.commandId,
              result: terminal.result
            }
          }
        : terminal.kind === "Failed"
          ? {
              kind: "ChildWorkflowFailed",
              failed: {
                commandId: parentLink.commandId,
                failure: terminal.failure
              }
            }
          : {
              kind: "ChildWorkflowCancelled",
              cancelled: {
                commandId: parentLink.commandId,
                reason: terminal.reason
              }
            };
    parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), data));
    markWorkflowReady(
      parent,
      terminal.kind === "Completed"
        ? "ChildWorkflowCompleted"
        : terminal.kind === "Failed"
          ? "ChildWorkflowFailed"
          : "ChildWorkflowCancelled"
    );
  }

  #cancelChildrenForClosedParent(parent: WorkflowState): void {
    for (const child of this.#workflowsByRun.values()) {
      if (
        child.parent?.parentRunId !== parent.runId ||
        child.parent.parentClosePolicy !== "Cancel" ||
        child.terminal
      ) {
        continue;
      }
      child.history.push(makeHistoryEvent(eventId(Number(tailEventId(child)) + 1), {
        kind: "WorkflowCancelled",
        reason: `parent workflow closed: ${parent.runId}`
      }));
      child.terminal = true;
      child.readyReason = null;
      child.readyAtMs = 0;
      child.claim = null;
      this.#abandonWorkForClosedRun(child);
    }
  }
}

interface NormalizedHistoryRow {
  readonly run_id: string;
  readonly event_id: number;
  readonly event_type: string;
  readonly data: HistoryEventData;
}

interface NormalizedHistorySelectRow {
  readonly event_id: number | string;
  readonly event_type: string;
  readonly data: HistoryEventData;
}

interface NormalizedClaimHistorySelectRow extends NormalizedHistorySelectRow {
  readonly run_id: string;
}

interface WorkflowClaimRow {
  readonly run_id: string;
  readonly workflow_id: string;
  readonly workflow_type: unknown;
  readonly tail_event_id: number | string;
  readonly reason: string;
  readonly token: number | string;
}

interface NormalizedWorkflowRunRow {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly workflow_type_name: string;
  readonly workflow_type_version: number;
  readonly workflow_type: WorkflowType;
  readonly task_queue: string;
  readonly tail_event_id: number;
  readonly ready_reason: WorkflowTaskReason | null;
  readonly ready_at_ms: number;
  readonly claim_worker_id: string | null;
  readonly claim_token: number | null;
  readonly claim_reason: WorkflowTaskReason | null;
  readonly claim_expires_at_ms: number | null;
  readonly query_projection: PayloadRef | null;
  readonly terminal: boolean;
  readonly parent: ParentWorkflowLink | null;
}

interface NormalizedQueryProjectionRow {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly projection: PayloadRef;
}

interface NormalizedQueryProjectionSelectRow {
  readonly run_id: string;
  readonly projection: PayloadRef | null;
}

interface NormalizedWaitRow {
  readonly wait_id: string;
  readonly run_id: string;
  readonly namespace: string;
  readonly command_id: CommandId;
  readonly kind: WaitRecord["kind"];
  readonly key: string;
  readonly ready_at_ms: number | null;
}

interface NormalizedSignalRow {
  readonly signal_id: string;
  readonly run_id: string;
  readonly signal_name: string;
  readonly payload: PayloadRef;
  readonly received_sequence: number;
  readonly consumed: boolean;
}

interface NormalizedSignalSelectRow {
  readonly signal_id: string;
  readonly signal_name: string;
  readonly payload: PayloadRef;
}

interface NormalizedClaimSignalSelectRow extends NormalizedSignalSelectRow {
  readonly run_id: string;
}

interface NormalizedActivityTaskRow {
  readonly activity_id: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: ActivityTask;
  readonly input: PayloadRef;
  readonly activity_name: string;
  readonly task_queue: string;
  readonly available_at_ms: number;
  readonly claim_worker_id: string | null;
  readonly claim_token: number | null;
  readonly claim_started_at_ms: number | null;
  readonly heartbeat_deadline_at_ms: number | null;
  readonly timeout_deadline_at_ms: number | null;
  readonly claim_expires_at_ms: number | null;
  readonly claim_lease_duration_ms: number | null;
  readonly terminal_event_id: number | null;
  readonly map_command_key: string | null;
  readonly map_item_ordinal: number | null;
}

interface NormalizedActivityTaskLoadRow {
  readonly activity_id: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: unknown;
  readonly input: unknown;
  readonly activity_name: string;
  readonly task_queue: string;
  readonly available_at_ms: number | string;
  readonly claim_worker_id: string | null;
  readonly claim_token: number | string | null;
  readonly claim_started_at_ms: number | string | null;
  readonly heartbeat_deadline_at_ms: number | string | null;
  readonly timeout_deadline_at_ms: number | string | null;
  readonly claim_expires_at_ms: number | string | null;
  readonly claim_lease_duration_ms: number | string | null;
  readonly terminal_event_id: number | string | null;
  readonly map_command_key: string | null;
  readonly map_item_ordinal: number | string | null;
}

interface NormalizedWorkflowRunLoadRow {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly workflow_type_name: string;
  readonly workflow_type_version: number;
  readonly workflow_type: unknown;
  readonly task_queue: string;
  readonly tail_event_id: number | string;
  readonly ready_reason: string | null;
  readonly ready_at_ms: number | string;
  readonly claim_worker_id: string | null;
  readonly claim_token: number | string | null;
  readonly claim_reason: string | null;
  readonly claim_expires_at_ms: number | string | null;
  readonly query_projection: unknown | null;
  readonly terminal: boolean;
  readonly parent: unknown | null;
}

interface NormalizedWaitLoadRow {
  readonly wait_id: string;
  readonly run_id: string;
  readonly namespace: string;
  readonly command_id: unknown;
  readonly kind: WaitRecord["kind"];
  readonly key: string;
  readonly ready_at_ms: number | string | null;
}

interface NormalizedSignalLoadRow {
  readonly signal_id: string;
  readonly run_id: string;
  readonly signal_name: string;
  readonly payload: unknown;
  readonly received_sequence: number | string;
  readonly consumed: boolean;
}

interface NormalizedActivityMapRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: ActivityMapTask;
  readonly input_manifest: PayloadRef;
  readonly input_count: number;
  readonly inputs: readonly PayloadRef[];
  readonly results: readonly (PayloadRef | null)[];
  readonly in_flight: number;
  readonly next_ordinal: number;
  readonly terminal: boolean;
}

interface NormalizedActivityMapItemRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: PayloadRef;
  readonly result: PayloadRef | null;
  readonly in_flight: boolean;
  readonly terminal: boolean;
}

interface NormalizedChildWorkflowMapRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: ChildWorkflowMapTask;
  readonly input_manifest: PayloadRef;
  readonly input_count: number;
  readonly inputs: readonly PayloadRef[];
  readonly outcomes: readonly (ChildWorkflowMapItemOutcome<unknown> | null)[];
  readonly in_flight: number;
  readonly next_ordinal: number;
  readonly terminal: boolean;
}

interface NormalizedChildWorkflowMapItemRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: PayloadRef;
  readonly outcome: ChildWorkflowMapItemOutcome<unknown> | null;
  readonly in_flight: boolean;
  readonly terminal: boolean;
}

interface NormalizedPayloadRootHistoryRow {
  readonly data: unknown;
}

interface NormalizedPayloadRootQueryProjectionRow {
  readonly projection: unknown;
}

interface NormalizedPayloadRootActivityTaskRow {
  readonly input: unknown;
}

interface NormalizedPayloadRootSignalRow {
  readonly payload: unknown;
}

interface NormalizedPayloadRootActivityMapRow {
  readonly input_manifest: unknown;
}

interface NormalizedPayloadRootActivityMapItemRow {
  readonly input: unknown;
  readonly result: unknown;
}

interface NormalizedPayloadRootChildWorkflowMapRow {
  readonly input_manifest: unknown;
}

interface NormalizedPayloadRootChildWorkflowMapItemRow {
  readonly input: unknown;
  readonly outcome: unknown;
}

interface NormalizedCounterRow {
  readonly name: string;
  readonly next_value: number | string;
}

function canUseTargetedWorkflowCommitProjectionUpdates(commit: WorkflowTaskCommit): boolean {
  if (
    // A cancelled command rewrites activity task rows and map descriptor rows,
    // neither of which the targeted projection covers.
    //
    // This is no longer hypothetical: losing-branch `select` cancellation gives
    // `cancelCommands` a runtime producer, so a workflow that races an activity
    // in a `select` drops off this path once per settle, and again on each cold
    // replay of that task. Measured on the fallback: 9.5x slower at 10 runs in
    // the table and 42x at 200, because `#loadNormalizedState` reads and
    // rewrites whole tables — the fast path is flat as the database grows and
    // this one is not. Narrowing it to admit plain-activity cancels needs a
    // state lookup (the commit alone cannot tell an activity id from a map
    // descriptor id) and row writes in `#cancelCommandOperationalState`; it is
    // tracked separately, not oversight.
    (commit.cancelCommands?.length ?? 0) > 0 ||
    (commit.scheduleActivityMaps?.length ?? 0) > 0 ||
    (commit.startChildWorkflows?.length ?? 0) > 0 ||
    (commit.scheduleChildWorkflowMaps?.length ?? 0) > 0 ||
    (commit.scheduleActivities ?? []).some((task) => task.mapItem !== null)
  ) {
    return false;
  }
  return !(commit.appendEvents ?? []).some((event) =>
    workflowCommitEventRequiresFullProjectionRewrite(event.data)
  );
}

function canUseSqlNativeWorkflowCommit(commit: WorkflowTaskCommit): boolean {
  if (
    // Cancelling a command tombstones activity rows and steps a map descriptor
    // through the engine; neither is expressible as one statement here.
    // Losing-branch `select` cancellation now produces this field, so the cost
    // of the bail is real rather than theoretical — see the measurement on
    // `canUseTargetedWorkflowCommitProjectionUpdates` above.
    (commit.cancelCommands?.length ?? 0) > 0 ||
    (commit.scheduleActivityMaps?.length ?? 0) > 0 ||
    (commit.scheduleChildWorkflowMaps?.length ?? 0) > 0 ||
    (commit.scheduleActivities ?? []).some((task) => task.mapItem !== null)
  ) {
    return false;
  }
  return !(commit.appendEvents ?? []).some((event) =>
    workflowCommitEventRequiresFullSqlNativeFallback(event.data)
  );
}

function workflowCommitEventRequiresFullProjectionRewrite(data: HistoryEventData): boolean {
  switch (data.kind) {
    case "WorkflowCompleted":
    case "WorkflowFailed":
    case "WorkflowCancelled":
    case "WorkflowContinuedAsNew":
    case "ActivityMapScheduled":
    case "ActivityMapCompleted":
    case "ActivityMapFailed":
    case "ChildWorkflowStartRequested":
    case "ChildWorkflowStarted":
    case "ChildWorkflowCompleted":
    case "ChildWorkflowFailed":
    case "ChildWorkflowCancelled":
    case "ChildWorkflowMapScheduled":
    case "ChildWorkflowMapCompleted":
    case "ChildWorkflowMapFailed":
      return true;
    default:
      return false;
  }
}

function workflowCommitEventRequiresFullSqlNativeFallback(data: HistoryEventData): boolean {
  switch (data.kind) {
    case "WorkflowContinuedAsNew":
    case "ActivityMapScheduled":
    case "ActivityMapCompleted":
    case "ActivityMapFailed":
    case "ChildWorkflowStarted":
    case "ChildWorkflowCompleted":
    case "ChildWorkflowFailed":
    case "ChildWorkflowCancelled":
    case "ChildWorkflowMapScheduled":
    case "ChildWorkflowMapCompleted":
    case "ChildWorkflowMapFailed":
      return true;
    default:
      return false;
  }
}

function workflowCommitEventClosesWorkflow(data: HistoryEventData): boolean {
  return (
    data.kind === "WorkflowCompleted" ||
    data.kind === "WorkflowFailed" ||
    data.kind === "WorkflowCancelled"
  );
}

function childTerminalUpdateFromAppendEvents(
  events: readonly { readonly data: HistoryEventData }[]
): ChildTerminalUpdate | null {
  for (const event of events) {
    if (event.data.kind === "WorkflowCompleted") {
      return { kind: "Completed", result: event.data.result };
    }
    if (event.data.kind === "WorkflowFailed") {
      return { kind: "Failed", failure: event.data.failure };
    }
    if (event.data.kind === "WorkflowCancelled") {
      return { kind: "Cancelled", reason: event.data.reason };
    }
  }
  return null;
}

function childTerminalHistoryData(
  parentLink: ChildParentWorkflowLink,
  terminal: ChildTerminalUpdate
): HistoryEventData {
  if (terminal.kind === "Completed") {
    return {
      kind: "ChildWorkflowCompleted",
      completed: {
        commandId: parentLink.commandId,
        result: terminal.result
      }
    };
  }
  if (terminal.kind === "Failed") {
    return {
      kind: "ChildWorkflowFailed",
      failed: {
        commandId: parentLink.commandId,
        failure: terminal.failure
      }
    };
  }
  return {
    kind: "ChildWorkflowCancelled",
    cancelled: {
      commandId: parentLink.commandId,
      reason: terminal.reason
    }
  };
}

function childTerminalReadyReason(terminal: ChildTerminalUpdate): WorkflowTaskReason {
  if (terminal.kind === "Completed") {
    return "ChildWorkflowCompleted";
  }
  if (terminal.kind === "Failed") {
    return "ChildWorkflowFailed";
  }
  return "ChildWorkflowCancelled";
}

/**
 * The descriptor's taken-slot count. Rows written before the count replaced a
 * set of in-flight ordinals hold that set instead, whose length is the same
 * count.
 */
function parseInFlight(value: unknown): number {
  return Array.isArray(value) ? value.length : (value as number);
}

function markWorkflowReady(state: WorkflowState, reason: WorkflowTaskReason): void {
  state.readyReason = reason;
  state.readyAtMs = 0;
}

function activityTimeoutDeadlineFromTask(
  task: ActivityTask,
  startedAtMs: number,
  heartbeatDeadlineAtMs: number | null
): number | null {
  const startToCloseDeadline =
    task.startToCloseTimeoutMs === null
      ? Number.POSITIVE_INFINITY
      : startedAtMs + Math.max(0, task.startToCloseTimeoutMs);
  const heartbeatDeadline = heartbeatDeadlineAtMs ?? Number.POSITIVE_INFINITY;
  const deadline = Math.min(startToCloseDeadline, heartbeatDeadline);
  return Number.isFinite(deadline) ? deadline : null;
}

function normalizedHistoryRow(runId: RunId, event: HistoryEvent): NormalizedHistoryRow {
  return {
    run_id: String(runId),
    event_id: Number(event.eventId),
    event_type: event.eventType,
    data: event.data
  };
}

function historyEventFromNormalizedRow(row: NormalizedHistorySelectRow): HistoryEvent {
  const data = parseJson<HistoryEventData>(JSON.stringify(row.data));
  const expectedType = historyEventType(data);
  if (row.event_type !== expectedType) {
    throw new Error(
      `normalized history event type mismatch: row has ${row.event_type}, data has ${expectedType}`
    );
  }
  return {
    eventId: eventId(Number(row.event_id)),
    eventType: row.event_type,
    data
  };
}

function workflowClaimRowsInDeterministicOrder(
  rows: readonly WorkflowClaimRow[]
): readonly WorkflowClaimRow[] {
  return [...rows].sort((left, right) => left.run_id.localeCompare(right.run_id));
}

function normalizedWorkflowRunRow(workflow: WorkflowState): NormalizedWorkflowRunRow {
  return {
    run_id: String(workflow.runId),
    namespace: workflow.namespace,
    workflow_id: workflow.workflowId,
    workflow_type_name: workflow.workflowType.name,
    workflow_type_version: workflow.workflowType.version,
    workflow_type: workflow.workflowType,
    task_queue: workflow.taskQueue,
    tail_event_id: Number(tailEventId(workflow)),
    ready_reason: workflow.readyReason,
    ready_at_ms: workflow.readyAtMs,
    claim_worker_id: workflow.claim === null ? null : String(workflow.claim.claim.workerId),
    claim_token: workflow.claim?.claim.token ?? null,
    claim_reason: workflow.claim?.reason ?? null,
    claim_expires_at_ms: workflow.claim?.expiresAtMs ?? null,
    query_projection: workflow.queryProjection,
    terminal: workflow.terminal,
    parent: workflow.parent
  };
}

function normalizedQueryProjectionRow(workflow: WorkflowState): NormalizedQueryProjectionRow {
  if (workflow.queryProjection === null) {
    throw new Error("query projection row requires a projection payload");
  }
  return {
    run_id: String(workflow.runId),
    namespace: workflow.namespace,
    workflow_id: workflow.workflowId,
    projection: workflow.queryProjection
  };
}

function normalizedWaitRow(
  waitId: string,
  namespace: string,
  wait: WaitRecord
): NormalizedWaitRow {
  return {
    wait_id: waitId,
    run_id: String(wait.runId),
    namespace,
    command_id: wait.commandId,
    kind: wait.kind,
    key: wait.key,
    ready_at_ms: wait.readyAt === null ? null : Number(wait.readyAt)
  };
}

function normalizedSignalRow(signalId: string, signal: SignalState): NormalizedSignalRow {
  return {
    signal_id: signalId,
    run_id: String(signal.runId),
    signal_name: String(signal.signalName),
    payload: signal.payload,
    received_sequence: signal.receivedSequence,
    consumed: signal.consumed
  };
}

function normalizedActivityTaskRow(activity: ActivityState): NormalizedActivityTaskRow {
  const timeoutDeadline = activityTimeoutDeadline(activity).deadline;
  return {
    activity_id: activity.task.activityId,
    namespace: activity.namespace,
    run_id: String(activity.workflow.runId),
    task: activity.task,
    input: activity.task.input,
    activity_name: String(activity.task.activityName),
    task_queue: String(activity.task.taskQueue),
    available_at_ms: activity.availableAtMs,
    claim_worker_id: activity.claim === null ? null : String(activity.claim.claim.workerId),
    claim_token: activity.claim?.claim.token ?? null,
    claim_started_at_ms: activity.claim?.startedAtMs ?? null,
    heartbeat_deadline_at_ms: activity.claim?.heartbeatDeadlineAtMs ?? null,
    timeout_deadline_at_ms: Number.isFinite(timeoutDeadline) ? timeoutDeadline : null,
    claim_expires_at_ms: activity.claim?.expiresAtMs ?? null,
    claim_lease_duration_ms: activity.claim?.leaseDurationMs ?? null,
    terminal_event_id: activity.terminalEventId === null ? null : Number(activity.terminalEventId),
    map_command_key:
      activity.task.mapItem === null ? null : commandKey(activity.task.mapItem.mapCommandId),
    map_item_ordinal: activity.task.mapItem?.itemOrdinal ?? null
  };
}

function normalizedActivityTaskRowFromTask(
  namespace: string,
  task: ActivityTask
): NormalizedActivityTaskRow {
  return {
    activity_id: task.activityId,
    namespace,
    run_id: String(task.runId),
    task,
    input: task.input,
    activity_name: String(task.activityName),
    task_queue: String(task.taskQueue),
    available_at_ms: 0,
    claim_worker_id: null,
    claim_token: null,
    claim_started_at_ms: null,
    heartbeat_deadline_at_ms: null,
    timeout_deadline_at_ms: null,
    claim_expires_at_ms: null,
    claim_lease_duration_ms: null,
    terminal_event_id: null,
    map_command_key: task.mapItem === null ? null : commandKey(task.mapItem.mapCommandId),
    map_item_ordinal: task.mapItem?.itemOrdinal ?? null
  };
}

function normalizedActivityMapRow(
  commandKey: string,
  map: ActivityMapState
): NormalizedActivityMapRow {
  return {
    command_key: commandKey,
    namespace: map.namespace,
    run_id: String(map.workflow.runId),
    task: map.task,
    input_manifest: map.task.inputManifest,
    input_count: map.inputs.length,
    inputs: map.inputs,
    results: map.results,
    in_flight: map.inFlight,
    next_ordinal: map.nextOrdinal,
    terminal: map.terminal
  };
}

function normalizedActivityMapItemRows(
  commandKey: string,
  map: ActivityMapState
): NormalizedActivityMapItemRow[] {
  const rows: NormalizedActivityMapItemRow[] = [];
  for (let ordinal = 0; ordinal < map.inputs.length; ordinal += 1) {
    const result = map.results[ordinal] ?? null;
    rows.push({
      command_key: commandKey,
      namespace: map.namespace,
      run_id: String(map.workflow.runId),
      item_ordinal: ordinal,
      input: map.inputs[ordinal] as PayloadRef,
      result,
      in_flight: mapItemInFlight(map.nextOrdinal, map.terminal, ordinal, result),
      terminal: map.terminal || result !== null
    });
  }
  return rows;
}

function normalizedChildWorkflowMapRow(
  commandKey: string,
  map: ChildWorkflowMapState
): NormalizedChildWorkflowMapRow {
  return {
    command_key: commandKey,
    namespace: map.namespace,
    run_id: String(map.workflow.runId),
    task: map.task,
    input_manifest: map.task.inputManifest,
    input_count: map.inputs.length,
    inputs: map.inputs,
    outcomes: map.outcomes,
    in_flight: map.inFlight,
    next_ordinal: map.nextOrdinal,
    terminal: map.terminal
  };
}

function normalizedChildWorkflowMapItemRows(
  commandKey: string,
  map: ChildWorkflowMapState
): NormalizedChildWorkflowMapItemRow[] {
  const rows: NormalizedChildWorkflowMapItemRow[] = [];
  for (let ordinal = 0; ordinal < map.inputs.length; ordinal += 1) {
    const outcome = map.outcomes[ordinal] ?? null;
    rows.push({
      command_key: commandKey,
      namespace: map.namespace,
      run_id: String(map.workflow.runId),
      item_ordinal: ordinal,
      input: map.inputs[ordinal] as PayloadRef,
      outcome,
      in_flight: mapItemInFlight(map.nextOrdinal, map.terminal, ordinal, outcome),
      terminal: map.terminal || outcome !== null
    });
  }
  return rows;
}

function sqlIdentifier(identifier: string): string {
  if (!/^[a-z_][a-z0-9_]*$/iu.test(identifier)) {
    throw new Error(`invalid Postgres identifier: ${identifier}`);
  }
  return `"${identifier.replaceAll("\"", "\"\"")}"`;
}

function derivedSqlIdentifier(base: string, suffix: string): string {
  const suffixPart = `_${suffix}`;
  if (base.length + suffixPart.length <= 63) {
    return sqlIdentifier(`${base}${suffixPart}`);
  }
  const hash = createHash("sha1").update(base).digest("hex").slice(0, 8);
  const prefixLength = Math.max(1, 63 - suffixPart.length - hash.length - 1);
  return sqlIdentifier(`${base.slice(0, prefixLength)}_${hash}${suffixPart}`);
}

async function postgresWalStats(client: PoolClient): Promise<
  Pick<
    PostgresBackendStatsSnapshot,
    | "walBytes"
    | "walRecords"
    | "walFpi"
    | "walBuffersFull"
    | "walWrite"
    | "walSync"
    | "walWriteTimeMs"
    | "walSyncTimeMs"
  >
> {
  try {
    const result = await client.query(`
      select
        wal_bytes,
        wal_records,
        wal_fpi,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time
      from pg_stat_wal
    `);
    const row = result.rows[0] as Record<string, unknown> | undefined;
    if (row === undefined) {
      return emptyPostgresWalStats();
    }
    return {
      walBytes: postgresStatNumber(row, "wal_bytes"),
      walRecords: postgresStatNumber(row, "wal_records"),
      walFpi: postgresStatNumber(row, "wal_fpi"),
      walBuffersFull: postgresStatNumber(row, "wal_buffers_full"),
      walWrite: postgresStatNumber(row, "wal_write"),
      walSync: postgresStatNumber(row, "wal_sync"),
      walWriteTimeMs: postgresStatNumber(row, "wal_write_time"),
      walSyncTimeMs: postgresStatNumber(row, "wal_sync_time")
    };
  } catch (error) {
    if (postgresErrorCode(error) === "42P01" || postgresErrorCode(error) === "42703") {
      return emptyPostgresWalStats();
    }
    throw error;
  }
}

async function postgresStatementStats(
  client: PoolClient
): Promise<{
  readonly statements: readonly PostgresStatementStatsSnapshot[];
  readonly unavailable: string | null;
}> {
  try {
    const result = await client.query(`
      select
        queryid::text as query_id,
        query,
        calls,
        total_exec_time
      from pg_stat_statements
      where dbid = (
        select oid
        from pg_database
        where datname = current_database()
      )
    `);
    return {
      statements: result.rows.map((raw) => {
        const row = raw as Record<string, unknown>;
        return {
          queryId: String(row.query_id ?? ""),
          query: String(row.query ?? ""),
          calls: postgresStatNumber(row, "calls"),
          totalExecTimeMs: postgresStatNumber(row, "total_exec_time")
        };
      }),
      unavailable: null
    };
  } catch (error) {
    if (
      postgresErrorCode(error) === "42P01" ||
      postgresErrorCode(error) === "42703" ||
      postgresErrorCode(error) === "55000"
    ) {
      // The same discard this used to do, except the reason survives. `42P01`
      // is the view not existing at all, which is what a server without
      // `shared_preload_libraries=pg_stat_statements` produces.
      return { statements: [], unavailable: statementStatsUnavailableMessage(postgresErrorReason(error)) };
    }
    throw error;
  }
}

/**
 * What to do when `pg_stat_statements` is missing. Named here once so the
 * message a user hits and the fixture that fixes it cannot drift apart, and so
 * the fixture is reachable by grep from the error rather than only from prose.
 *
 * Mirrors `PG_STAT_STATEMENTS_PRELOAD_HINT` in
 * `benchtools/src/bin/durust-benchmark-workload.rs`; the two runtimes hit the
 * same server limitation and should say the same thing about it.
 */
const PG_STAT_STATEMENTS_PRELOAD_HINT =
  "pg_stat_statements has to be loaded at server start, which `create extension` cannot do on " +
  "its own: run the server with `-c shared_preload_libraries=pg_stat_statements`. The checked-in " +
  "fixture does exactly that — `docker compose -f tests/fixtures/postgres.compose.yml up -d " +
  "--wait`, then DURUST_POSTGRES_URL=postgres://durable:durable@127.0.0.1:55432/durable";

function statementStatsUnavailableMessage(reason: string): string {
  return `pg_stat_statements snapshot failed (${reason}). ${PG_STAT_STATEMENTS_PRELOAD_HINT}`;
}

function postgresErrorReason(error: unknown): string {
  const code = postgresErrorCode(error);
  const message = error instanceof Error ? error.message : String(error);
  return code === null || code === undefined ? message : `${code}: ${message}`;
}

/**
 * Records the setup failure instead of discarding it.
 *
 * Returns the reason rather than throwing, because a missing
 * `pg_stat_statements` must not stop a provider from opening — it is optional
 * instrumentation, and every non-benchmark caller works fine without it. What
 * it must not do is *vanish*: a benchmark that gates on statement stats needs
 * to fail with the cause, not with `expected null not to be null`.
 */
async function ensureOptionalPostgresStatementStats(pool: Pool): Promise<string | null> {
  try {
    await pool.query("create extension if not exists pg_stat_statements");
    return null;
  } catch (error) {
    if (isOptionalPostgresStatementStatsSetupError(error)) {
      return statementStatsUnavailableMessage(postgresErrorReason(error));
    }
    throw error;
  }
}

function isOptionalPostgresStatementStatsSetupError(error: unknown): boolean {
  switch (postgresErrorCode(error)) {
    case "0A000":
    case "42501":
    case "42704":
    case "55000":
    case "58P01":
      return true;
    default:
      return false;
  }
}

function emptyPostgresWalStats(): Pick<
  PostgresBackendStatsSnapshot,
  | "walBytes"
  | "walRecords"
  | "walFpi"
  | "walBuffersFull"
  | "walWrite"
  | "walSync"
  | "walWriteTimeMs"
  | "walSyncTimeMs"
> {
  return {
    walBytes: 0,
    walRecords: 0,
    walFpi: 0,
    walBuffersFull: 0,
    walWrite: 0,
    walSync: 0,
    walWriteTimeMs: 0,
    walSyncTimeMs: 0
  };
}

function postgresStatNumber(row: Record<string, unknown>, key: string): number {
  const value = row[key];
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  throw new Error(`unexpected Postgres stats value for ${key}`);
}

function postgresErrorCode(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error
    ? String((error as { readonly code?: unknown }).code)
    : undefined;
}

function parsePostgresJson(value: unknown): unknown {
  return parseJson<unknown>(JSON.stringify(value));
}

function postgresOptionalNumber(value: number | string | null): number | null {
  return value === null ? null : postgresRequiredNumber(value);
}

function postgresRequiredNumber(value: number | string | null): number {
  if (value === null) {
    throw new Error("expected Postgres numeric value");
  }
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error(`invalid Postgres numeric value: ${String(value)}`);
  }
  return numeric;
}
