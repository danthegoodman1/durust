/**
 * Server-side counters around a Postgres benchmark run, read through `pg`
 * from the run's own database: the provider itself lives in Rust behind
 * `@durust/native`, so the instrumentation lives with the benchmark that
 * reads it.
 */
import { Client } from "pg";

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
   * Why `statements` is empty, when it is empty for a reason. `null` means
   * the statements were collected, or the run issued none; a string means
   * the snapshot failed, and it carries the server's own error plus the hint
   * that fixes it, so a benchmark that gates on statement stats fails with
   * the cause.
   */
  readonly statementStatsUnavailable: string | null;
}

export interface PostgresStatementStatsSnapshot {
  readonly queryId: string;
  readonly query: string;
  readonly calls: number;
  readonly totalExecTimeMs: number;
}

/**
 * What to do when `pg_stat_statements` is missing. Named once so the message
 * a user hits and the fixture that fixes it cannot drift apart. Mirrors
 * `PG_STAT_STATEMENTS_PRELOAD_HINT` in
 * `benchtools/src/bin/durust-benchmark-workload.rs`.
 */
const PG_STAT_STATEMENTS_PRELOAD_HINT =
  "pg_stat_statements has to be loaded at server start, which `create extension` cannot do on " +
  "its own: run the server with `-c shared_preload_libraries=pg_stat_statements`. The checked-in " +
  "fixture does exactly that — `docker compose -f tests/fixtures/postgres.compose.yml up -d " +
  "--wait`, then DURUST_POSTGRES_URL=postgres://durable:durable@127.0.0.1:55432/durable";

/** Reads every counter the report compares, from the database `url` names. */
export class PostgresStatsReader {
  readonly #url: string;
  #setupFailure: string | null = null;

  constructor(url: string) {
    this.#url = url;
  }

  /**
   * Enables `pg_stat_statements` when the server allows it. A server that
   * cannot load the extension keeps the reason, which every later snapshot
   * reports instead of an empty statement list.
   */
  async prepare(): Promise<void> {
    await this.#withClient(async (client) => {
      try {
        await client.query("create extension if not exists pg_stat_statements");
        await client.query("select pg_stat_statements_reset()").catch(() => undefined);
      } catch (error) {
        if (!isOptionalStatementStatsSetupError(error)) {
          throw error;
        }
        this.#setupFailure = statementStatsUnavailableMessage(postgresErrorReason(error));
      }
    });
  }

  async snapshot(): Promise<PostgresBackendStatsSnapshot> {
    return await this.#withClient(async (client) => {
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
      const wal = await walStats(client);
      const statementStats = await statementStatsOf(client);
      return {
        ...wal,
        xactCommit: statNumber(databaseRow, "xact_commit"),
        xactRollback: statNumber(databaseRow, "xact_rollback"),
        rowsReturned: statNumber(databaseRow, "tup_returned"),
        rowsFetched: statNumber(databaseRow, "tup_fetched"),
        rowsInserted: statNumber(databaseRow, "tup_inserted"),
        rowsUpdated: statNumber(databaseRow, "tup_updated"),
        rowsDeleted: statNumber(databaseRow, "tup_deleted"),
        blocksRead: statNumber(databaseRow, "blks_read"),
        blocksHit: statNumber(databaseRow, "blks_hit"),
        tempFiles: statNumber(databaseRow, "temp_files"),
        tempBytes: statNumber(databaseRow, "temp_bytes"),
        deadlocks: statNumber(databaseRow, "deadlocks"),
        blockReadTimeMs: statNumber(databaseRow, "blk_read_time"),
        blockWriteTimeMs: statNumber(databaseRow, "blk_write_time"),
        activeConnections:
          connectionRow === undefined ? 0 : statNumber(connectionRow, "active_connections"),
        statements: statementStats.statements,
        statementStatsUnavailable: this.#setupFailure ?? statementStats.unavailable
      };
    });
  }

  async #withClient<T>(run: (client: Client) => Promise<T>): Promise<T> {
    const client = new Client({ connectionString: this.#url });
    await client.connect();
    try {
      return await run(client);
    } finally {
      await client.end();
    }
  }
}

type WalStats = Pick<
  PostgresBackendStatsSnapshot,
  | "walBytes"
  | "walRecords"
  | "walFpi"
  | "walBuffersFull"
  | "walWrite"
  | "walSync"
  | "walWriteTimeMs"
  | "walSyncTimeMs"
>;

async function walStats(client: Client): Promise<WalStats> {
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
      return emptyWalStats();
    }
    return {
      walBytes: statNumber(row, "wal_bytes"),
      walRecords: statNumber(row, "wal_records"),
      walFpi: statNumber(row, "wal_fpi"),
      walBuffersFull: statNumber(row, "wal_buffers_full"),
      walWrite: statNumber(row, "wal_write"),
      walSync: statNumber(row, "wal_sync"),
      walWriteTimeMs: statNumber(row, "wal_write_time"),
      walSyncTimeMs: statNumber(row, "wal_sync_time")
    };
  } catch (error) {
    if (postgresErrorCode(error) === "42P01" || postgresErrorCode(error) === "42703") {
      return emptyWalStats();
    }
    throw error;
  }
}

async function statementStatsOf(client: Client): Promise<{
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
          calls: statNumber(row, "calls"),
          totalExecTimeMs: statNumber(row, "total_exec_time")
        };
      }),
      unavailable: null
    };
  } catch (error) {
    const code = postgresErrorCode(error);
    if (code === "42P01" || code === "42703" || code === "55000") {
      return { statements: [], unavailable: statementStatsUnavailableMessage(postgresErrorReason(error)) };
    }
    throw error;
  }
}

function statementStatsUnavailableMessage(reason: string): string {
  return `pg_stat_statements snapshot failed (${reason}). ${PG_STAT_STATEMENTS_PRELOAD_HINT}`;
}

function postgresErrorReason(error: unknown): string {
  const code = postgresErrorCode(error);
  const message = error instanceof Error ? error.message : String(error);
  return code === undefined ? message : `${code}: ${message}`;
}

function isOptionalStatementStatsSetupError(error: unknown): boolean {
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

function emptyWalStats(): WalStats {
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

function statNumber(row: Record<string, unknown>, key: string): number {
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
