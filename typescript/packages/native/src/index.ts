/**
 * `NativeBackend`: the Rust providers behind the TypeScript `DurableBackend`
 * contract. Each call encodes its request as msgpack, crosses into the
 * `durust-node` addon, and decodes the outcome the Rust side produced in the
 * same shape the contract describes, so the worker, the client, and the
 * shared conformance suite see an ordinary provider.
 */
import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { decode, encode } from "@msgpack/msgpack";
import {
  ProviderError,
  timestampMs,
  type ProviderErrorCode,
  type ActivityHeartbeatOutcome,
  type ActivityHeartbeatRequest,
  type ClaimActivityBatchOptions,
  type ClaimActivityOptions,
  type ClaimWorkflowBatchOptions,
  type ClaimWorkflowTaskOptions,
  type ClaimedActivityTask,
  eventId,
  type ClaimedWorkflowTask,
  type EventId,
  type CompleteActivitiesOutcome,
  type CompleteActivitiesRequest,
  type CompleteActivityOutcome,
  type CompleteActivityRequest,
  type DurableBackend,
  type FailActivityOutcome,
  type FailActivityRequest,
  type FireDueTimersOutcome,
  type FireDueTimersRequest,
  type HistoryChunk,
  type QueryWorkflowOutcome,
  type QueryWorkflowRequest,
  type ReadSignalInboxRequest,
  type ReleaseWorkflowTaskOptions,
  type SignalInboxRecord,
  type SignalWorkflowOutcome,
  type SignalWorkflowRequest,
  type StartWorkflowOutcome,
  type StartWorkflowRequest,
  type StreamHistoryRequest,
  type TimestampMs,
  type TimeoutDueActivitiesOutcome,
  type TimeoutDueActivitiesRequest,
  type WorkerId,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit
} from "@durust/core";

/** The addon class as napi-rs exports it: snake_case Rust methods in camelCase. */
interface NativeHandle {
  advanceTimeTo(nowMs: number): void;
  close(): void;
  destroy(): Promise<void>;
  currentTime(): Promise<number>;
  startWorkflow(req: Uint8Array): Promise<Uint8Array>;
  claimWorkflowTask(workerId: string, opts: Uint8Array): Promise<Uint8Array>;
  claimWorkflowTasks(workerId: string, opts: Uint8Array): Promise<Uint8Array>;
  streamHistory(req: Uint8Array): Promise<Uint8Array>;
  commitWorkflowTask(claim: Uint8Array, commit: Uint8Array): Promise<Uint8Array>;
  releaseWorkflowTask(claim: Uint8Array, options: Uint8Array): Promise<void>;
  claimActivityTask(workerId: string, opts: Uint8Array): Promise<Uint8Array>;
  claimActivityTasks(workerId: string, opts: Uint8Array): Promise<Uint8Array>;
  completeActivity(req: Uint8Array): Promise<Uint8Array>;
  completeActivities(req: Uint8Array): Promise<Uint8Array>;
  failActivity(req: Uint8Array): Promise<Uint8Array>;
  heartbeatActivity(req: Uint8Array): Promise<Uint8Array>;
  fireDueTimers(req: Uint8Array): Promise<Uint8Array>;
  timeoutDueActivities(req: Uint8Array): Promise<Uint8Array>;
  signalWorkflow(req: Uint8Array): Promise<Uint8Array>;
  readSignalInbox(req: Uint8Array): Promise<Uint8Array>;
  queryWorkflow(req: Uint8Array): Promise<Uint8Array>;
  gcPayloadBlobs(req: Uint8Array): Promise<Uint8Array>;
  payloadRoots(): Promise<Uint8Array>;
}

interface NativeModule {
  readonly NativeBackend: {
    memory(options?: Uint8Array): NativeHandle;
    sqlite(path: string, options?: Uint8Array): NativeHandle;
  };
  connectPostgres(url: string, options?: Uint8Array): Promise<NativeHandle>;
}

const require = createRequire(import.meta.url);

/**
 * The napi-rs target name for this process: the suffix of the addon file and
 * of the platform package that ships it. Linux builds link glibc.
 */
export function nativeTarget(): string {
  const { platform, arch } = process;
  if (platform === "darwin" && (arch === "arm64" || arch === "x64")) {
    return `darwin-${arch}`;
  }
  if (platform === "linux" && (arch === "arm64" || arch === "x64")) {
    if (isMusl()) {
      throw new Error("@durust/native ships glibc builds only; this Linux links musl");
    }
    return `linux-${arch}-gnu`;
  }
  throw new Error(`@durust/native has no build for ${platform}-${arch}`);
}

function isMusl(): boolean {
  const report = process.report?.getReport() as
    | { header?: { glibcVersionRuntime?: string }; sharedObjects?: string[] }
    | undefined;
  if (report?.header?.glibcVersionRuntime) {
    return false;
  }
  return report?.sharedObjects?.some((file) => file.includes("ld-musl-")) ?? false;
}

/** Where a locally built addon lives: next to this package's `package.json`. */
export function nativeModulePath(): string {
  return fileURLToPath(new URL(`../durust-node.${nativeTarget()}.node`, import.meta.url));
}

function platformPackage(): string {
  return `@durust/native-${nativeTarget()}`;
}

/**
 * Whether an addon can be loaded: `DURUST_NATIVE_LIBRARY_PATH`, a local
 * build, or the installed platform package, in that order.
 */
export function nativeModuleAvailable(): boolean {
  if (process.env.DURUST_NATIVE_LIBRARY_PATH) {
    return existsSync(process.env.DURUST_NATIVE_LIBRARY_PATH);
  }
  if (existsSync(nativeModulePath())) {
    return true;
  }
  try {
    require.resolve(platformPackage());
    return true;
  } catch {
    return false;
  }
}

let loaded: NativeModule | null = null;

function nativeModule(): NativeModule {
  if (loaded !== null) {
    return loaded;
  }
  const override = process.env.DURUST_NATIVE_LIBRARY_PATH;
  if (override) {
    loaded = require(override) as NativeModule;
    return loaded;
  }
  const local = nativeModulePath();
  if (existsSync(local)) {
    loaded = require(local) as NativeModule;
    return loaded;
  }
  try {
    loaded = require(platformPackage()) as NativeModule;
  } catch (error) {
    throw new Error(
      `@durust/native could not load its addon: no ${platformPackage()} is installed and no ` +
        `local build exists at ${local}; run \`npm run build:native --workspace @durust/native\` ` +
        "in the workspace, or reinstall with optional dependencies enabled",
      { cause: error }
    );
  }
  return loaded;
}

/**
 * The addon reports a refused call with the message the TypeScript contract
 * uses for the same condition; this turns it back into the typed error.
 */
function providerErrorFromNative(error: unknown): unknown {
  if (!(error instanceof Error)) {
    return error;
  }
  const codes: readonly (readonly [string, ProviderErrorCode])[] = [
    ["stale workflow task lease", "StaleWorkflowLease"],
    ["stale activity task lease", "StaleActivityLease"],
    ["terminal workflow rejects workflow-visible mutations", "TerminalWorkflow"],
    ["terminal workflow rejects signals", "TerminalWorkflowSignal"],
    ["workflow not found: ", "WorkflowNotFound"],
    ["maxInFlight must be a positive integer", "InvalidMapOptions"]
  ];
  for (const [text, code] of codes) {
    if (error.message.includes(text)) {
      return new ProviderError(code, error.message);
    }
  }
  return error;
}

function pack(value: unknown): Uint8Array {
  return encode(value);
}

function unpack<T>(bytes: Uint8Array): T {
  return decode(bytes) as T;
}

/** Where payloads above the inline threshold are stored. */
export type NativeBlobStoreOptions =
  | { readonly kind: "Memory" }
  | { readonly kind: "LocalDirectory"; readonly root: string; readonly prefix?: string }
  | {
      readonly kind: "S3";
      readonly bucket: string;
      readonly endpoint: string;
      readonly region: string;
      readonly prefix?: string;
      readonly accessKeyId: string;
      readonly secretAccessKey: string;
    };

export interface NativePayloadOptions {
  /** Payloads at or under this many bytes stay inline in the provider. */
  readonly inlineThresholdBytes?: number;
  readonly blobStore: NativeBlobStoreOptions;
}

export interface NativeClockOptions {
  /**
   * The clock every provider follows; `Date.now`, read at call time, by
   * default. Each call moves the Rust provider clock up to this reading
   * first, so a stubbed clock drives leases, deadlines, retries, due scans,
   * and `currentTime()`.
   */
  readonly nowMs?: () => number;
}

export interface NativeBackendOptions extends NativeClockOptions {
  /** Offload large payloads to a blob store through the Rust payload backend. */
  readonly payload?: NativePayloadOptions;
}

export interface NativePostgresOptions extends NativeBackendOptions {
  /** The schema every table lives in; `durust` by default. */
  readonly schema?: string;
  readonly maxPoolSize?: number;
  readonly logicalShards?: number;
  readonly physicalPartitions?: number;
  readonly statementTimeoutMs?: number;
  readonly lockTimeoutMs?: number;
}

export interface PayloadGcRequest {
  /** Report what a sweep would delete without deleting it. */
  readonly dryRun?: boolean;
  /** Blobs modified more recently than this are kept; one hour by default. */
  readonly minAgeMs?: number;
}

export interface PayloadGcOutcome {
  readonly scannedBlobs: number;
  readonly retainedBlobs: number;
  readonly deletedBlobs: number;
  readonly failedBlobs: number;
}

function packOptions(options: NativeBackendOptions | NativePostgresOptions): Uint8Array {
  const { nowMs: _nowMs, ...wire } = options;
  return pack(wire);
}

export class NativeBackend implements DurableBackend {
  readonly #handle: NativeHandle;
  readonly #nowMs: () => number;

  private constructor(handle: NativeHandle, nowMs: () => number) {
    this.#handle = handle;
    this.#nowMs = nowMs;
  }

  /** The Rust in-memory provider. */
  static memory(options: NativeBackendOptions = {}): NativeBackend {
    return new NativeBackend(
      nativeModule().NativeBackend.memory(packOptions(options)),
      options.nowMs ?? (() => Date.now())
    );
  }

  /** The Rust SQLite provider over the database file at `path`. */
  static sqlite(path: string, options: NativeBackendOptions = {}): NativeBackend {
    return new NativeBackend(
      nativeModule().NativeBackend.sqlite(path, packOptions(options)),
      options.nowMs ?? (() => Date.now())
    );
  }

  /**
   * The Rust Postgres provider. Connecting runs the schema migration, so
   * this is asynchronous.
   */
  static async postgres(url: string, options: NativePostgresOptions = {}): Promise<NativeBackend> {
    const handle = await nativeModule().connectPostgres(url, packOptions(options));
    return new NativeBackend(handle, options.nowMs ?? (() => Date.now()));
  }

  async #invoke<T>(run: (handle: NativeHandle) => Promise<T>): Promise<T> {
    this.#handle.advanceTimeTo(this.#nowMs());
    try {
      return await run(this.#handle);
    } catch (error) {
      throw providerErrorFromNative(error);
    }
  }

  /**
   * Releases the provider. Postgres connections close once every call in
   * flight has returned; later calls fail as closed.
   */
  close(): void {
    this.#handle.close();
  }

  /**
   * Deletes the provider's own storage (the Postgres schema) and closes the
   * backend. Memory and SQLite have nothing to delete; the SQLite file stays
   * for its owner to remove.
   */
  async destroy(): Promise<void> {
    await this.#invoke((handle) => handle.destroy());
  }

  async currentTime(): Promise<TimestampMs> {
    return timestampMs(await this.#invoke((handle) => handle.currentTime()));
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    return unpack(await this.#invoke((handle) => handle.startWorkflow(pack(req))));
  }

  async claimWorkflowTask(
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    return unpack(await this.#invoke((handle) => handle.claimWorkflowTask(String(workerId), pack(opts))));
  }

  async claimWorkflowTasks(
    workerId: WorkerId | string,
    opts: ClaimWorkflowBatchOptions
  ): Promise<readonly ClaimedWorkflowTask[]> {
    return unpack(await this.#invoke((handle) => handle.claimWorkflowTasks(String(workerId), pack(opts))));
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    return unpack(await this.#invoke((handle) => handle.streamHistory(pack(req))));
  }

  async commitWorkflowTask(claim: WorkflowTaskClaim, commit: WorkflowTaskCommit): Promise<EventId> {
    return eventId(
      Number(unpack(await this.#invoke((handle) => handle.commitWorkflowTask(pack(claim), pack(commit)))))
    );
  }

  async releaseWorkflowTask(claim: WorkflowTaskClaim, options?: ReleaseWorkflowTaskOptions): Promise<void> {
    await this.#invoke((handle) => handle.releaseWorkflowTask(pack(claim), pack(options ?? {})));
  }

  async claimActivityTask(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    return unpack(await this.#invoke((handle) => handle.claimActivityTask(String(workerId), pack(opts))));
  }

  async claimActivityTasks(
    workerId: WorkerId | string,
    opts: ClaimActivityBatchOptions
  ): Promise<readonly ClaimedActivityTask[]> {
    return unpack(await this.#invoke((handle) => handle.claimActivityTasks(String(workerId), pack(opts))));
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    return unpack(await this.#invoke((handle) => handle.completeActivity(pack(req))));
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    return unpack(await this.#invoke((handle) => handle.completeActivities(pack(req))));
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    return unpack(await this.#invoke((handle) => handle.failActivity(pack(req))));
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    return unpack(await this.#invoke((handle) => handle.heartbeatActivity(pack(req))));
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    return unpack(await this.#invoke((handle) => handle.fireDueTimers(pack(req))));
  }

  async timeoutDueActivities(req: TimeoutDueActivitiesRequest): Promise<TimeoutDueActivitiesOutcome> {
    return unpack(await this.#invoke((handle) => handle.timeoutDueActivities(pack(req))));
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    return unpack(await this.#invoke((handle) => handle.signalWorkflow(pack(req))));
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    return unpack(await this.#invoke((handle) => handle.readSignalInbox(pack(req))));
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    return unpack(await this.#invoke((handle) => handle.queryWorkflow(pack(req))));
  }

  /**
   * Sweeps the payload blob store: every blob no root reaches and older than
   * the grace period is deleted. Providers without a blob store report zero
   * blobs scanned.
   */
  async gcPayloadBlobs(req: PayloadGcRequest = {}): Promise<PayloadGcOutcome> {
    return unpack(await this.#invoke((handle) => handle.gcPayloadBlobs(pack(req))));
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    return unpack(await this.#invoke((handle) => handle.payloadRoots()));
  }
}
