import { createHash, createHmac } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import {
  decodePayload,
  digestBytes,
  encodePayload,
  toBlobRef,
  type ActivityHeartbeatOutcome,
  type ActivityHeartbeatRequest,
  type ActivityMapTask,
  type ChildWorkflowMapTask,
  type ClaimActivityOptions,
  type ClaimWorkflowTaskOptions,
  type ClaimedActivityTask,
  type ClaimedWorkflowTask,
  type CommitOutcome,
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
  type BlobPayloadRef,
  type CodecId,
  type InlinePayloadRef,
  type PayloadRef,
  type QueryWorkflowOutcome,
  type QueryWorkflowRequest,
  type ReadSignalInboxRequest,
  type SignalInboxRecord,
  type SignalWorkflowOutcome,
  type SignalWorkflowRequest,
  type SchemaAdapter,
  type StartWorkflowOutcome,
  type StartWorkflowRequest,
  type StreamHistoryRequest,
  type TimeoutDueActivitiesOutcome,
  type TimeoutDueActivitiesRequest,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit
} from "@durust/core";

export interface PayloadBlobStore {
  put(bytes: Uint8Array, digest: string): Promise<string>;
  get(uri: string): Promise<Uint8Array>;
  delete(uri: string): Promise<void>;
  list(): Promise<readonly string[]>;
  owns(uri: string): boolean;
}

export interface LocalDirectoryBlobStoreOptions {
  readonly root: string;
  readonly prefix?: string;
}

export class LocalDirectoryBlobStore implements PayloadBlobStore {
  readonly #root: string;
  readonly #prefix: string;

  constructor(options: LocalDirectoryBlobStoreOptions) {
    this.#root = resolve(options.root);
    this.#prefix = sanitizePrefix(options.prefix ?? "payloads");
  }

  async put(bytes: Uint8Array, digest: string): Promise<string> {
    const path = this.#pathForDigest(digest);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, bytes);
    return pathToFileUri(path);
  }

  async get(uri: string): Promise<Uint8Array> {
    this.#assertOwned(uri);
    return new Uint8Array(await readFile(fileUriToPath(uri)));
  }

  async delete(uri: string): Promise<void> {
    this.#assertOwned(uri);
    await rm(fileUriToPath(uri), { force: true });
  }

  async list(): Promise<readonly string[]> {
    const dir = join(this.#root, this.#prefix);
    try {
      const entries = await readdir(dir, { recursive: true, withFileTypes: true });
      return entries
        .filter((entry) => entry.isFile())
        .map((entry) => pathToFileUri(join(entry.parentPath, entry.name)))
        .sort();
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        return [];
      }
      throw error;
    }
  }

  owns(uri: string): boolean {
    try {
      this.#assertOwned(uri);
      return true;
    } catch {
      return false;
    }
  }

  #pathForDigest(digest: string): string {
    const hash = digest.replace(/^sha256:/, "");
    return join(this.#root, this.#prefix, `${hash}.bin`);
  }

  #assertOwned(uri: string): void {
    const path = fileUriToPath(uri);
    const root = join(this.#root, this.#prefix);
    if (path !== root && !path.startsWith(`${root}/`)) {
      throw new Error(`blob URI is not owned by this store: ${uri}`);
    }
  }
}

export interface S3CompatibleBlobStoreOptions {
  readonly bucket: string;
  readonly endpoint: string;
  readonly region: string;
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly prefix?: string;
  readonly forcePathStyle?: boolean;
}

export class S3CompatibleBlobStore implements PayloadBlobStore {
  readonly #bucket: string;
  readonly #endpoint: URL;
  readonly #region: string;
  readonly #accessKeyId: string;
  readonly #secretAccessKey: string;
  readonly #prefix: string;
  readonly #forcePathStyle: boolean;

  constructor(options: S3CompatibleBlobStoreOptions) {
    this.#bucket = options.bucket;
    this.#endpoint = new URL(options.endpoint);
    this.#region = options.region;
    this.#accessKeyId = options.accessKeyId;
    this.#secretAccessKey = options.secretAccessKey;
    this.#prefix = sanitizePrefix(options.prefix ?? "payloads");
    this.#forcePathStyle = options.forcePathStyle ?? true;
  }

  async put(bytes: Uint8Array, digest: string): Promise<string> {
    const key = this.#keyForDigest(digest);
    await this.#request("PUT", key, [], bytes);
    return this.#uriForKey(key);
  }

  async get(uri: string): Promise<Uint8Array> {
    const response = await this.#request("GET", this.#keyFromUri(uri), []);
    return new Uint8Array(await response.arrayBuffer());
  }

  async delete(uri: string): Promise<void> {
    await this.#request("DELETE", this.#keyFromUri(uri), []);
  }

  async list(): Promise<readonly string[]> {
    const keys: string[] = [];
    let continuationToken: string | undefined;
    do {
      const params: [string, string][] = [
        ["list-type", "2"],
        ["prefix", `${this.#prefix}/`]
      ];
      if (continuationToken !== undefined) {
        params.push(["continuation-token", continuationToken]);
      }
      const response = await this.#request("GET", null, params);
      const xml = await response.text();
      keys.push(...extractXmlTags(xml, "Key").filter((key) => key.startsWith(`${this.#prefix}/`)));
      const truncated = extractXmlTags(xml, "IsTruncated")[0] === "true";
      continuationToken = truncated ? extractXmlTags(xml, "NextContinuationToken")[0] : undefined;
      if (truncated && continuationToken === undefined) {
        throw new Error("S3 list response was truncated without a continuation token");
      }
    } while (continuationToken !== undefined);

    return keys.map((key) => this.#uriForKey(key)).sort();
  }

  owns(uri: string): boolean {
    try {
      this.#keyFromUri(uri);
      return true;
    } catch {
      return false;
    }
  }

  async #request(
    method: string,
    key: string | null,
    params: readonly (readonly [string, string])[],
    body?: Uint8Array
  ): Promise<Response> {
    const request = this.#buildSignedRequest(method, key, params, body);
    const response = await fetch(request.url, {
      method,
      headers: request.headers,
      ...(body === undefined ? {} : { body: body as BodyInit })
    });
    if (!response.ok) {
      const details = await response.text().catch(() => "");
      throw new Error(
        `S3 ${method} ${request.url.pathname} failed: ${response.status} ${response.statusText}` +
          (details.length === 0 ? "" : `: ${details}`)
      );
    }
    return response;
  }

  #buildSignedRequest(
    method: string,
    key: string | null,
    params: readonly (readonly [string, string])[],
    body?: Uint8Array
  ): { readonly url: URL; readonly headers: Headers } {
    const { url, canonicalUri, canonicalQuery } = this.#urlFor(key, params);
    const bodyBytes = body ?? new Uint8Array();
    const payloadHash = sha256Hex(bodyBytes);
    const { amzDate, dateStamp } = formatAmzDate(new Date());
    const canonicalHeaders =
      `host:${url.host}\n` +
      `x-amz-content-sha256:${payloadHash}\n` +
      `x-amz-date:${amzDate}\n`;
    const signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    const canonicalRequest = [
      method,
      canonicalUri,
      canonicalQuery,
      canonicalHeaders,
      signedHeaders,
      payloadHash
    ].join("\n");
    const credentialScope = `${dateStamp}/${this.#region}/s3/aws4_request`;
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      amzDate,
      credentialScope,
      sha256Hex(new TextEncoder().encode(canonicalRequest))
    ].join("\n");
    const signature = hmacHex(signingKey(this.#secretAccessKey, dateStamp, this.#region), stringToSign);
    const headers = new Headers({
      authorization:
        `AWS4-HMAC-SHA256 Credential=${this.#accessKeyId}/${credentialScope}, ` +
        `SignedHeaders=${signedHeaders}, Signature=${signature}`,
      "x-amz-content-sha256": payloadHash,
      "x-amz-date": amzDate
    });
    return { url, headers };
  }

  #urlFor(
    key: string | null,
    params: readonly (readonly [string, string])[]
  ): { readonly url: URL; readonly canonicalUri: string; readonly canonicalQuery: string } {
    const url = new URL(this.#endpoint.href);
    const endpointPath = url.pathname.replace(/\/+$/g, "");
    const encodedKey = key === null ? "" : key.split("/").map(awsPercentEncode).join("/");
    if (this.#forcePathStyle) {
      url.pathname = `${endpointPath}/${awsPercentEncode(this.#bucket)}${
        encodedKey.length === 0 ? "" : `/${encodedKey}`
      }`;
    } else {
      url.hostname = `${this.#bucket}.${url.hostname}`;
      url.pathname = `${endpointPath}${encodedKey.length === 0 ? "/" : `/${encodedKey}`}`;
    }
    const canonicalQuery = canonicalQueryString(params);
    url.search = canonicalQuery;
    return {
      url,
      canonicalUri: url.pathname || "/",
      canonicalQuery
    };
  }

  #keyForDigest(digest: string): string {
    return `${this.#prefix}/${hashFromDigest(digest)}.bin`;
  }

  #uriForKey(key: string): string {
    return `s3://${this.#bucket}/${key.split("/").map(encodeURIComponent).join("/")}`;
  }

  #keyFromUri(uri: string): string {
    const parsed = new URL(uri);
    if (parsed.protocol !== "s3:") {
      throw new Error(`unsupported blob URI protocol: ${parsed.protocol}`);
    }
    if (parsed.hostname !== this.#bucket) {
      throw new Error(`blob URI bucket is not owned by this store: ${uri}`);
    }
    const key = decodeUriPath(parsed.pathname);
    if (!key.startsWith(`${this.#prefix}/`)) {
      throw new Error(`blob URI prefix is not owned by this store: ${uri}`);
    }
    return key;
  }
}

export interface EncodePayloadWithStorageOptions<T> {
  readonly codec?: CodecId;
  readonly schema?: SchemaAdapter<T>;
  readonly schemaFingerprint?: string;
  readonly inlineThresholdBytes: number;
  readonly blobStore: PayloadBlobStore;
}

export async function encodePayloadWithStorage<T>(
  value: T,
  options: EncodePayloadWithStorageOptions<T>
): Promise<PayloadRef<T>> {
  const payload = encodePayload(value, options);
  if (payload.kind === "Blob" || payload.bytes.byteLength <= options.inlineThresholdBytes) {
    return payload;
  }
  const digest = digestBytes(payload.bytes);
  const uri = await options.blobStore.put(payload.bytes, digest);
  return toBlobRef(payload, uri);
}

export async function hydratePayloadRef<T>(
  payload: PayloadRef<T>,
  blobStore: PayloadBlobStore
): Promise<InlinePayloadRef<T>> {
  if (payload.kind === "Inline") {
    return payload;
  }

  const bytes = await blobStore.get(payload.uri);
  validateBlobBytes(payload, bytes);
  return {
    kind: "Inline",
    codec: payload.codec,
    schemaFingerprint: payload.schemaFingerprint,
    compression: payload.compression,
    encryption: payload.encryption,
    bytes
  };
}

export async function decodePayloadWithStorage<T>(
  payload: PayloadRef<T>,
  blobStore: PayloadBlobStore,
  schema?: SchemaAdapter<T>
): Promise<T> {
  return decodePayload(await hydratePayloadRef(payload, blobStore), schema);
}

export function validateBlobBytes(payload: BlobPayloadRef<unknown>, bytes: Uint8Array): void {
  if (bytes.byteLength !== payload.size) {
    throw new Error(
      `blob payload size mismatch for ${payload.uri}: expected ${payload.size}, got ${bytes.byteLength}`
    );
  }
  const digest = digestBytes(bytes);
  if (digest !== payload.digest) {
    throw new Error(
      `blob payload digest mismatch for ${payload.uri}: expected ${payload.digest}, got ${digest}`
    );
  }
}

export interface PayloadBackendOptions {
  readonly backend: DurableBackend;
  readonly blobStore: PayloadBlobStore;
  readonly inlineThresholdBytes: number;
}

export class PayloadBackend implements DurableBackend {
  readonly #backend: DurableBackend;
  readonly #blobStore: PayloadBlobStore;
  readonly #inlineThresholdBytes: number;

  constructor(options: PayloadBackendOptions) {
    this.#backend = options.backend;
    this.#blobStore = options.blobStore;
    this.#inlineThresholdBytes = options.inlineThresholdBytes;
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    return this.#backend.startWorkflow(await this.#offload(req));
  }

  async claimWorkflowTask(
    workerId: string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    const claimed = await this.#backend.claimWorkflowTask(workerId, opts);
    return claimed === null ? null : this.#hydrate(claimed);
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    return this.#hydrate(await this.#backend.streamHistory(req));
  }

  async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    return this.#backend.commitWorkflowTask(claim, await this.#offloadWorkflowTaskCommit(commit));
  }

  async claimActivityTask(
    workerId: string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    const claimed = await this.#backend.claimActivityTask(workerId, opts);
    return claimed === null ? null : this.#hydrate(claimed);
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    return this.#backend.completeActivity(await this.#offload(req));
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    return this.#backend.completeActivities(await this.#offload(req));
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    return this.#backend.failActivity(await this.#offload(req));
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    return this.#backend.heartbeatActivity(req);
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    return this.#backend.fireDueTimers(req);
  }

  async timeoutDueActivities(
    req: TimeoutDueActivitiesRequest
  ): Promise<TimeoutDueActivitiesOutcome> {
    return this.#backend.timeoutDueActivities(req);
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    return this.#backend.signalWorkflow(await this.#offload(req));
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    const record = await this.#backend.readSignalInbox(req);
    return record === null ? null : this.#hydrate(record);
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    return this.#hydrate(await this.#backend.queryWorkflow(req));
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    return this.#backend.payloadRoots();
  }

  async planGarbageCollection(): Promise<PayloadGarbageCollectionPlan> {
    return planPayloadGarbageCollection({
      blobStore: this.#blobStore,
      roots: await this.#backend.payloadRoots()
    });
  }

  async collectGarbage(
    options: { readonly dryRun?: boolean } = {}
  ): Promise<CollectPayloadGarbageOutcome> {
    return collectPayloadGarbage({
      blobStore: this.#blobStore,
      roots: await this.#backend.payloadRoots(),
      ...(options.dryRun === undefined ? {} : { dryRun: options.dryRun })
    });
  }

  async #offload<T>(value: T): Promise<T> {
    return transformPayloadRefs(value, (payload) => this.#offloadPayloadRef(payload));
  }

  async #hydrate<T>(value: T): Promise<T> {
    return transformPayloadRefs(value, (payload) => this.#hydratePayloadRefDeep(payload));
  }

  async #offloadPayloadRef<T>(payload: PayloadRef<T>): Promise<PayloadRef<T>> {
    if (payload.kind === "Blob" || payload.bytes.byteLength <= this.#inlineThresholdBytes) {
      return payload;
    }
    const digest = digestBytes(payload.bytes);
    const uri = await this.#blobStore.put(payload.bytes, digest);
    return toBlobRef(payload, uri);
  }

  async #hydratePayloadRefDeep<T>(payload: PayloadRef<T>): Promise<PayloadRef<T>> {
    const hydrated = await hydratePayloadRef(payload, this.#blobStore);
    const decoded = decodePayload<unknown>(hydrated);
    const nestedHydrated = await transformPayloadRefs(decoded, (nested) =>
      this.#hydratePayloadRefDeep(nested)
    );
    return encodePayload(nestedHydrated as T, {
      codec: hydrated.codec,
      schemaFingerprint: hydrated.schemaFingerprint
    });
  }

  async #offloadWorkflowTaskCommit(commit: WorkflowTaskCommit): Promise<WorkflowTaskCommit> {
    const transformed: Mutable<Partial<WorkflowTaskCommit>> & Pick<WorkflowTaskCommit, "expectedTailEventId"> = {
      expectedTailEventId: commit.expectedTailEventId
    };
    if (commit.appendEvents !== undefined) {
      transformed.appendEvents = await this.#offload(commit.appendEvents);
    }
    if (commit.upsertWaits !== undefined) {
      transformed.upsertWaits = commit.upsertWaits;
    }
    if (commit.deleteWaits !== undefined) {
      transformed.deleteWaits = commit.deleteWaits;
    }
    if (commit.consumeSignals !== undefined) {
      transformed.consumeSignals = commit.consumeSignals;
    }
    if (commit.scheduleActivities !== undefined) {
      transformed.scheduleActivities = await this.#offload(commit.scheduleActivities);
    }
    if (commit.scheduleActivityMaps !== undefined) {
      transformed.scheduleActivityMaps = await this.#hydrateProviderMapTasks(
        commit.scheduleActivityMaps
      );
    }
    if (commit.startChildWorkflows !== undefined) {
      transformed.startChildWorkflows = await this.#offload(commit.startChildWorkflows);
    }
    if (commit.scheduleChildWorkflowMaps !== undefined) {
      transformed.scheduleChildWorkflowMaps = await this.#hydrateProviderMapTasks(
        commit.scheduleChildWorkflowMaps
      );
    }
    if (commit.queryProjection !== undefined) {
      transformed.queryProjection = await this.#offload(commit.queryProjection);
    }
    return transformed;
  }

  async #hydrateProviderMapTasks<T extends ActivityMapTask | ChildWorkflowMapTask>(
    tasks: readonly T[]
  ): Promise<readonly T[]> {
    return Promise.all(tasks.map((task) => this.#hydrate(task)));
  }
}

export function collectPayloadRefs(value: unknown): readonly PayloadRef[] {
  const refs: PayloadRef[] = [];
  collectPayloadRefsInto(value, refs, new WeakSet<object>());
  return refs;
}

export interface PayloadGarbageCollectionOptions {
  readonly blobStore: PayloadBlobStore;
  readonly roots: readonly unknown[];
}

export interface PayloadGarbageCollectionPlan {
  readonly reachableUris: readonly string[];
  readonly unreachableUris: readonly string[];
  readonly retainedCount: number;
  readonly unreachableCount: number;
}

export interface CollectPayloadGarbageOptions extends PayloadGarbageCollectionOptions {
  readonly dryRun?: boolean;
}

export interface CollectPayloadGarbageOutcome extends PayloadGarbageCollectionPlan {
  readonly deletedUris: readonly string[];
  readonly deletedCount: number;
}

export async function collectPayloadRefsDeep(
  value: unknown,
  blobStore: PayloadBlobStore
): Promise<readonly PayloadRef[]> {
  const refs: PayloadRef[] = [];
  await collectPayloadRefsIntoDeep(value, blobStore, refs, new WeakSet<object>(), new Set());
  return refs;
}

export async function planPayloadGarbageCollection(
  options: PayloadGarbageCollectionOptions
): Promise<PayloadGarbageCollectionPlan> {
  const reachableRefs = await collectPayloadRefsDeep(options.roots, options.blobStore);
  const reachableUris = uniqueSorted(
    reachableRefs
      .filter((payload): payload is BlobPayloadRef => payload.kind === "Blob")
      .filter((payload) => options.blobStore.owns(payload.uri))
      .map((payload) => payload.uri)
  );
  const reachable = new Set(reachableUris);
  const storedUris = (await options.blobStore.list()).filter((uri) => options.blobStore.owns(uri));
  const unreachableUris = uniqueSorted(storedUris.filter((uri) => !reachable.has(uri)));
  return {
    reachableUris,
    unreachableUris,
    retainedCount: reachableUris.length,
    unreachableCount: unreachableUris.length
  };
}

export async function collectPayloadGarbage(
  options: CollectPayloadGarbageOptions
): Promise<CollectPayloadGarbageOutcome> {
  const plan = await planPayloadGarbageCollection(options);
  if (options.dryRun ?? true) {
    return {
      ...plan,
      deletedUris: [],
      deletedCount: 0
    };
  }
  for (const uri of plan.unreachableUris) {
    await options.blobStore.delete(uri);
  }
  return {
    ...plan,
    deletedUris: plan.unreachableUris,
    deletedCount: plan.unreachableUris.length
  };
}

function hashFromDigest(digest: string): string {
  const hash = digest.replace(/^sha256:/, "");
  if (!/^[0-9a-f]{64}$/u.test(hash)) {
    throw new Error(`payload digest must be a sha256 hex digest: ${digest}`);
  }
  return hash;
}

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function hmac(key: Uint8Array | string, data: string): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

function hmacHex(key: Uint8Array, data: string): string {
  return createHmac("sha256", key).update(data).digest("hex");
}

function signingKey(secretAccessKey: string, dateStamp: string, region: string): Buffer {
  const dateKey = hmac(`AWS4${secretAccessKey}`, dateStamp);
  const regionKey = hmac(dateKey, region);
  const serviceKey = hmac(regionKey, "s3");
  return hmac(serviceKey, "aws4_request");
}

function formatAmzDate(date: Date): { readonly amzDate: string; readonly dateStamp: string } {
  const iso = date.toISOString().replace(/[:-]|\.\d{3}/g, "");
  return {
    amzDate: iso,
    dateStamp: iso.slice(0, 8)
  };
}

function awsPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/gu, (char) =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

function canonicalQueryString(params: readonly (readonly [string, string])[]): string {
  return params
    .map(([key, value]) => [awsPercentEncode(key), awsPercentEncode(value)] as const)
    .sort(([leftKey, leftValue], [rightKey, rightValue]) =>
      leftKey === rightKey ? leftValue.localeCompare(rightValue) : leftKey.localeCompare(rightKey)
    )
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

function decodeUriPath(pathname: string): string {
  return pathname
    .replace(/^\/+/u, "")
    .split("/")
    .filter((segment) => segment.length > 0)
    .map((segment) => decodeURIComponent(segment))
    .join("/");
}

function extractXmlTags(xml: string, tagName: string): readonly string[] {
  const pattern = new RegExp(`<${tagName}>([\\s\\S]*?)</${tagName}>`, "gu");
  const values: string[] = [];
  for (const match of xml.matchAll(pattern)) {
    values.push(decodeXmlText(match[1] ?? ""));
  }
  return values;
}

function decodeXmlText(value: string): string {
  return value
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", "\"")
    .replaceAll("&apos;", "'")
    .replaceAll("&amp;", "&");
}

function sanitizePrefix(prefix: string): string {
  const clean = prefix.replace(/^\/+|\/+$/g, "");
  if (clean.length === 0 || clean.includes("..")) {
    throw new Error("blob store prefix must be a non-empty relative path");
  }
  return clean;
}

function pathToFileUri(path: string): string {
  return new URL(`file://${resolve(path)}`).href;
}

function fileUriToPath(uri: string): string {
  const url = new URL(uri);
  if (url.protocol !== "file:") {
    throw new Error(`unsupported blob URI protocol: ${url.protocol}`);
  }
  return resolve(url.pathname);
}

function isNodeError(error: unknown, code: string): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error as { readonly code?: unknown }).code === code
  );
}

type Mutable<T> = {
  -readonly [Key in keyof T]: T[Key];
};

async function transformPayloadRefs<T>(
  value: T,
  transform: <Payload>(payload: PayloadRef<Payload>) => Promise<PayloadRef<Payload>>,
  seen = new WeakMap<object, unknown>()
): Promise<T> {
  if (isPayloadRef(value)) {
    return (await transform(value)) as T;
  }
  if (value === null || typeof value !== "object" || value instanceof Uint8Array) {
    return value;
  }
  const cached = seen.get(value);
  if (cached !== undefined) {
    return cached as T;
  }
  if (Array.isArray(value)) {
    const output: unknown[] = [];
    seen.set(value, output);
    for (const item of value) {
      output.push(await transformPayloadRefs(item, transform, seen));
    }
    return output as T;
  }
  const output: Record<string, unknown> = {};
  seen.set(value, output);
  for (const [key, nested] of Object.entries(value)) {
    output[key] = await transformPayloadRefs(nested, transform, seen);
  }
  return output as T;
}

function collectPayloadRefsInto(
  value: unknown,
  refs: PayloadRef[],
  seen: WeakSet<object>
): void {
  if (isPayloadRef(value)) {
    refs.push(value);
    return;
  }
  if (value === null || typeof value !== "object" || value instanceof Uint8Array) {
    return;
  }
  if (seen.has(value)) {
    return;
  }
  seen.add(value);
  if (Array.isArray(value)) {
    for (const item of value) {
      collectPayloadRefsInto(item, refs, seen);
    }
    return;
  }
  for (const nested of Object.values(value)) {
    collectPayloadRefsInto(nested, refs, seen);
  }
}

async function collectPayloadRefsIntoDeep(
  value: unknown,
  blobStore: PayloadBlobStore,
  refs: PayloadRef[],
  seenObjects: WeakSet<object>,
  seenPayloads: Set<string>
): Promise<void> {
  if (isPayloadRef(value)) {
    refs.push(value);
    const key = payloadIdentityKey(value);
    if (seenPayloads.has(key)) {
      return;
    }
    seenPayloads.add(key);

    if (value.kind === "Blob" && !blobStore.owns(value.uri)) {
      return;
    }
    const hydrated = value.kind === "Inline" ? value : await hydratePayloadRef(value, blobStore);
    const decoded = decodePayload<unknown>(hydrated);
    await collectPayloadRefsIntoDeep(decoded, blobStore, refs, seenObjects, seenPayloads);
    return;
  }
  if (value === null || typeof value !== "object" || value instanceof Uint8Array) {
    return;
  }
  if (seenObjects.has(value)) {
    return;
  }
  seenObjects.add(value);
  if (Array.isArray(value)) {
    for (const item of value) {
      await collectPayloadRefsIntoDeep(item, blobStore, refs, seenObjects, seenPayloads);
    }
    return;
  }
  for (const nested of Object.values(value)) {
    await collectPayloadRefsIntoDeep(nested, blobStore, refs, seenObjects, seenPayloads);
  }
}

function isPayloadRef(value: unknown): value is PayloadRef {
  if (!value || typeof value !== "object") {
    return false;
  }
  const maybe = value as {
    readonly kind?: unknown;
    readonly codec?: unknown;
    readonly schemaFingerprint?: unknown;
    readonly compression?: unknown;
    readonly encryption?: unknown;
    readonly bytes?: unknown;
    readonly digest?: unknown;
    readonly size?: unknown;
    readonly uri?: unknown;
  };
  if (
    maybe.kind === "Inline" &&
    typeof maybe.codec === "string" &&
    typeof maybe.schemaFingerprint === "string" &&
    maybe.compression === "None" &&
    "encryption" in maybe &&
    maybe.bytes instanceof Uint8Array
  ) {
    return true;
  }
  return (
    maybe.kind === "Blob" &&
    typeof maybe.codec === "string" &&
    typeof maybe.schemaFingerprint === "string" &&
    maybe.compression === "None" &&
    "encryption" in maybe &&
    typeof maybe.digest === "string" &&
    typeof maybe.size === "number" &&
    typeof maybe.uri === "string"
  );
}

function payloadIdentityKey(payload: PayloadRef): string {
  return payload.kind === "Blob"
    ? `blob:${payload.uri}:${payload.digest}`
    : `inline:${payloadDigestForIdentity(payload)}`;
}

function payloadDigestForIdentity(payload: InlinePayloadRef): string {
  return digestBytes(payload.bytes);
}

function uniqueSorted(values: readonly string[]): readonly string[] {
  return [...new Set(values)].sort();
}
