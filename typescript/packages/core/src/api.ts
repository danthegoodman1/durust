import type {
  DurableCallInput,
  DurableHandlerInput,
  DurableInput,
  DurableInputObject,
  MaybePromise,
  OneDurableInputHandler,
  TaskQueue,
  WorkflowId,
  WorkflowType
} from "./types.js";
import { eventId, signalId, type Namespace, type RunId } from "./types.js";
import type { DurableBackend } from "./backend.js";
import type {
  ActivityCallOptions,
  ChildWorkflowMapFailureMode,
  ChildWorkflowOptions,
  ParentClosePolicy
} from "./options.js";
import { decodePayload, encodePayload, type CodecId, type PayloadRef, type SchemaAdapter } from "./payload.js";
import {
  createActivityDurablePromise,
  createActivityMapHandle,
  createChildWorkflowMapHandle,
  createChildWorkflowStart,
  createJoinAllDurablePromise,
  createJoinDurablePromise,
  createSelectAllDurablePromise,
  createSelectDurablePromise,
  createSignalDurablePromise,
  createTimerDurablePromise,
  WorkflowCancelledError,
  WorkflowFailureError
} from "./runtime.js";

export interface ActivityDefinition<
  Input extends DurableInputObject,
  Output,
  Name extends string = string
> {
  readonly kind: "activity";
  readonly name: Name;
  readonly handler: (input: Input) => MaybePromise<Output>;
  readonly inputSchema?: SchemaAdapter<Input>;
  readonly outputSchema?: SchemaAdapter<Output>;
  readonly sourcePath?: string;
}

export interface WorkflowDefinition<
  Input extends DurableInputObject,
  Output,
  QueryState = unknown,
  Name extends string = string
> {
  readonly kind: "workflow";
  readonly name: Name;
  readonly version: number;
  readonly workflowType: WorkflowType;
  readonly handler: (input: Input) => MaybePromise<Output>;
  readonly inputSchema?: SchemaAdapter<Input>;
  readonly outputSchema?: SchemaAdapter<Output>;
  readonly queryStateSchema?: SchemaAdapter<QueryState>;
  readonly queryState?: unknown;
  readonly sourcePath?: string;
  readonly __queryState?: QueryState;
}

export type ActivityInput<T> = T extends ActivityDefinition<infer Input, any, string>
  ? Input
  : never;

export type ActivityOutput<T> = T extends ActivityDefinition<any, infer Output, string>
  ? Output
  : never;

export type WorkflowInput<T> = T extends WorkflowDefinition<infer Input, any, any, string>
  ? Input
  : never;

export type WorkflowOutput<T> = T extends WorkflowDefinition<any, infer Output, any, string>
  ? Output
  : never;

export type WorkflowQueryState<T> =
  T extends WorkflowDefinition<any, any, infer QueryState, string> ? QueryState : never;

export interface SignalDefinition<Payload extends DurableInputObject, Name extends string = string>
  extends DurableBranch<Payload> {
  readonly durableBranchKind: "signal";
  readonly kind: "signal";
  readonly name: Name;
  readonly __payload?: Payload;
}

export interface DurableBranch<T> extends PromiseLike<T> {
  readonly durableBranchKind: "activity" | "timer" | "signal";
}

export interface DurablePromise<T> extends DurableBranch<T> {
  readonly durableKind: "durable-promise";
  readonly durableBranchKind: "activity";
  spawn(): PromiseLike<ActivityHandle<T>>;
}

export interface ActivityHandle<Output> {
  readonly kind: "activity-handle";
  result(): PromiseLike<Output>;
}

export interface ChildWorkflowStart<Output> {
  readonly kind: "child-workflow-start";
  spawn(): PromiseLike<ChildWorkflowHandle<Output>>;
}

export interface ChildWorkflowHandle<Output> {
  readonly kind: "child-workflow-handle";
  readonly workflowId: WorkflowId | string;
  readonly runId: RunId;
  result(): PromiseLike<Output>;
}

export interface WorkflowHandle<Output, QueryState = unknown> {
  readonly kind: "workflow-handle";
  readonly runId: RunId;
  result(): PromiseLike<Output>;
  query(): PromiseLike<QueryState>;
}

export interface WorkflowConfig<
  Name extends string,
  Handler extends (...args: readonly any[]) => MaybePromise<any>,
  QueryState
> {
  readonly name: Name;
  readonly version: number;
  readonly handler: Handler & OneDurableInputHandler<Handler>;
  readonly inputSchema?: SchemaAdapter<DurableHandlerInput<Handler>>;
  readonly outputSchema?: SchemaAdapter<Awaited<ReturnType<Handler>>>;
  readonly queryStateSchema?: SchemaAdapter<QueryState>;
  readonly queryState?: unknown;
  readonly queryStateType?: QueryState;
  readonly sourcePath?: string;
}

export function activity<
  const Name extends string,
  Handler extends (...args: readonly any[]) => MaybePromise<any>
>(
  config: {
    readonly name: Name;
    readonly handler: Handler & OneDurableInputHandler<Handler>;
    readonly inputSchema?: SchemaAdapter<DurableHandlerInput<Handler>>;
    readonly outputSchema?: SchemaAdapter<Awaited<ReturnType<Handler>>>;
    readonly sourcePath?: string;
  }
): ActivityDefinition<DurableHandlerInput<Handler>, Awaited<ReturnType<Handler>>, Name> {
  assertObjectInputSchema(config.inputSchema, `activity ${config.name} input schema`);
  return {
    kind: "activity",
    name: config.name,
    handler: config.handler as (
      input: DurableHandlerInput<Handler>
    ) => MaybePromise<Awaited<ReturnType<Handler>>>,
    ...(config.inputSchema === undefined ? {} : { inputSchema: config.inputSchema }),
    ...(config.outputSchema === undefined ? {} : { outputSchema: config.outputSchema }),
    ...(config.sourcePath === undefined ? {} : { sourcePath: config.sourcePath })
  };
}

export function workflow<
  const Name extends string,
  Handler extends (...args: readonly any[]) => MaybePromise<any>,
  QueryState = unknown
>(
  config: WorkflowConfig<Name, Handler, QueryState>
): WorkflowDefinition<DurableHandlerInput<Handler>, Awaited<ReturnType<Handler>>, QueryState, Name> {
  assertObjectInputSchema(config.inputSchema, `workflow ${config.name} input schema`);
  return {
    kind: "workflow",
    name: config.name,
    version: config.version,
    workflowType: { name: config.name, version: config.version },
    handler: config.handler as (
      input: DurableHandlerInput<Handler>
    ) => MaybePromise<Awaited<ReturnType<Handler>>>,
    ...(config.inputSchema === undefined ? {} : { inputSchema: config.inputSchema }),
    ...(config.outputSchema === undefined ? {} : { outputSchema: config.outputSchema }),
    ...(config.queryStateSchema === undefined ? {} : { queryStateSchema: config.queryStateSchema }),
    ...(config.queryState === undefined ? {} : { queryState: config.queryState }),
    ...(config.sourcePath === undefined ? {} : { sourcePath: config.sourcePath })
  };
}

export function callActivity<
  A extends ActivityDefinition<any, any, string>,
  Input extends ActivityInput<A>
>(
  activityDefinition: A,
  input: DurableCallInput<ActivityInput<A>, Input>,
  options: ActivityCallOptions = {}
): DurablePromise<ActivityOutput<A>> {
  return createActivityDurablePromise(activityDefinition, input, options);
}

export function sleepUntil(deadline: number): DurableBranch<void> {
  return createTimerDurablePromise({ kind: "sleep_until", fireAt: deadline, fingerprintAt: deadline });
}

export function sleep(durationMs: number): DurableBranch<void> {
  return createTimerDurablePromise({ kind: "sleep", durationMs });
}

export type JoinBranches = Record<string, DurableBranch<unknown>>;

export type JoinResult<Branches extends JoinBranches> = {
  readonly [Key in keyof Branches]: Awaited<Branches[Key]>;
};

export function join<const Branches extends JoinBranches>(
  branches: Branches
): PromiseLike<JoinResult<Branches>> {
  return createJoinDurablePromise(branches) as PromiseLike<JoinResult<Branches>>;
}

export type JoinAllResult<Branches extends readonly DurableBranch<unknown>[]> = {
  readonly [Index in keyof Branches]: Awaited<Branches[Index]>;
};

export function joinAll<const Branches extends readonly DurableBranch<unknown>[]>(
  branches: Branches
): PromiseLike<JoinAllResult<Branches>> {
  return createJoinAllDurablePromise(branches) as PromiseLike<JoinAllResult<Branches>>;
}

export type SelectResult<Branches extends JoinBranches> = {
  readonly [Key in keyof Branches]: {
    readonly branch: Key;
    readonly value: Awaited<Branches[Key]>;
  };
}[keyof Branches];

export function select<const Branches extends JoinBranches>(
  branches: Branches
): PromiseLike<SelectResult<Branches>> {
  return createSelectDurablePromise(branches) as PromiseLike<SelectResult<Branches>>;
}

export type SelectAllResult<Branches extends readonly DurableBranch<unknown>[]> = {
  readonly index: number;
  readonly value: Awaited<Branches[number]>;
};

export function selectAll<const Branches extends readonly DurableBranch<unknown>[]>(
  branches: Branches
): PromiseLike<SelectAllResult<Branches>> {
  return createSelectAllDurablePromise(branches) as PromiseLike<SelectAllResult<Branches>>;
}

export function childWorkflow<
  W extends WorkflowDefinition<any, any, any, string>,
  Input extends WorkflowInput<W>
>(
  workflowDefinition: W,
  input: DurableCallInput<WorkflowInput<W>, Input>,
  options: ChildWorkflowOptions
): ChildWorkflowStart<WorkflowOutput<W>> {
  return createChildWorkflowStart(workflowDefinition, input, options);
}

export function signal<Payload extends DurableInputObject, const Name extends string = string>(
  name: DurableInput<Payload> extends never ? never : Name
): SignalDefinition<DurableInput<Payload>, Name> {
  return createSignalDurablePromise<DurableInput<Payload>, Name>(name);
}

export interface SendSignalRequest<
  S extends SignalDefinition<DurableInputObject, string>,
  Payload extends SignalPayload<S> = SignalPayload<S>
> {
  readonly workflowId: WorkflowId | string;
  readonly signal: S;
  readonly payload: DurableCallInput<SignalPayload<S>, Payload>;
  readonly idempotencyKey?: string;
}

export type SignalPayload<S> = S extends SignalDefinition<infer Payload, string> ? Payload : never;

export interface ClientOptions {
  readonly namespace?: Namespace | string;
  readonly payloadCodec?: CodecId;
  readonly signalIdFactory?: () => string;
}

export class Client {
  readonly #backend: DurableBackend | undefined;
  readonly #namespace: Namespace | string;
  readonly #payloadCodec: CodecId;
  readonly #signalIdFactory: () => string;
  #nextSignalId = 1;

  constructor(backend?: DurableBackend, options: ClientOptions = {}) {
    this.#backend = backend;
    this.#namespace = options.namespace ?? "default";
    this.#payloadCodec = options.payloadCodec ?? "MessagePack";
    this.#signalIdFactory =
      options.signalIdFactory ?? (() => `signal-${this.#nextSignalId++}`);
  }

  async startWorkflow<
    W extends WorkflowDefinition<any, any, any, string>,
    Input extends WorkflowInput<W>
  >(
    workflowDefinition: W,
    workflowId: WorkflowId | string,
    taskQueue: TaskQueue | string,
    input: DurableCallInput<WorkflowInput<W>, Input>
  ): Promise<WorkflowHandle<WorkflowOutput<W>, WorkflowQueryState<W>>> {
    const backend = this.#requireBackend("Client.startWorkflow");
    const encodedInput = encodePayload(input, {
      codec: this.#payloadCodec,
      ...(workflowDefinition.inputSchema === undefined
        ? {}
        : { schema: workflowDefinition.inputSchema })
    });
    const outcome = await backend.startWorkflow({
      namespace: this.#namespace,
      workflowId,
      workflowType: workflowDefinition.workflowType,
      taskQueue,
      input: encodedInput
    });
    return new BackendWorkflowHandle(
      backend,
      workflowDefinition,
      this.#namespace,
      workflowId,
      outcome.runId
    );
  }

  async sendSignal<
    S extends SignalDefinition<DurableInputObject, string>,
    Payload extends SignalPayload<S>
  >(
    request: SendSignalRequest<S, Payload>
  ): Promise<void> {
    const backend = this.#requireBackend("Client.sendSignal");
    await backend.signalWorkflow({
      namespace: this.#namespace,
      workflowId: request.workflowId,
      signalId: signalId(request.idempotencyKey ?? this.#signalIdFactory()),
      signalName: request.signal.name,
      payload: encodePayload(request.payload, { codec: this.#payloadCodec })
    });
  }

  async queryWorkflow<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    workflowId: WorkflowId | string
  ): Promise<WorkflowQueryState<W>> {
    const backend = this.#requireBackend("Client.queryWorkflow");
    const outcome = await backend.queryWorkflow({
      namespace: this.#namespace,
      workflowId
    });
    if (outcome.kind === "NotFound") {
      throw new Error(`workflow not found: ${workflowId}`);
    }
    if (outcome.kind === "NoProjection") {
      throw new Error("workflow query projection is not available");
    }
    return decodePayload<WorkflowQueryState<W>>(
      outcome.projection as PayloadRef<WorkflowQueryState<W>>,
      workflowDefinition.queryStateSchema
    );
  }

  #requireBackend(operation: string): DurableBackend {
    if (!this.#backend) {
      throw new Error(`${operation} requires a DurableBackend`);
    }
    return this.#backend;
  }
}

class BackendWorkflowHandle<W extends WorkflowDefinition<any, any, any, string>>
  implements WorkflowHandle<WorkflowOutput<W>, WorkflowQueryState<W>>
{
  readonly kind = "workflow-handle" as const;
  readonly #backend: DurableBackend;
  readonly #workflowDefinition: W;
  readonly #namespace: Namespace | string;
  readonly #workflowId: WorkflowId | string;
  readonly runId: RunId;

  constructor(
    backend: DurableBackend,
    workflowDefinition: W,
    namespace: Namespace | string,
    workflowId: WorkflowId | string,
    runId: RunId
  ) {
    this.#backend = backend;
    this.#workflowDefinition = workflowDefinition;
    this.#namespace = namespace;
    this.#workflowId = workflowId;
    this.runId = runId;
  }

  async result(): Promise<WorkflowOutput<W>> {
    const history = await this.#backend.streamHistory({
      runId: this.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(Number.MAX_SAFE_INTEGER),
      maxEvents: Number.MAX_SAFE_INTEGER,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const completed = history.events.find((event) => event.data.kind === "WorkflowCompleted");
    if (completed?.data.kind === "WorkflowCompleted") {
      return decodePayload<WorkflowOutput<W>>(
        completed.data.result as PayloadRef<WorkflowOutput<W>>,
        this.#workflowDefinition.outputSchema
      );
    }
    const failed = history.events.find((event) => event.data.kind === "WorkflowFailed");
    if (failed?.data.kind === "WorkflowFailed") {
      throw new WorkflowFailureError(failed.data.failure);
    }
    const cancelled = history.events.find((event) => event.data.kind === "WorkflowCancelled");
    if (cancelled?.data.kind === "WorkflowCancelled") {
      throw new WorkflowCancelledError(cancelled.data.reason);
    }
    throw new Error("workflow result is not available");
  }

  async query(): Promise<WorkflowQueryState<W>> {
    const outcome = await this.#backend.queryWorkflow({
      namespace: this.#namespace,
      workflowId: this.#workflowId
    });
    if (outcome.kind === "NotFound") {
      throw new Error(`workflow not found: ${this.#workflowId}`);
    }
    if (outcome.kind === "NoProjection") {
      throw new Error("workflow query projection is not available");
    }
    return decodePayload<WorkflowQueryState<W>>(
      outcome.projection as PayloadRef<WorkflowQueryState<W>>,
      this.#workflowDefinition.queryStateSchema
    );
  }
}

export interface ActivityMapInputManifest<Input extends DurableInputObject> {
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRef<ActivityMapInputPage<Input>>[];
}

export interface ActivityMapInputPage<Input extends DurableInputObject> {
  readonly items: readonly PayloadRef<Input>[];
}

export interface ActivityMapResultManifest<Output> {
  readonly name: string;
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRef<ActivityMapResultPage<Output>>[];
}

export interface ActivityMapResultPage<Output> {
  readonly results: readonly PayloadRef<Output>[];
}

export function decodeActivityMapResultRefs<Output>(
  manifestRef: PayloadRef<ActivityMapResultManifest<Output>>
): readonly PayloadRef<Output>[] {
  const manifest = decodePayload<ActivityMapResultManifest<Output>>(manifestRef);
  const results: PayloadRef<Output>[] = [];
  for (const pageRef of manifest.pages) {
    const page = decodePayload<ActivityMapResultPage<Output>>(pageRef);
    results.push(...page.results);
  }
  if (results.length !== manifest.itemCount) {
    throw new Error(
      `activity map result manifest item count mismatch: expected ${manifest.itemCount}, got ${results.length}`
    );
  }
  const pageItemCount = manifest.pageLengths.reduce((sum, count) => sum + count, 0);
  if (pageItemCount !== manifest.itemCount) {
    throw new Error(
      `activity map result manifest page length mismatch: expected ${manifest.itemCount}, got ${pageItemCount}`
    );
  }
  return results;
}

export function decodeActivityMapResults<Output>(
  manifestRef: PayloadRef<ActivityMapResultManifest<Output>>
): readonly Output[] {
  return decodeActivityMapResultRefs(manifestRef).map((resultRef) =>
    decodePayload<Output>(resultRef)
  );
}

export interface ActivityMapOptions<Input extends DurableInputObject> {
  readonly inputManifest: PayloadRef<ActivityMapInputManifest<Input>>;
  readonly resultManifest: string;
  readonly taskQueue?: string;
  readonly maxInFlight: number;
}

export interface ActivityMapHandle<Output> {
  readonly kind: "activity-map-handle";
  resultManifest(): PromiseLike<PayloadRef<ActivityMapResultManifest<Output>>>;
}

export function activityMap<A extends ActivityDefinition<any, any, string>>(
  activityDefinition: A,
  options: ActivityMapOptions<ActivityInput<A>>
): ActivityMapHandle<ActivityOutput<A>> {
  return createActivityMapHandle(activityDefinition, options);
}

export function activityMapManifest<Input extends DurableInputObject>(
  items: readonly (DurableInput<Input> extends never ? never : Input)[],
  pageSize = 128
): PayloadRef<ActivityMapInputManifest<Input>> {
  if (pageSize <= 0 || !Number.isInteger(pageSize)) {
    throw new Error("activityMapManifest pageSize must be a positive integer");
  }
  const pages: PayloadRef<ActivityMapInputPage<Input>>[] = [];
  const pageLengths: number[] = [];
  for (let index = 0; index < items.length; index += pageSize) {
    const pageItems = items.slice(index, index + pageSize);
    pageLengths.push(pageItems.length);
    pages.push(
      encodePayload<ActivityMapInputPage<Input>>({
        items: pageItems.map((item) => encodePayload(item))
      })
    );
  }
  return encodePayload<ActivityMapInputManifest<Input>>({
    itemCount: items.length,
    pageLengths,
    pages
  });
}

export type ChildWorkflowMapItemOutcome<Output> =
  | { readonly kind: "Succeeded"; readonly result: PayloadRef<Output> }
  | { readonly kind: "Failed"; readonly failure: DurableFailure }
  | { readonly kind: "Cancelled"; readonly reason: string };

export interface ChildWorkflowMapResultManifest<Output> {
  readonly name: string;
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRef<ChildWorkflowMapResultPage<Output>>[];
}

export interface ChildWorkflowMapResultPage<Output> {
  readonly outcomes: readonly ChildWorkflowMapItemOutcome<Output>[];
}

export function decodeChildWorkflowMapOutcomes<Output>(
  manifestRef: PayloadRef<ChildWorkflowMapResultManifest<Output>>
): readonly ChildWorkflowMapItemOutcome<Output>[] {
  const manifest = decodePayload<ChildWorkflowMapResultManifest<Output>>(manifestRef);
  const outcomes: ChildWorkflowMapItemOutcome<Output>[] = [];
  for (const pageRef of manifest.pages) {
    const page = decodePayload<ChildWorkflowMapResultPage<Output>>(pageRef);
    outcomes.push(...page.outcomes);
  }
  if (outcomes.length !== manifest.itemCount) {
    throw new Error(
      `child workflow map result manifest item count mismatch: expected ${manifest.itemCount}, got ${outcomes.length}`
    );
  }
  const pageItemCount = manifest.pageLengths.reduce((sum, count) => sum + count, 0);
  if (pageItemCount !== manifest.itemCount) {
    throw new Error(
      `child workflow map result manifest page length mismatch: expected ${manifest.itemCount}, got ${pageItemCount}`
    );
  }
  return outcomes;
}

export function decodeChildWorkflowMapSuccessRefs<Output>(
  manifestRef: PayloadRef<ChildWorkflowMapResultManifest<Output>>
): readonly PayloadRef<Output>[] {
  return decodeChildWorkflowMapOutcomes(manifestRef).map((outcome, index) => {
    if (outcome.kind !== "Succeeded") {
      throw new Error(`child workflow map item ${index} did not succeed: ${outcome.kind}`);
    }
    return outcome.result;
  });
}

export function decodeChildWorkflowMapSuccesses<Output>(
  manifestRef: PayloadRef<ChildWorkflowMapResultManifest<Output>>
): readonly Output[] {
  return decodeChildWorkflowMapSuccessRefs(manifestRef).map((resultRef) =>
    decodePayload<Output>(resultRef)
  );
}

export interface ChildWorkflowMapOptions<Input extends DurableInputObject> {
  readonly inputManifest: PayloadRef<ActivityMapInputManifest<Input>>;
  readonly resultManifest: string;
  readonly workflowIdPrefix: string;
  readonly taskQueue?: string;
  readonly maxInFlight: number;
  readonly parentClosePolicy?: ParentClosePolicy;
  readonly failureMode?: ChildWorkflowMapFailureMode;
}

export interface ChildWorkflowMapHandle<Output> {
  readonly kind: "child-workflow-map-handle";
  resultManifest(): PromiseLike<PayloadRef<ChildWorkflowMapResultManifest<Output>>>;
}

export function childWorkflowMap<W extends WorkflowDefinition<any, any, any, string>>(
  workflowDefinition: W,
  options: ChildWorkflowMapOptions<WorkflowInput<W>>
): ChildWorkflowMapHandle<WorkflowOutput<W>> {
  return createChildWorkflowMapHandle(workflowDefinition, options);
}

export interface DurableFailure {
  readonly errorType: string;
  readonly message: string;
  readonly nonRetryable: boolean;
  readonly details?: PayloadRef<unknown>;
}

function assertObjectInputSchema(schema: SchemaAdapter<any> | undefined, label: string): void {
  if (schema?.rootKind !== undefined && schema.rootKind !== "object") {
    throw new Error(`${label} must describe an object root`);
  }
}
