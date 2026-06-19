export interface RetryPolicyConfig {
  readonly initialIntervalMs?: number;
  readonly maxIntervalMs?: number;
  readonly maxAttempts?: number;
  readonly backoffCoefficient?: number;
  readonly nonRetryableErrorTypes?: readonly string[];
}

export class RetryPolicy {
  readonly initialIntervalMs: number;
  readonly maxIntervalMs: number;
  readonly maxAttempts: number;
  readonly backoffCoefficient: number;
  readonly nonRetryableErrorTypes: readonly string[];

  private constructor(config: Required<RetryPolicyConfig>) {
    this.initialIntervalMs = config.initialIntervalMs;
    this.maxIntervalMs = config.maxIntervalMs;
    this.maxAttempts = config.maxAttempts;
    this.backoffCoefficient = config.backoffCoefficient;
    this.nonRetryableErrorTypes = config.nonRetryableErrorTypes;
  }

  static none(): RetryPolicy {
    return new RetryPolicy({
      initialIntervalMs: 0,
      maxIntervalMs: 0,
      maxAttempts: 1,
      backoffCoefficient: 1,
      nonRetryableErrorTypes: []
    });
  }

  static exponential(config: RetryPolicyConfig = {}): RetryPolicy {
    return new RetryPolicy({
      initialIntervalMs: config.initialIntervalMs ?? 1_000,
      maxIntervalMs: config.maxIntervalMs ?? 60_000,
      maxAttempts: config.maxAttempts ?? 3,
      backoffCoefficient: config.backoffCoefficient ?? 2,
      nonRetryableErrorTypes: config.nonRetryableErrorTypes ?? []
    });
  }
}

export type ParentClosePolicy = "Cancel" | "Abandon";
export type ChildWorkflowMapFailureMode = "FailFast" | "CollectAll";

export interface ActivityCallOptions {
  readonly taskQueue?: string;
  readonly retry?: RetryPolicy;
  readonly startToCloseTimeoutMs?: number;
  readonly heartbeatTimeoutMs?: number;
}

export interface ChildWorkflowOptions {
  readonly workflowId: string;
  readonly taskQueue?: string;
  readonly parentClosePolicy?: ParentClosePolicy;
}
