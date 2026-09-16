/**
 * Why a provider refused a call. Every provider throws a `ProviderError`
 * for these conditions, so the worker, the client, and the conformance
 * suite branch on `code` instead of on message text.
 */
export type ProviderErrorCode =
  | "StaleWorkflowLease"
  | "StaleActivityLease"
  | "TerminalWorkflow"
  | "TerminalWorkflowSignal"
  | "WorkflowNotFound"
  | "InvalidMapOptions";

export class ProviderError extends Error {
  readonly code: ProviderErrorCode;

  constructor(code: ProviderErrorCode, message: string) {
    super(message);
    this.name = "ProviderError";
    this.code = code;
  }
}

/**
 * Matches by shape as well as by class, so a `ProviderError` from another
 * copy of `@durust/core` (a duplicated install, a native addon's adapter) is
 * still recognised.
 */
export function isProviderError(error: unknown, code?: ProviderErrorCode): error is ProviderError {
  if (error === null || typeof error !== "object") {
    return false;
  }
  const candidate = error as { readonly name?: unknown; readonly code?: unknown };
  const shaped =
    error instanceof ProviderError ||
    (candidate.name === "ProviderError" && typeof candidate.code === "string");
  return shaped && (code === undefined || candidate.code === code);
}

export function staleWorkflowLeaseError(): ProviderError {
  return new ProviderError("StaleWorkflowLease", "stale workflow task lease");
}

export function staleActivityLeaseError(): ProviderError {
  return new ProviderError("StaleActivityLease", "stale activity task lease");
}

export function terminalWorkflowError(): ProviderError {
  return new ProviderError("TerminalWorkflow", "terminal workflow rejects workflow-visible mutations");
}

export function terminalWorkflowSignalError(): ProviderError {
  return new ProviderError("TerminalWorkflowSignal", "terminal workflow rejects signals");
}

export function workflowNotFoundError(workflowId: string): ProviderError {
  return new ProviderError("WorkflowNotFound", `workflow not found: ${workflowId}`);
}

export function invalidMapOptionsError(message: string): ProviderError {
  return new ProviderError("InvalidMapOptions", message);
}
