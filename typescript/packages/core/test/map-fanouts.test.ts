import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import {
  RetryPolicy,
  activityMapFingerprint,
  activityMapManifest,
  commandId,
  encodePayload,
  namespace,
  payloadDigest,
  taskQueue,
  workflowId,
  workflowType,
  type ActivityTaskClaim
} from "@durust/core";
import { NativeBackend } from "@durust/native";
import { claimWorkflow, readHistory, startTestWorkflow } from "@durust/testing";

/**
 * The `fanouts` half of the shared map transition table: scripted whole
 * fanouts expressed through the public provider contract alone. The Rust
 * runner (`tests/map_transitions.rs`) replays the same cases; the
 * `transitions` half drives the map engine directly and has a Rust runner
 * only, because the engine lives in Rust.
 */
interface FanoutCase {
  readonly name: string;
  readonly itemCount: number;
  readonly maxInFlight: number;
  readonly steps: readonly {
    readonly action: "claim" | "complete" | "fail" | "completeAbandoned";
    readonly ordinal: number | null;
  }[];
  readonly parentHistory: readonly string[];
}

/**
 * Pinned to `SHARED_TABLE_FANOUTS` in `tests/map_transitions.rs`, so a change
 * to the table fails in both runtimes rather than quietly in neither.
 */
const SHARED_TABLE_FANOUTS = 5;

const TRANSITION_TABLE = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../../fixtures/contract/map-transitions.json", import.meta.url)),
    "utf8"
  )
) as { readonly fanouts: readonly FanoutCase[] };

describe("shared map transition table fanouts", () => {
  let replayedFanouts = 0;
  afterAll(() => {
    expect(
      replayedFanouts,
      "this file replayed a different number of the shared table's fanouts than expected; a generated-case loop that runs fewer times than the table has cases still reports success while asserting nothing about the cases that vanished, so restore the missing fanouts or move SHARED_TABLE_FANOUTS in tests/map_transitions.rs and this number together"
    ).toBe(SHARED_TABLE_FANOUTS);
  });

  for (const fanout of TRANSITION_TABLE.fanouts) {
    it(`fanout: ${fanout.name}`, async () => {
      replayedFanouts += 1;
      const backend = NativeBackend.memory();
      await startTestWorkflow(backend, {
        workflowId: workflowId("wf/map-table-fanout"),
        workflowType: workflowType("map-table.workflow", 1),
        input: encodePayload({ value: 1 }, { codec: "Json" })
      });
      const claimed = await claimWorkflow(backend, "fanout-scheduler", {
        workflowTypes: [workflowType("map-table.workflow", 1)]
      });
      const items = Array.from({ length: fanout.itemCount }, (_, index) => ({ value: index }));
      const inputManifest = activityMapManifest(items, 2);
      const scheduled = {
        commandId: commandId(claimed.runId, 1),
        activityName: "map-table.item",
        taskQueue: "activities",
        retryPolicy: RetryPolicy.none(),
        startToCloseTimeoutMs: null,
        heartbeatTimeoutMs: null,
        inputManifest,
        resultManifestName: "mapped",
        maxInFlight: fanout.maxInFlight,
        fingerprint: activityMapFingerprint(
          "map-table.item",
          payloadDigest(inputManifest),
          "mapped",
          fanout.maxInFlight,
          "sha256:map-table"
        )
      };
      await backend.commitWorkflowTask(claimed.claim, {
        appendEvents: [{ data: { kind: "ActivityMapScheduled", scheduled } }],
        scheduleActivityMaps: [
          {
            mapCommandId: scheduled.commandId,
            activityName: scheduled.activityName,
            taskQueue: scheduled.taskQueue,
            retryPolicy: scheduled.retryPolicy,
            startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
            heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
            inputManifest: scheduled.inputManifest,
            resultManifestName: scheduled.resultManifestName,
            maxInFlight: scheduled.maxInFlight
          }
        ]
      });

      const claims = new Map<number, ActivityTaskClaim>();
      for (const [index, stepCase] of fanout.steps.entries()) {
        const where = `${fanout.name} step ${index} (${stepCase.action})`;
        if (stepCase.action === "claim") {
          const task = await backend.claimActivityTask(`fanout-worker-${index}`, {
            namespace: namespace(),
            taskQueue: taskQueue("activities"),
            registeredActivityNames: ["map-table.item"],
            leaseDurationMs: 30_000
          });
          expect(task?.task.mapItem?.itemOrdinal ?? null, where).toBe(stepCase.ordinal);
          if (task !== null) {
            claims.set(task.task.mapItem?.itemOrdinal ?? -1, task.claim);
          }
          continue;
        }
        const claim = claims.get(stepCase.ordinal as number);
        if (claim === undefined) {
          throw new Error(`${where}: ordinal was never claimed`);
        }
        if (stepCase.action === "complete") {
          const outcome = await backend.completeActivity({
            claim,
            result: encodePayload({ value: stepCase.ordinal }, { codec: "Json" })
          });
          expect(outcome.kind, where).toBe("Completed");
          continue;
        }
        if (stepCase.action === "completeAbandoned") {
          const outcome = await backend.completeActivity({
            claim,
            result: encodePayload({ value: stepCase.ordinal }, { codec: "Json" })
          });
          expect(outcome.kind, where).toBe("AlreadyCompleted");
          continue;
        }
        const outcome = await backend.failActivity({
          claim,
          failure: { errorType: "map-table.fatal", message: "fatal", nonRetryable: true }
        });
        expect(outcome.kind, where).toBe("Failed");
      }

      const history = await readHistory(backend, claimed.runId, 50);
      expect(history.events.map((event) => event.eventType), fanout.name).toEqual(
        fanout.parentHistory
      );
    });
  }
});
