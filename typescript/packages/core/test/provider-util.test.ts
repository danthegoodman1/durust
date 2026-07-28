import { describe, expect, it } from "vitest";
import { commandId, runId, sameCommandId } from "@durust/core";
import type { CommandId } from "@durust/core";

/**
 * Pins the one semantic that the three-way unification of `sameCommandId` had to
 * choose between.
 *
 * Before `provider-util.ts` there were three copies. `@durust/core`'s compared
 * `left.seq === right.seq`; the SQLite and Postgres copies compared
 * `Number(left.seq) === Number(right.seq)`. Unifying onto the strict spelling is
 * a behaviour change for two of the three providers, and **nothing in the suite
 * noticed**: putting the coercion back into the unified helper still gives
 * `646 passed | 133 skipped`, measured.
 *
 * That is the reason this file exists. A silent behaviour change is exactly what
 * gets reverted by the next person who sees `Number()` in a git history and
 * assumes it was there for a reason. These cases make the choice fail loudly
 * instead, and they are written against the coerced spelling specifically: each
 * one passes today and fails if `Number(...)` comes back.
 *
 * The casts below are the point rather than a workaround. `CommandSeq` is a
 * branded `number`, so a well-typed caller cannot produce these values — but a
 * JSON/JSONB column can, which is the situation the coercion was there to
 * tolerate, and the situation in which it does damage.
 */
describe("sameCommandId", () => {
  const run = runId("run/command-identity");
  const other = runId("run/command-identity-other");

  // A `seq` that is not a number, spelled as the database could deliver it.
  const withSeq = (seq: unknown): CommandId =>
    ({ runId: run, seq }) as unknown as CommandId;

  it("matches a command against itself", () => {
    expect(sameCommandId(commandId(run, 7), commandId(run, 7))).toBe(true);
  });

  it("separates commands by seq and by run", () => {
    expect(sameCommandId(commandId(run, 7), commandId(run, 8))).toBe(false);
    expect(sameCommandId(commandId(run, 7), commandId(other, 7))).toBe(false);
  });

  // The four values `Number()` folds onto 0. Under the coerced spelling every
  // one of these compares equal to command 0 — and each caller of this function
  // acts on a match by tombstoning an activity task, abandoning a map item, or
  // cancelling a child run, so a false positive here is an irreversible write
  // against a command the caller never named.
  it.each([
    ["null", null],
    ["empty string", ""],
    ["false", false],
    ["empty array", []]
  ])("does not treat a %s seq as command 0", (_label, seq) => {
    expect(sameCommandId(commandId(run, 0), withSeq(seq))).toBe(false);
  });

  // The mirror image: `Number(undefined)` is `NaN`, and `NaN !== NaN`, so the
  // coerced spelling was also *less* permissive here — two structurally
  // identical ids compared unequal. Strict equality gets this right too, and
  // pinning it stops a future "fix" from reaching for `Number()` to repair the
  // false negative and silently restoring the false positives above.
  it("treats two structurally identical ids as equal even when seq is absent", () => {
    expect(sameCommandId(withSeq(undefined), withSeq(undefined))).toBe(true);
  });

  it("compares seq by value, not by string", () => {
    expect(sameCommandId(commandId(run, 10), withSeq("10"))).toBe(false);
  });
});
