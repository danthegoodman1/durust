import { describe, expect, it } from "vitest";
import {
  decodePayload,
  digestBytes,
  encodePayload,
  payloadDigest,
  payloadRefFromJson,
  payloadRefToJson,
  toBlobRef
} from "@durust/core";

describe("payload refs", () => {
  it("round-trips inline JSON payload refs through canonical fixture JSON shape", () => {
    const payload = encodePayload({ orderId: "o-1" }, {
      codec: "Json",
      schemaFingerprint: "sha256:checkout-input"
    });

    expect(decodePayload(payload)).toEqual({ orderId: "o-1" });

    const json = payloadRefToJson(payload);
    expect(json).toMatchObject({
      kind: "Inline",
      codec: "Json",
      schemaFingerprint: "sha256:checkout-input",
      compression: "None",
      encryption: null
    });

    const decodedJson = payloadRefFromJson<{ orderId: string }>(json);
    expect(decodePayload(decodedJson)).toEqual({ orderId: "o-1" });
  });

  it("uses inline bytes for payload digest and digest text for blob digest", () => {
    const payload = encodePayload({ value: 7 }, { codec: "Json" });
    const inlineDigest = payloadDigest(payload);
    const blob = toBlobRef(payload, "file:///payloads/value-7");

    expect(blob.kind).toBe("Blob");
    expect(blob.digest).toBe(digestBytes(payload.kind === "Inline" ? payload.bytes : ""));
    expect(payloadDigest(blob)).not.toBe(inlineDigest);
  });

  it("round-trips MessagePack payloads", () => {
    const payload = encodePayload({ sku: "sku-1", quantity: 2 });

    expect(payload.kind).toBe("Inline");
    expect(payload.codec).toBe("MessagePack");
    expect(decodePayload(payload)).toEqual({ sku: "sku-1", quantity: 2 });
  });
});
