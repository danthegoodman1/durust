import { createHash } from "node:crypto";
import { Buffer } from "node:buffer";
import { decode, encode } from "@msgpack/msgpack";

export type CodecId = "MessagePack" | "Json" | "Protobuf";
export type CompressionId = "None";

export interface EncryptionMetadata {
  readonly keyId: string;
}

export interface SchemaAdapter<T> {
  readonly fingerprint: string;
  readonly rootKind?: "object" | "array" | "primitive" | "unknown";
  readonly encode?: (value: T) => unknown;
  readonly decode?: (value: unknown) => T;
}

export type PayloadRef<T = unknown> =
  | InlinePayloadRef<T>
  | BlobPayloadRef<T>;

export interface InlinePayloadRef<T = unknown> {
  readonly kind: "Inline";
  readonly codec: CodecId;
  readonly schemaFingerprint: string;
  readonly compression: CompressionId;
  readonly encryption: EncryptionMetadata | null;
  readonly bytes: Uint8Array;
  readonly __payloadType?: T;
}

export interface BlobPayloadRef<T = unknown> {
  readonly kind: "Blob";
  readonly codec: CodecId;
  readonly schemaFingerprint: string;
  readonly compression: CompressionId;
  readonly encryption: EncryptionMetadata | null;
  readonly digest: string;
  readonly size: number;
  readonly uri: string;
  readonly __payloadType?: T;
}

export interface PayloadStorageConfig {
  readonly codec: CodecId;
  readonly inlineThresholdBytes: number;
  readonly blobStore?: BlobStoreConfig;
}

export type BlobStoreConfig = {
  readonly kind: "LocalDirectory";
  readonly root: string;
  readonly prefix: string;
};

export const DEFAULT_INLINE_THRESHOLD_BYTES = 8 * 1024;

export const defaultPayloadStorageConfig: PayloadStorageConfig = {
  codec: "MessagePack",
  inlineThresholdBytes: DEFAULT_INLINE_THRESHOLD_BYTES
};

export function digestBytes(bytes: Uint8Array | string): string {
  const hasher = createHash("sha256");
  hasher.update(typeof bytes === "string" ? Buffer.from(bytes) : Buffer.from(bytes));
  return `sha256:${hasher.digest("hex")}`;
}

export function typeNameFingerprint(typeName: string): string {
  return digestBytes(Buffer.from(typeName, "utf8"));
}

export function payloadDigest(payload: PayloadRef<unknown>): string {
  return payload.kind === "Inline"
    ? digestBytes(payload.bytes)
    : digestBytes(payload.digest);
}

export interface EncodePayloadOptions<T> {
  readonly codec?: CodecId;
  readonly schema?: SchemaAdapter<T>;
  readonly schemaFingerprint?: string;
}

export function encodePayload<T>(
  value: T,
  options: EncodePayloadOptions<T> = {}
): PayloadRef<T> {
  const codec = options.codec ?? "MessagePack";
  const encodedValue = options.schema?.encode ? options.schema.encode(value) : value;
  const bytes = encodeValue(encodedValue, codec);
  return {
    kind: "Inline",
    codec,
    schemaFingerprint:
      options.schemaFingerprint ?? options.schema?.fingerprint ?? typeNameFingerprint("unknown"),
    compression: "None",
    encryption: null,
    bytes
  };
}

export function decodePayload<T>(payload: PayloadRef<T>, schema?: SchemaAdapter<T>): T {
  if (payload.kind === "Blob") {
    throw new Error("blob payload must be hydrated before decode");
  }

  const decoded = decodeValue(payload.bytes, payload.codec);
  return schema?.decode ? schema.decode(decoded) : (decoded as T);
}

export function toBlobRef<T>(payload: PayloadRef<T>, uri: string): PayloadRef<T> {
  if (payload.kind === "Blob") {
    return payload;
  }

  return {
    kind: "Blob",
    codec: payload.codec,
    schemaFingerprint: payload.schemaFingerprint,
    compression: payload.compression,
    encryption: payload.encryption,
    digest: digestBytes(payload.bytes),
    size: payload.bytes.byteLength,
    uri
  };
}

export interface PayloadRefJson {
  readonly kind: "Inline" | "Blob";
  readonly codec: CodecId;
  readonly schemaFingerprint: string;
  readonly compression: CompressionId;
  readonly encryption: EncryptionMetadata | null;
  readonly bytes?: readonly number[];
  readonly digest?: string;
  readonly size?: number;
  readonly uri?: string;
}

export function payloadRefToJson(payload: PayloadRef<unknown>): PayloadRefJson {
  if (payload.kind === "Inline") {
    return {
      kind: "Inline",
      codec: payload.codec,
      schemaFingerprint: payload.schemaFingerprint,
      compression: payload.compression,
      encryption: payload.encryption,
      bytes: [...payload.bytes]
    };
  }

  return {
    kind: "Blob",
    codec: payload.codec,
    schemaFingerprint: payload.schemaFingerprint,
    compression: payload.compression,
    encryption: payload.encryption,
    digest: payload.digest,
    size: payload.size,
    uri: payload.uri
  };
}

export function payloadRefFromJson<T = unknown>(json: PayloadRefJson): PayloadRef<T> {
  if (json.kind === "Inline") {
    if (!json.bytes) {
      throw new Error("inline payload JSON missing bytes");
    }
    return {
      kind: "Inline",
      codec: json.codec,
      schemaFingerprint: json.schemaFingerprint,
      compression: json.compression,
      encryption: json.encryption,
      bytes: Uint8Array.from(json.bytes)
    };
  }

  if (!json.digest || json.size === undefined || !json.uri) {
    throw new Error("blob payload JSON missing digest, size, or uri");
  }

  return {
    kind: "Blob",
    codec: json.codec,
    schemaFingerprint: json.schemaFingerprint,
    compression: json.compression,
    encryption: json.encryption,
    digest: json.digest,
    size: json.size,
    uri: json.uri
  };
}

function encodeValue(value: unknown, codec: CodecId): Uint8Array {
  switch (codec) {
    case "MessagePack":
      return encode(value);
    case "Json":
      return new TextEncoder().encode(JSON.stringify(value));
    case "Protobuf":
      throw new Error("protobuf payload codec is not enabled");
  }
}

function decodeValue(bytes: Uint8Array, codec: CodecId): unknown {
  switch (codec) {
    case "MessagePack":
      return decode(bytes);
    case "Json":
      return JSON.parse(new TextDecoder().decode(bytes)) as unknown;
    case "Protobuf":
      throw new Error("protobuf payload codec is not enabled");
  }
}
