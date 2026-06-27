import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { describe, expect, it } from "vitest";
import {
  MemoryBackend,
  decodePayload,
  digestBytes,
  encodePayload,
  eventId,
  namespace,
  taskQueue,
  workflowId,
  workflowType
} from "@durust/core";
import {
  PayloadBackend,
  S3CompatibleBlobStore,
  decodePayloadWithStorage,
  encodePayloadWithStorage
} from "@durust/payload";

describe("S3-compatible payload storage", () => {
  it("offloads, lists, hydrates, and deletes payloads through signed S3 requests", async () => {
    const fakeS3 = await startFakeS3();
    try {
      const store = fakeS3Store(fakeS3.endpoint);
      const payload = await encodePayloadWithStorage(
        { body: "x".repeat(128) },
        {
          codec: "Json",
          inlineThresholdBytes: 8,
          blobStore: store
        }
      );

      expect(payload.kind).toBe("Blob");
      if (payload.kind !== "Blob") {
        throw new Error("expected blob payload");
      }
      expect(payload.uri).toMatch(/^s3:\/\/durust-test\/payloads\/[0-9a-f]{64}\.bin$/u);
      expect(store.owns(payload.uri)).toBe(true);
      expect(store.owns("s3://durust-test/other/hash.bin")).toBe(false);
      expect(store.owns("s3://other-bucket/payloads/hash.bin")).toBe(false);
      expect(store.owns("file:///tmp/hash.bin")).toBe(false);

      await expect(store.list()).resolves.toEqual([payload.uri]);
      await expect(decodePayloadWithStorage(payload, store)).resolves.toEqual({
        body: "x".repeat(128)
      });

      await store.delete(payload.uri);
      await expect(store.list()).resolves.toEqual([]);
      expect(fakeS3.requests.map((request) => request.method)).toEqual([
        "PUT",
        "GET",
        "GET",
        "DELETE",
        "GET"
      ]);
      expect(fakeS3.requests.every((request) => request.authorization.startsWith("AWS4-HMAC-SHA256")))
        .toBe(true);
    } finally {
      await fakeS3.close();
    }
  });

  it("works as the object store behind PayloadBackend", async () => {
    const fakeS3 = await startFakeS3();
    try {
      const inner = new MemoryBackend();
      const backend = new PayloadBackend({
        backend: inner,
        blobStore: fakeS3Store(fakeS3.endpoint),
        inlineThresholdBytes: 8
      });
      const started = await backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/s3-payload-start"),
        workflowType: workflowType("payload.s3", 1),
        taskQueue: taskQueue("workflows"),
        input: encodePayload({ body: "reachable".repeat(32) }, { codec: "Json" })
      });

      const rawHistory = await inner.streamHistory({
        runId: started.runId,
        afterEventId: eventId(0),
        upToEventId: eventId(10),
        maxEvents: 10,
        maxBytes: Number.MAX_SAFE_INTEGER
      });
      const rawStarted = rawHistory.events[0]?.data;
      expect(rawStarted?.kind).toBe("WorkflowStarted");
      if (rawStarted?.kind !== "WorkflowStarted") {
        throw new Error("expected raw WorkflowStarted");
      }
      expect(rawStarted.input.kind).toBe("Blob");
      expect(rawStarted.input.kind === "Blob" ? rawStarted.input.uri.startsWith("s3://") : false)
        .toBe(true);

      const claim = await backend.claimWorkflowTask("worker-a", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [workflowType("payload.s3", 1)],
        leaseDurationMs: 30_000
      });
      const startedEvent = claim?.prefetchedHistory[0]?.data;
      expect(startedEvent?.kind).toBe("WorkflowStarted");
      if (startedEvent?.kind !== "WorkflowStarted") {
        throw new Error("expected hydrated WorkflowStarted");
      }
      expect(startedEvent.input.kind).toBe("Inline");
      expect(decodePayload(startedEvent.input)).toEqual({ body: "reachable".repeat(32) });
    } finally {
      await fakeS3.close();
    }
  });
});

interface RecordedS3Request {
  readonly method: string;
  readonly key: string | null;
  readonly authorization: string;
}

interface FakeS3Server {
  readonly endpoint: string;
  readonly requests: readonly RecordedS3Request[];
  close(): Promise<void>;
}

function fakeS3Store(endpoint: string): S3CompatibleBlobStore {
  return new S3CompatibleBlobStore({
    bucket: "durust-test",
    endpoint,
    region: "us-test-1",
    accessKeyId: "test-key",
    secretAccessKey: "test-secret",
    prefix: "payloads"
  });
}

async function startFakeS3(): Promise<FakeS3Server> {
  const objects = new Map<string, Uint8Array>();
  const requests: RecordedS3Request[] = [];
  const server = createServer((request, response) => {
    void handleFakeS3Request(request, response, objects, requests).catch((error: unknown) => {
      response.statusCode = 500;
      response.end(error instanceof Error ? error.stack : String(error));
    });
  });
  await new Promise<void>((resolveListen) => {
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("expected TCP server address");
  }
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    requests,
    close: () => closeServer(server)
  };
}

async function handleFakeS3Request(
  request: IncomingMessage,
  response: ServerResponse,
  objects: Map<string, Uint8Array>,
  requests: RecordedS3Request[]
): Promise<void> {
  const body = await readRequestBody(request);
  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  const { bucket, key } = parsePathStyleS3Path(url.pathname);
  const authorization = request.headers.authorization ?? "";
  if (bucket !== "durust-test") {
    response.writeHead(404);
    response.end("unknown bucket");
    return;
  }
  if (!authorization.startsWith("AWS4-HMAC-SHA256")) {
    response.writeHead(403);
    response.end("missing authorization");
    return;
  }
  if (typeof request.headers["x-amz-date"] !== "string") {
    response.writeHead(403);
    response.end("missing x-amz-date");
    return;
  }
  const expectedHash = digestBytes(body).replace(/^sha256:/u, "");
  if (request.headers["x-amz-content-sha256"] !== expectedHash) {
    response.writeHead(403);
    response.end("bad payload hash");
    return;
  }

  requests.push({ method: request.method ?? "GET", key, authorization });
  if (request.method === "GET" && key === null && url.searchParams.get("list-type") === "2") {
    const prefix = url.searchParams.get("prefix") ?? "";
    const keys = [...objects.keys()].filter((storedKey) => storedKey.startsWith(prefix)).sort();
    response.writeHead(200, { "content-type": "application/xml" });
    response.end(
      `<ListBucketResult><IsTruncated>false</IsTruncated>${keys
        .map((storedKey) => `<Contents><Key>${escapeXml(storedKey)}</Key></Contents>`)
        .join("")}</ListBucketResult>`
    );
    return;
  }
  if (key === null) {
    response.writeHead(400);
    response.end("missing object key");
    return;
  }
  if (request.method === "PUT") {
    objects.set(key, body);
    response.writeHead(200);
    response.end();
    return;
  }
  if (request.method === "GET") {
    const object = objects.get(key);
    if (object === undefined) {
      response.writeHead(404);
      response.end("not found");
      return;
    }
    response.writeHead(200);
    response.end(object);
    return;
  }
  if (request.method === "DELETE") {
    objects.delete(key);
    response.writeHead(204);
    response.end();
    return;
  }
  response.writeHead(405);
  response.end("method not allowed");
}

async function readRequestBody(request: IncomingMessage): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of request) {
    chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : new Uint8Array(chunk));
  }
  const size = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function parsePathStyleS3Path(pathname: string): { readonly bucket: string; readonly key: string | null } {
  const [bucket = "", ...keyParts] = pathname
    .replace(/^\/+/u, "")
    .split("/")
    .map((segment) => decodeURIComponent(segment));
  return {
    bucket,
    key: keyParts.length === 0 ? null : keyParts.join("/")
  };
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&apos;");
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolveClose, rejectClose) => {
    server.close((error?: Error) => {
      if (error) {
        rejectClose(error);
        return;
      }
      resolveClose();
    });
  });
}
