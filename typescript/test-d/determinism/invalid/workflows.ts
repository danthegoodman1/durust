import { readFileSync } from "node:fs";
import { request } from "node:http";
import { Worker } from "node:worker_threads";
import { workflow } from "@durust/core";

interface NoInput {}

export const invalidWorkflow = workflow({
  name: "lint.invalid.workflow",
  version: 1,
  handler: async (_input: NoInput): Promise<string> => {
    const timestamp = Date.now();
    const random = Math.random();
    setTimeout(() => undefined, 1);
    queueMicrotask(() => undefined);
    await Promise.race([Promise.resolve("raced")]);
    await Promise.allSettled([Promise.resolve("settled")]);
    await fetch("https://example.com");
    await import("node:fs/promises");
    const childProcess = require("node:child_process");
    new WebSocket("wss://example.com");
    new Worker(new URL(import.meta.url));
    request("https://example.com");
    readFileSync(new URL(import.meta.url));
    return `${timestamp}:${random}:${childProcess}`;
  }
});
