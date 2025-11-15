#!/usr/bin/env node

import * as yredis from "@y/redis";
import * as env from "lib0/environment";
import * as Y from "yjs";

const redisPrefix = env.getConf("redis-prefix") || "y";
const postgresUrl = env.getConf("postgres");
const s3Endpoint = env.getConf("s3-endpoint");
let _lastUpdateCallbackError = 0;
let _updateCallbackBackoffUntil = 0;

let store;
if (s3Endpoint) {
  console.log("using s3 store");
  const { createS3Storage } = await import("../src/storage/s3.js");
  const bucketName = "ydocs";
  store = createS3Storage(bucketName);
  try {
    // make sure the bucket exists
    await store.client.makeBucket(bucketName);
  } catch (e) {}
} else if (postgresUrl) {
  console.log("using postgres store");
  const { createPostgresStorage } = await import("../src/storage/postgres.js");
  store = await createPostgresStorage();
} else {
  console.log("ATTENTION! using in-memory store");
  const { createMemoryStorage } = await import("../src/storage/memory.js");
  store = createMemoryStorage();
}

let ydocUpdateCallback = env.getConf("ydoc-update-callback");
if (ydocUpdateCallback != null && ydocUpdateCallback.slice(-1) !== "/") {
  ydocUpdateCallback += "/";
}

/**
 * @type {(room: string, ydoc: Y.Doc) => Promise<void>}
 */
const updateCallback = async (room, ydoc) => {
  if (ydocUpdateCallback != null) {
    const nowStart = Date.now();
    if (nowStart < _updateCallbackBackoffUntil) {
      return;
    }
    // call YDOC_UPDATE_CALLBACK here
    const formData = new FormData();
    // @todo only convert ydoc to updatev2 once
    const yUpdate = Y.encodeStateAsUpdateV2(ydoc);
    const arrayBuffer = new ArrayBuffer(yUpdate.byteLength);
    new Uint8Array(arrayBuffer).set(yUpdate);
    formData.append("ydoc", new Blob([arrayBuffer]));
    // @todo should add a timeout to fetch (see fetch signal abortcontroller)
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 3000);
      const res = await fetch(new URL(room, ydocUpdateCallback), {
        body: formData,
        method: "PUT",
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (!res.ok) {
        const now = Date.now();
        _updateCallbackBackoffUntil = now + 5 * 60 * 1000; // 5 minutes
        if (now - _lastUpdateCallbackError > 60000) {
          console.error(
            `Issue sending data to YDOC_UPDATE_CALLBACK. status="${res.status}" statusText="${res.statusText}"`
          );
          _lastUpdateCallbackError = now;
        }
      }
    } catch (e) {
      const now = Date.now();
      _updateCallbackBackoffUntil = now + 5 * 60 * 1000; // 5 minutes
      if (now - _lastUpdateCallbackError > 60000) {
        console.error("YDOC_UPDATE_CALLBACK request failed", e);
        _lastUpdateCallbackError = now;
      }
    }
  }
};

yredis.createWorker(store, redisPrefix, {
  updateCallback,
});
