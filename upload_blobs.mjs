#!/usr/bin/env node
/**
 * Uploads the files the tracker staged this run to the Vercel Blob store.
 *
 * The tracker writes _upload_manifest.json listing only what changed, so a
 * normal run pushes ~140 files rather than the whole ~800-file tree.
 *
 * Usage: node upload_blobs.mjs <data-dir>
 * Requires BLOB_READ_WRITE_TOKEN in the environment.
 */
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { put } from '@vercel/blob';

const MANIFEST = '_upload_manifest.json';
// Blob's minimum is 60s. The tracker runs every 30 min, so this only bounds
// how long the dashboard can serve a stale copy after an update.
const CACHE_MAX_AGE = 60;
const CONCURRENCY = 8;
const MAX_ATTEMPTS = 4;

const dataDir = process.argv[2] ?? '.';

if (!process.env.BLOB_READ_WRITE_TOKEN) {
  console.error('BLOB_READ_WRITE_TOKEN is not set');
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function uploadOne(relpath) {
  const body = await readFile(path.join(dataDir, relpath));

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await put(relpath, body, {
        access: 'public',
        contentType: 'application/json',
        addRandomSuffix: false,
        allowOverwrite: true,
        cacheControlMaxAge: CACHE_MAX_AGE,
      });
      return;
    } catch (err) {
      if (attempt === MAX_ATTEMPTS) throw err;
      // Mostly rate limiting (advanced-operation cap) or transient network.
      const backoff = 500 * 2 ** (attempt - 1);
      console.warn(`  retry ${attempt}/${MAX_ATTEMPTS - 1} for ${relpath}: ${err.message}`);
      await sleep(backoff);
    }
  }
}

async function main() {
  let manifest;
  try {
    manifest = JSON.parse(await readFile(path.join(dataDir, MANIFEST), 'utf-8'));
  } catch (err) {
    console.error(`Could not read ${MANIFEST}: ${err.message}`);
    process.exit(1);
  }

  if (!Array.isArray(manifest) || manifest.length === 0) {
    console.error('Upload manifest is empty - the tracker staged nothing. Treating as a failure.');
    process.exit(1);
  }

  console.log(`Uploading ${manifest.length} file(s) to Blob...`);
  const started = Date.now();

  const queue = [...manifest];
  const failures = [];
  let done = 0;

  async function worker() {
    while (queue.length > 0) {
      const relpath = queue.shift();
      try {
        await uploadOne(relpath);
        done++;
      } catch (err) {
        failures.push({ relpath, message: err.message });
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, manifest.length) }, worker));

  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`Uploaded ${done}/${manifest.length} file(s) in ${elapsed}s`);

  if (failures.length > 0) {
    console.error(`${failures.length} upload(s) failed:`);
    for (const f of failures.slice(0, 20)) console.error(`  ${f.relpath}: ${f.message}`);
    process.exit(1);
  }
}

await main();
