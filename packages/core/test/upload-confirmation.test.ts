import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { aggregateForIngest } from '../src/aggregate.js';
import { appendBuckets } from '../src/queue/index.js';
import { ingestBucketKey } from '../src/queue/keys.js';
import type { QueueBucket, TudConfig } from '../src/types.js';
import {
  drainBackfillRound,
  getUploadStatus,
  uploadToServer,
} from '../src/upload/client.js';
import { applyCalibrateSelectedDates } from '../src/upload/calibrate.js';
import { eventsMatch, readRemoteEvents } from '../src/upload/confirmation.js';
import { DEFAULT_STATS_TIMEZONE, localDateAndHour } from '../src/timezone.js';
import { bucketToIngestEvent } from '../src/upload/events.js';
import {
  bucketHash,
  commitBucketHashes,
  getUploadSlot,
  loadUploadStateFile,
  saveUploadStateFile,
  setUploadSlot,
} from '../src/upload/state.js';

const apiUrl = 'https://example.invalid';
const deviceId = '550e8400-e29b-41d4-a716-446655440000';
function row(tokens = 100): QueueBucket {
  return {
    source: 'codex',
    collector: 'codex-cli',
    model: 'test-model',
    project: 'synthetic',
    hour_start: new Date(
      Math.floor(Date.now() / 1_800_000) * 1_800_000,
    ).toISOString(),
    input_tokens: tokens,
    output_tokens: 0,
    cached_input_tokens: 0,
    cache_creation_input_tokens: 0,
    reasoning_output_tokens: 0,
    total_tokens: tokens,
    conversation_count: 0,
  };
}
function config(dataDir: string): TudConfig {
  return {
    dataDir,
    deviceId,
    hostname: 'test',
    statsSince: '2020-01-01T00:00:00.000Z',
    juejin: {
      enabled: true,
      apiUrl,
      token: 'synthetic-linked-user',
      authMode: 'tbd',
    },
  };
}
const json = (data: unknown) =>
  new Response(JSON.stringify({ success: true, data }));
async function harness(
  fn: (dir: string, bucket: QueueBucket) => Promise<void>,
) {
  const dir = await mkdtemp(join(tmpdir(), 'upload-confirmation-'));
  const fetchBefore = globalThis.fetch;
  try {
    const bucket = row();
    await appendBuckets(dir, [bucket]);
    await fn(dir, bucket);
  } finally {
    globalThis.fetch = fetchBefore;
    await rm(dir, { recursive: true, force: true });
  }
}
const watermark = () =>
  json({
    ingestMinOccurredAt: '2020-01-01T00:00:00.000Z',
    lastUploadAt: new Date().toISOString(),
  });

test('zero accepted never commits hash; pending is already durable when POST starts', async () => {
  await harness(async (dir, bucket) => {
    let durableAtPost = false;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      if (String(input).includes('tud-usage-device-events'))
        return json({ events: [] });
      const slot = getUploadSlot(
        await loadUploadStateFile(dir),
        apiUrl,
        deviceId,
      );
      durableAtPost = !!slot.backfill?.items.some(
        (item) => item.key === ingestBucketKey(bucket),
      );
      const pending = slot.backfill!.items.find(
        (item) => item.key === ingestBucketKey(bucket),
      )!;
      assert.equal(pending.event?.conversations_count, 1);
      assert.equal(pending.event?.reported_cost_usd, undefined);
      assert.deepEqual(
        pending.event,
        bucketToIngestEvent(aggregateForIngest([bucket])[0]!, deviceId),
      );
      return json({ accepted_count: 0, duplicate_count: 0 });
    }) as typeof fetch;
    await uploadToServer(dir, config(dir), { skipDrain: true }).catch(
      () => undefined,
    );
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(durableAtPost, true);
    assert.equal(slot.buckets[ingestBucketKey(bucket)], undefined);
    assert.equal(slot.backfill?.items.length, 1);
  });
});

test('duplicate needs exact readback and ignores shifted timestamp representation', async () => {
  await harness(async (dir, bucket) => {
    const event = bucketToIngestEvent(
      aggregateForIngest([bucket])[0]!,
      deviceId,
    )!;
    let reads = 0;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      if (String(input).includes('tud-usage-device-events')) {
        reads++;
        return json({
          events: [
            {
              ...event,
              occurred_at: new Date(
                Date.parse(event.occurred_at) + 28_800_000,
              ).toISOString(),
            },
          ],
        });
      }
      return json({ accepted_count: 0, duplicate_count: 1 });
    }) as typeof fetch;
    const result = await uploadToServer(dir, config(dir), { skipDrain: true });
    assert.ok(reads > 0);
    assert.equal(result?.uploaded, 1);
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(slot.backfill?.items.length, 0);
    assert.equal(
      slot.buckets[ingestBucketKey(bucket)],
      bucketHash(aggregateForIngest([bucket])[0]!),
    );
  });
});

test('upgrade repair sends previously poisoned matching full hashes once', async () => {
  await harness(async (dir, bucket) => {
    await saveUploadStateFile(
      dir,
      setUploadSlot({ version: 2, remotes: {} }, apiUrl, deviceId, {
        ...commitBucketHashes({ buckets: {} }, aggregateForIngest([bucket])),
        backfill: { items: [], enqueuedSince: '2020-01-01T00:00:00.000Z' },
      }),
    );
    let posts = 0;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      posts++;
      return json({ accepted_count: 1, duplicate_count: 0 });
    }) as typeof fetch;
    await uploadToServer(dir, config(dir), {
      recentBuckets: [],
      skipDrain: true,
    });
    await uploadToServer(dir, config(dir), {
      recentBuckets: [],
      skipDrain: true,
    });
    assert.equal(posts, 1);
  });
});

test('partial accepted commits only readback matches and preserves every other snapshot', async () => {
  await harness(async (dir, bucket) => {
    const other = {
      ...bucket,
      model: 'other-model',
      input_tokens: 200,
      total_tokens: 200,
    };
    await appendBuckets(dir, [other]);
    const event = bucketToIngestEvent(
      aggregateForIngest([bucket])[0]!,
      deviceId,
    )!;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      if (String(input).includes('tud-usage-device-events'))
        return json({ events: [event] });
      return json({ accepted_count: 1, duplicate_count: 1 });
    }) as typeof fetch;
    const result = await uploadToServer(dir, config(dir), { skipDrain: true });
    assert.equal(result?.uploaded, 1);
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(slot.backfill?.items.length, 1);
    assert.equal(slot.backfill?.items[0]?.snapshot?.input_tokens, 200);
    assert.equal(slot.buckets[ingestBucketKey(other)], undefined);
    assert.equal((await getUploadStatus(dir, config(dir))).state, 'failed');
  });
});

test('missing counts, zero acceptance and invalid counts require readback', async () => {
  for (const counts of [
    {},
    { accepted_count: 0, duplicate_count: 0 },
    { accepted_count: -1, duplicate_count: 2 },
    { accepted_count: 2, duplicate_count: 0 },
    { accepted_count: 1, duplicate_count: 1 },
  ]) {
    await harness(async (dir, bucket) => {
      let reads = 0;
      globalThis.fetch = (async (input) => {
        if (String(input).includes('tud-sync-status')) return watermark();
        if (String(input).includes('tud-usage-device-events')) {
          reads++;
          return json({ events: [] });
        }
        return json(counts);
      }) as typeof fetch;
      await uploadToServer(dir, config(dir), { skipDrain: true });
      assert.ok(reads > 0);
      const slot = getUploadSlot(
        await loadUploadStateFile(dir),
        apiUrl,
        deviceId,
      );
      assert.equal(slot.buckets[ingestBucketKey(bucket)], undefined);
      assert.equal(slot.backfill?.items.length, 1);
    });
  }
});

test('late local growth keeps newest pending snapshot and never confirms unsent data', async () => {
  await harness(async (dir, bucket) => {
    const grown = { ...bucket, input_tokens: 150, total_tokens: 150 };
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      await appendBuckets(dir, [grown]);
      return json({ accepted_count: 1, duplicate_count: 0 });
    }) as typeof fetch;
    await uploadToServer(dir, config(dir), { skipDrain: true });
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(
      slot.buckets[ingestBucketKey(bucket)],
      bucketHash(aggregateForIngest([bucket])[0]!),
    );
    assert.equal(slot.backfill?.items[0]?.snapshot?.input_tokens, 150);
    assert.equal((await getUploadStatus(dir, config(dir))).state, 'pending');
    await drainBackfillRound(dir, config(dir));
    assert.equal((await getUploadStatus(dir, config(dir))).state, 'confirmed');
  });
});

test('timeout keeps a durable snapshot, and background retry works without new collection', async () => {
  await harness(async (dir, bucket) => {
    let fail = true;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      if (fail) throw new Error('synthetic timeout');
      return json({ accepted_count: 1, duplicate_count: 0 });
    }) as typeof fetch;
    await assert.rejects(
      uploadToServer(dir, config(dir), { skipDrain: true }),
      /timeout/,
    );
    assert.equal((await getUploadStatus(dir, config(dir))).state, 'failed');
    let slot = getUploadSlot(await loadUploadStateFile(dir), apiUrl, deviceId);
    assert.equal(slot.backfill?.items[0]?.snapshot?.total_tokens, 100);
    fail = false;
    await drainBackfillRound(dir, config(dir), { nowMs: Date.now() + 120_000 });
    slot = getUploadSlot(await loadUploadStateFile(dir), apiUrl, deviceId);
    assert.equal(slot.backfill?.items.length, 0);
    assert.equal(
      slot.buckets[ingestBucketKey(bucket)],
      bucketHash(aggregateForIngest([bucket])[0]!),
    );
  });
});

test('live tasks hold below or without ingest floor, then recover when it expands', async () => {
  for (const initial of [
    null,
    new Date(Date.now() + 86_400_000).toISOString(),
  ]) {
    await harness(async (dir) => {
      let floor: string | null = initial;
      let posts = 0;
      globalThis.fetch = (async (input) => {
        if (String(input).includes('tud-sync-status'))
          return json({
            ingestMinOccurredAt: floor,
            lastUploadAt: new Date().toISOString(),
          });
        posts++;
        return json({ accepted_count: 1, duplicate_count: 0 });
      }) as typeof fetch;
      await uploadToServer(dir, config(dir), { skipDrain: true });
      assert.equal(posts, 0);
      assert.equal((await getUploadStatus(dir, config(dir))).state, 'pending');
      floor = '2020-01-01T00:00:00.000Z';
      await drainBackfillRound(dir, config(dir), {
        nowMs: Date.now() + 120_000,
      });
      assert.equal(posts, 1);
      assert.equal(
        (await getUploadStatus(dir, config(dir))).state,
        'confirmed',
      );
    });
  }
});

test('readback compares all token classes, normalized conversations and explicit cost', () => {
  const event = bucketToIngestEvent(aggregateForIngest([row()])[0]!, deviceId)!;
  assert.equal(eventsMatch(event, { ...event, conversations_count: 0 }), true);
  assert.equal(eventsMatch(event, { ...event, conversations_count: 2 }), false);
  assert.equal(eventsMatch(event, { ...event, reported_cost_usd: 1 }), false);
  for (const field of Object.keys(event.usage) as Array<
    keyof typeof event.usage
  >) {
    assert.equal(
      eventsMatch(event, {
        ...event,
        usage: { ...event.usage, [field]: event.usage[field] + 1 },
      }),
      false,
    );
  }
  assert.equal(
    eventsMatch(
      { ...event, reported_cost_usd: 1 },
      { ...event, reported_cost_usd: 1 },
    ),
    true,
  );
});

test('readback subdivides full ranges without using the broken cursor', async () => {
  const original = globalThis.fetch;
  const event = bucketToIngestEvent(aggregateForIngest([row()])[0]!, deviceId)!;
  let calls = 0;
  try {
    globalThis.fetch = (async (input) => {
      const url = new URL(String(input));
      assert.equal(url.searchParams.has('cursor'), false);
      calls++;
      return json(
        calls === 1
          ? { events: [event], next_cursor: 'broken-cursor' }
          : { events: [event] },
      );
    }) as typeof fetch;
    const remote = await readRemoteEvents(
      apiUrl,
      'synthetic',
      deviceId,
      '2026-01-01T00:00:00Z',
      '2026-01-01T01:00:00Z',
    );
    assert.equal(remote.complete, true);
    assert.equal(calls, 3);
    assert.equal(remote.events.length, 1);
  } finally {
    globalThis.fetch = original;
  }
});

test('calibration confirms its sent snapshot, preserves unrelated tasks and queues concurrent growth', async () => {
  await harness(async (dir, bucket) => {
    const unrelated = {
      ...bucket,
      hour_start: new Date(
        Date.parse(bucket.hour_start) - 2 * 86_400_000,
      ).toISOString(),
    };
    const key = ingestBucketKey(unrelated);
    await saveUploadStateFile(
      dir,
      setUploadSlot({ version: 2, remotes: {} }, apiUrl, deviceId, {
        buckets: {},
        repairVersion: 1,
        needsFullScan: true,
        backfill: {
          items: [{ key, attempts: 2, nextRetryAt: null, snapshot: unrelated }],
        },
      }),
    );
    globalThis.fetch = (async (input, init) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      if (String(input).includes('tud-usage-device-events'))
        return json({ events: [] });
      const sent = JSON.parse(String(init?.body));
      assert.equal(sent.events[0].usage.input_tokens, 100);
      await appendBuckets(dir, [
        { ...bucket, input_tokens: 150, total_tokens: 150 },
      ]);
      return json({
        deleted_count: 1,
        upserted_count: 1,
        floored_count: 0,
        received_at: new Date().toISOString(),
      });
    }) as typeof fetch;
    await applyCalibrateSelectedDates(dir, config(dir), [
      localDateAndHour(bucket.hour_start, DEFAULT_STATS_TIMEZONE).date,
    ]);
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(slot.needsFullScan, true);
    assert.equal(slot.repairVersion, 1);
    assert.ok(
      slot.backfill?.items.some(
        (item) => item.key === key && item.attempts === 2,
      ),
    );
    assert.equal(
      slot.backfill?.items.find((item) => item.key === ingestBucketKey(bucket))
        ?.snapshot?.input_tokens,
      150,
    );
    assert.equal(
      slot.buckets[ingestBucketKey(bucket)],
      bucketHash(aggregateForIngest([bucket])[0]!),
    );
  });
});

test('incremental full groups work across months and retain explicit zero snapshots', async () => {
  await harness(async (dir, bucket) => {
    const prior = new Date(bucket.hour_start);
    prior.setUTCDate(0);
    prior.setUTCHours(12, 0, 0, 0);
    const alpha = {
      ...bucket,
      hour_start: prior.toISOString(),
      project: 'alpha',
    };
    const beta = {
      ...alpha,
      project: 'beta',
      input_tokens: 200,
      total_tokens: 200,
    };
    await appendBuckets(dir, [alpha, beta]);
    await saveUploadStateFile(
      dir,
      setUploadSlot({ version: 2, remotes: {} }, apiUrl, deviceId, {
        ...commitBucketHashes(
          { buckets: {} },
          aggregateForIngest([bucket, alpha, beta]),
        ),
        repairVersion: 1,
        backfill: { items: [], enqueuedSince: '2020-01-01T00:00:00.000Z' },
      }),
    );
    const sent: Array<{
      usage: { input_tokens: number };
      occurred_at: string;
    }> = [];
    globalThis.fetch = (async (input, init) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      const payload = JSON.parse(String(init?.body));
      sent.push(...payload.events);
      return json({
        accepted_count: payload.events.length,
        duplicate_count: 0,
      });
    }) as typeof fetch;
    const grown = { ...alpha, input_tokens: 150, total_tokens: 150 };
    const zero = { ...bucket, input_tokens: 0, total_tokens: 0 };
    await appendBuckets(dir, [grown, zero]);
    await uploadToServer(dir, config(dir), {
      recentBuckets: [grown, zero],
      skipDrain: true,
    });
    assert.equal(sent.length, 2);
    assert.equal(
      sent.find((event) => event.occurred_at === alpha.hour_start)?.usage
        .input_tokens,
      350,
    );
    assert.equal(
      sent.find((event) => event.occurred_at === bucket.hour_start)?.usage
        .input_tokens,
      0,
    );
    assert.equal((await getUploadStatus(dir, config(dir))).state, 'confirmed');
  });
});

test('concurrent live and drain calls serialize and never overwrite slot metadata', async () => {
  await harness(async (dir) => {
    let active = 0;
    let peak = 0;
    let posts = 0;
    globalThis.fetch = (async (input) => {
      if (String(input).includes('tud-sync-status')) return watermark();
      active++;
      peak = Math.max(active, peak);
      posts++;
      await new Promise((resolve) => setTimeout(resolve, 10));
      active--;
      return json({ accepted_count: 1, duplicate_count: 0 });
    }) as typeof fetch;
    await Promise.all([
      uploadToServer(dir, config(dir), { skipDrain: true }),
      drainBackfillRound(dir, config(dir)),
      uploadToServer(dir, config(dir), { skipDrain: true }),
    ]);
    assert.equal(peak, 1);
    assert.equal(posts, 1);
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(slot.repairVersion, 1);
    assert.ok(slot.lastAttemptAt);
    assert.ok(slot.lastConfirmedAt);
    assert.equal(slot.backfill?.items.length, 0);
  });
});

test('a saturated smallest readback range is incomplete rather than proof of absence', async () => {
  const original = globalThis.fetch;
  try {
    globalThis.fetch = (async () =>
      json({ events: [], next_cursor: 'broken' })) as typeof fetch;
    const result = await readRemoteEvents(
      apiUrl,
      'synthetic',
      deviceId,
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.001Z',
    );
    assert.equal(result.complete, false);
  } finally {
    globalThis.fetch = original;
  }
});

test('corrupt upload state fails closed without replacing the pending file', async () => {
  await harness(async (dir) => {
    for (const broken of ['{truncated', '{"version":2}']) {
      const statePath = join(dir, 'upload.state.json');
      await writeFile(statePath, broken);
      let calls = 0;
      globalThis.fetch = (async () => {
        calls++;
        return watermark();
      }) as typeof fetch;
      await assert.rejects(
        uploadToServer(dir, config(dir), { skipDrain: true }),
        /上传状态读取失败/,
      );
      assert.equal(calls, 0);
      assert.equal(await readFile(statePath, 'utf8'), broken);
      assert.equal((await getUploadStatus(dir, config(dir))).state, 'failed');
    }
  });
});

test('calibration never mutates cloud when its readback inventory is incomplete', async () => {
  await harness(async (dir, bucket) => {
    let mutations = 0;
    globalThis.fetch = (async (_input, init) => {
      if (init?.method === 'POST') mutations++;
      return json({ events: [], next_cursor: 'broken' });
    }) as typeof fetch;
    await assert.rejects(
      applyCalibrateSelectedDates(dir, config(dir), [
        localDateAndHour(bucket.hour_start, DEFAULT_STATS_TIMEZONE).date,
      ]),
      /读取不完整/,
    );
    assert.equal(mutations, 0);
    const slot = getUploadSlot(
      await loadUploadStateFile(dir),
      apiUrl,
      deviceId,
    );
    assert.equal(slot.backfill?.items.length, 1);
    assert.equal(slot.buckets[ingestBucketKey(bucket)], undefined);
  });
});
