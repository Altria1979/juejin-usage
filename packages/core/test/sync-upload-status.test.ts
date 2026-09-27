import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadConfig } from '../src/config.js';
import {
  BucketStore,
  getSyncStatusPayload,
  runSync,
} from '../src/server/state.js';
import { saveUploadStateFile, setUploadSlot } from '../src/upload/state.js';

test('manual runner reads persisted upload outcome separately from local collection', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'jusage-sync-status-'));
  try {
    const { config } = await loadConfig(dir);
    config.juejin = {
      enabled: true,
      apiUrl: 'https://example.invalid',
      authMode: 'tbd',
      token: 'synthetic-user',
    };
    const result = await runSync({
      dataDir: dir,
      getConfig: () => config,
      bucketStore: new BucketStore(),
      runSyncViaRunner: async () => {
        await saveUploadStateFile(
          dir,
          setUploadSlot(
            { version: 2, remotes: {} },
            config.juejin.apiUrl!,
            config.deviceId,
            {
              buckets: {},
              lastAttemptAt: '2026-01-01T00:00:00Z',
              lastError: 'HTTP 503',
              backfill: {
                items: [{ key: 'synthetic', attempts: 1, nextRetryAt: null }],
              },
            },
          ),
        );
        return [];
      },
    });
    assert.equal(result.ok, true);
    assert.equal(result.upload.state, 'failed');
    assert.equal(result.upload.pendingBuckets, 1);
    const status = await getSyncStatusPayload(dir, config, [], {
      claude: false,
      codex: false,
    });
    assert.deepEqual(status.upload, result.upload);
    config.juejin.enabled = false;
    const disabled = await getSyncStatusPayload(dir, config, [], {
      claude: false,
      codex: false,
    });
    assert.equal(disabled.upload?.state, 'disabled');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('runner source failures fail local sync while preserving the separate persisted upload outcome', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'jusage-sync-source-error-'));
  try {
    const { config } = await loadConfig(dir);
    config.juejin = {
      enabled: true,
      apiUrl: 'https://example.invalid',
      authMode: 'tbd',
      token: 'synthetic-user',
    };
    await saveUploadStateFile(
      dir,
      setUploadSlot(
        { version: 2, remotes: {} },
        config.juejin.apiUrl!,
        config.deviceId,
        {
          buckets: {},
          lastConfirmedAt: '2026-01-01T00:00:00Z',
          backfill: { items: [] },
        },
      ),
    );
    for (const { error, skipped } of [
      {
        error: 'Invalid pending Codex ledger transaction; collection paused',
        skipped: true,
      },
      { error: undefined, skipped: true },
      { error: undefined, skipped: false },
    ]) {
      const sourceResult = {
        source: 'codex',
        eventsParsed: 0,
        filesProcessed: 0,
        bucketsWritten: 0,
        writtenBuckets: [],
        skipped,
        ...(error ? { error } : {}),
      };
      const result = await runSync({
        dataDir: dir,
        getConfig: () => config,
        bucketStore: new BucketStore(),
        runSyncViaRunner: async () => [sourceResult],
      });
      assert.equal(result.ok, error === undefined);
      assert.deepEqual(result.results, [sourceResult]);
      assert.equal(result.upload.state, 'confirmed');
      assert.equal(result.upload.lastConfirmedAt, '2026-01-01T00:00:00Z');
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
