import assert from 'node:assert/strict';
import { appendFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { alignUnknownIntoDominant } from '../src/queue/align-unknown.js';
import { recoverCodexLedgerTransaction } from '../src/parsers/codex-ledger-state.js';
import { syncCodex } from '../src/sync/index.js';
import { appendBuckets, clearCursors, loadCursors, loadRecentBuckets, resetCursorsCache, resetLocalUsageCache } from '../src/queue/index.js';
import { resetSqliteQueryCache } from '../src/parsers/sqlite.js';
import { resetJsonlWalkCache } from '../src/parsers/shared.js';
import type { QueueBucket, TudConfig } from '../src/types.js';

const SINCE = '2026-01-01T00:00:00.000Z';
const TIME = '2026-02-28T23:50:00.000Z';
const DETAIL_TIME = '2026-03-01T01:01:00.000Z';
async function fixture(run: (f: { dir: string; home: string; db: string; file: string; config: TudConfig; sync: () => ReturnType<typeof syncCodex>; rows: () => Promise<QueueBucket[]> }) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), 'jusage-reconcile-'));
  const home = join(dir, 'codex');
  const env = Object.fromEntries(['HOME', 'USERPROFILE', 'CODEX_HOME', 'AI_USAGE_CODEX_HOME'].map(k => [k, process.env[k]]));
  process.env.HOME = dir;
  process.env.USERPROFILE = dir;
  process.env.CODEX_HOME = home;
  process.env.AI_USAGE_CODEX_HOME = home;
  await mkdir(join(home, 'sessions'), { recursive: true });
  const db = join(home, 'state_5.sqlite');
  const file = join(home, 'sessions', 'rollout-thread.jsonl');
  const sqlite = new DatabaseSync(db);
  sqlite.exec('CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT, tokens_used INTEGER, model TEXT, cwd TEXT, recency_at_ms INTEGER, created_at_ms INTEGER)');
  sqlite.prepare('INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?)').run('thread', file, 100, 'unknown', '/synthetic/old-project', Date.parse(TIME), Date.parse(TIME));
  sqlite.close();
  resetSqliteQueryCache(); resetJsonlWalkCache(); resetCursorsCache(dir);
  const config = { dataDir: dir, deviceId: 'synthetic', hostname: 'synthetic', statsSince: SINCE, localCollectSince: SINCE, juejin: { enabled: false, apiUrl: '', authMode: 'device', token: null } } as TudConfig;
  try { await run({dir, home, db, file, config, sync: () => syncCodex(dir, config), rows: () => loadRecentBuckets(dir, SINCE)}); }
  finally {
    for (const [k, value] of Object.entries(env)) { if (value === undefined) delete process.env[k]; else process.env[k] = value; }
    resetSqliteQueryCache(); resetJsonlWalkCache(); resetCursorsCache(dir);
    await rm(dir, { recursive: true, force: true });
  }
}
function detail(count: number, cumulative: number | undefined, timestamp = DETAIL_TIME, model = 'gpt-test'): string {
  return JSON.stringify({timestamp, type: 'event_msg', payload: {type: 'token_count', info: {model, last_token_usage: { input_tokens: count, output_tokens: 0, total_tokens: count }, ...(cumulative === undefined ? {} : {total_token_usage: {input_tokens: cumulative, output_tokens: 0, total_tokens: cumulative}})}}}) + '\n';
}
async function rollout(file: string, body: string): Promise<void> {
  await writeFile(file, JSON.stringify({type: 'session_meta', payload: {id: 'thread', cwd: '/synthetic/new-project'}}) + '\n' + body);
  resetJsonlWalkCache();
}
function total(rows: QueueBucket[]): number { return rows.reduce((sum, row) => sum + row.total_tokens, 0); }

test('sync replaces a tracked ledger gap with late precise usage across month/model/project and emits zero snapshot', async () => {
  await fixture(async f => {
    await f.sync(); assert.equal(total(await f.rows()), 100);
    await rollout(f.file, detail(100, 100));
    const result = await f.sync();
    assert.equal(total(await f.rows()), 100);
    assert.equal(result.writtenBuckets.find(row => row.collector === 'codex-ledger')?.total_tokens, 0);
    assert.equal((await f.rows()).find(row => row.collector !== 'codex-ledger')?.model, 'gpt-test');
  });
});

test('partial precise intervals retire only their overlap, and replay remains idempotent', async () => {
  await fixture(async f => {
    await f.sync();
    await rollout(f.file, detail(60, 60)); await f.sync();
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 40);
    await appendFile(f.file, detail(40, 100, '2026-03-01T01:32:00.000Z')); await f.sync();
    assert.equal(total(await f.rows()), 100);
    await clearCursors(f.dir); await f.sync();
    assert.equal(total(await f.rows()), 100);
  });
});

function updateLedger(dbPath: string, tokens: number, id = 'thread'): void {
  const db = new DatabaseSync(dbPath);
  db.prepare('UPDATE threads SET tokens_used = ? WHERE id = ?').run(tokens, id);
  db.close(); resetSqliteQueryCache();
}

test('new precise usage after the gap does not retire older missing intervals', async () => {
  await fixture(async f => {
    await f.sync();
    await rollout(f.file, detail(50, 150));
    await f.sync();
    let rows = await f.rows();
    assert.equal(total(rows), 150);
    assert.equal(rows.find(row => row.collector === 'codex-ledger')?.total_tokens, 100);
    // Ledger catching up must not mistake the precise lead for a counter reset.
    updateLedger(f.db, 150); await f.sync();
    rows = await f.rows(); assert.equal(total(rows), 150);
    assert.equal(rows.find(row => row.collector === 'codex-ledger')?.ledger_unverified_tokens, 0);
  });
});

test('a detail delta without cumulative position cannot subtract a ledger estimate', async () => {
  await fixture(async f => {
    await f.sync(); await rollout(f.file, detail(100, undefined)); await f.sync();
    assert.equal(total(await f.rows()), 200);
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 100);
  });
});

test('partial interval spanning an existing gap retires its exact intersection only', async () => {
  await fixture(async f => {
    await f.sync(); await rollout(f.file, detail(60, 130)); await f.sync();
    assert.equal(total(await f.rows()), 130);
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 70);
  });
});

test('two threads sharing one ledger bucket keep independent remaining ranges', async () => {
  await fixture(async f => {
    const db = new DatabaseSync(f.db);
    db.prepare('INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?)').run('other-thread', join(f.home, 'sessions', 'other.jsonl'), 80, 'unknown', '/synthetic/old-project', Date.parse(TIME), Date.parse(TIME));
    db.close(); resetSqliteQueryCache();
    await f.sync(); assert.equal(total(await f.rows()), 180);
    await rollout(f.file, detail(100, 100)); await f.sync();
    assert.equal(total(await f.rows()), 180);
    const ledger = (await f.rows()).filter(row => row.collector === 'codex-ledger');
    assert.equal(ledger.length, 1); assert.equal(ledger[0]?.total_tokens, 80);
    assert.equal(ledger[0]?.conversation_count, 1);
  });
});

test('legacy baseline survives migration, cursor clearing, and full cache reset', async () => {
  await fixture(async f => {
    const baseline: QueueBucket = {source: 'codex', collector: 'codex-ledger', model: 'unknown', project: 'old-project', hour_start: '2026-02-28T23:30:00.000Z', input_tokens: 100, output_tokens: 0, cached_input_tokens: 0, cache_creation_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: 100, conversation_count: 1};
    await appendBuckets(f.dir, [baseline]);
    await f.sync();
    assert.equal(total(await f.rows()), 100);
    assert.equal((await f.rows())[0]?.ledger_unverified_tokens, 100);
    await rollout(f.file, detail(100, 100)); await f.sync();
    assert.equal(total(await f.rows()), 200);
    await clearCursors(f.dir); await f.sync(); assert.equal(total(await f.rows()), 200);
    await resetLocalUsageCache(f.dir); await f.sync(); assert.equal(total(await f.rows()), 200);
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.ledger_unverified_tokens, 100);
  });
});

test('counter reset never retires the earlier generation and labels it unverified', async () => {
  await fixture(async f => {
    await f.sync();
    updateLedger(f.db, 40);
    await rollout(f.file, detail(40, 40)); await f.sync();
    assert.equal(total(await f.rows()), 140);
    const ledger = (await f.rows()).find(row => row.collector === 'codex-ledger')!;
    assert.equal(ledger.total_tokens, 100); assert.equal(ledger.ledger_unverified_tokens, 100);
    updateLedger(f.db, 100);
    await appendFile(f.file, detail(60, 100, '2026-03-01T02:01:00.000Z')); await f.sync();
    assert.equal(total(await f.rows()), 200);
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 100);
  });
});

test('ledger unknown model retains its original key during dominant-model alignment', async () => {
  await fixture(async f => {
    await f.sync(); const unknown = (await f.rows())[0]!;
    const known = {...unknown, model: 'gpt-test', input_tokens: 20, total_tokens: 20};
    assert.deepEqual(alignUnknownIntoDominant([unknown, known], {retractUnknown: true}), [unknown, known]);
  });
});

test('empty subsequent sync has no replacement rows or repeated token additions', async () => {
  await fixture(async f => {
    await f.sync(); const before = await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8');
    const next = await f.sync();
    assert.deepEqual(next.writtenBuckets, []); assert.equal(total(await f.rows()), 100);
    assert.equal(await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8'), before);
  });
});

for (const stage of ['journal', 'one-month', 'all-rows', 'cursor', 'provenance'] as const) {
  test(`recovery replays final snapshots after interruption at ${stage}`, async () => {
    await fixture(async f => {
      await f.sync();
      const oldRows = await f.rows();
      const oldCursor = await readFile(join(f.dir, 'cursors.json'), 'utf8');
      const oldState = await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8');
      await rollout(f.file, detail(100, 100));
      const changed = await f.sync();
      const expectedCursor = await readFile(join(f.dir, 'cursors.json'), 'utf8');
      const expectedState = await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8');
      await rm(join(f.dir, 'queue'), {recursive: true});
      await appendBuckets(f.dir, oldRows);
      await writeFile(join(f.dir, 'cursors.json'), oldCursor);
      await writeFile(join(f.dir, 'codex-ledger-provenance.json'), oldState);
      if (stage === 'one-month') {
        await appendBuckets(f.dir, changed.writtenBuckets.slice(0, 1));
        // Interrupted append can leave a non-newline-terminated partial JSON row.
        await appendFile(join(f.dir, 'queue', '2026-02.jsonl'), '{"source":"codex","input_');
      }
      if (stage === 'all-rows' || stage === 'cursor' || stage === 'provenance') await appendBuckets(f.dir, changed.writtenBuckets);
      if (stage === 'cursor' || stage === 'provenance') await writeFile(join(f.dir, 'cursors.json'), expectedCursor);
      if (stage === 'provenance') await writeFile(join(f.dir, 'codex-ledger-provenance.json'), expectedState);
      await writeFile(join(f.dir, 'codex-ledger-pending.json'), JSON.stringify({version: 1, buckets: changed.writtenBuckets, codexCursor: JSON.parse(expectedCursor).codex, provenance: JSON.parse(expectedState)}));
      resetCursorsCache(f.dir);
      // Both entry points must recover before any caller can see partial totals.
      if (stage === 'cursor') await loadCursors(f.dir);
      assert.equal(total(await f.rows()), 100);
      assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 0);
      assert.deepEqual((await loadCursors(f.dir)).codex, JSON.parse(expectedCursor).codex);
      assert.deepEqual(JSON.parse(await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8')), JSON.parse(expectedState));
      await assert.rejects(readFile(join(f.dir, 'codex-ledger-pending.json')), {code: 'ENOENT'});
      await recoverCodexLedgerTransaction(f.dir); await f.sync();
      assert.equal(total(await f.rows()), 100);
    });
  });
}

test('malformed pending transaction fails closed before reads or another sync', async () => {
  await fixture(async f => {
    await f.sync();
    const queueBefore = await readFile(join(f.dir, 'queue', '2026-02.jsonl'), 'utf8');
    await writeFile(join(f.dir, 'codex-ledger-pending.json'), '{"version":1,"buckets":[]}');
    await assert.rejects(f.rows(), /Invalid pending Codex ledger transaction/);
    await assert.rejects(loadCursors(f.dir), /Invalid pending Codex ledger transaction/);
    await assert.rejects(f.sync(), /Invalid pending Codex ledger transaction/);
    assert.equal(await readFile(join(f.dir, 'queue', '2026-02.jsonl'), 'utf8'), queueBefore);
  });
});

test('malformed provenance is not silently treated as empty history', async () => {
  await fixture(async f => {
    await f.sync();
    await writeFile(join(f.dir, 'codex-ledger-provenance.json'), '{"version":99}');
    await assert.rejects(f.sync(), /Invalid Codex ledger provenance/);
    assert.equal(total(await f.rows()), 100);
  });
});


test('queue readers never expose a partially replaced cross-month ledger snapshot', async () => {
  await fixture(async f => {
    await f.sync(); await rollout(f.file, detail(100, 100));
    const syncing = f.sync();
    const reads = await Promise.all(Array.from({length: 10}, () => f.rows()));
    await syncing;
    for (const rows of reads) assert.equal(total(rows), 100);
    assert.equal(total(await f.rows()), 100);
  });
});

for (const damage of ['other-source', 'duplicate-key', 'wrong-total', 'negative-offset'] as const) {
  test(`structurally valid but corrupt journal ${damage} is rejected`, async () => {
    await fixture(async f => {
      await f.sync();
      const buckets = await f.rows();
      const provenance = JSON.parse(await readFile(join(f.dir, 'codex-ledger-provenance.json'), 'utf8'));
      const codexCursor = (await loadCursors(f.dir)).codex!;
      if (damage === 'other-source') buckets[0]!.source = 'claude';
      if (damage === 'duplicate-key') buckets.push({...buckets[0]!});
      if (damage === 'wrong-total') buckets[0]!.total_tokens += 1;
      if (damage === 'negative-offset') codexCursor.files['synthetic.jsonl'] = {inode: 1, offset: -1};
      await writeFile(join(f.dir, 'codex-ledger-pending.json'), JSON.stringify({version: 1, buckets, codexCursor, provenance}));
      await assert.rejects(f.rows(), /(?:Invalid|Duplicate) pending Codex/);
    });
  });
}


test('delta-only detail rescans do not advance the persistent watermark twice', async () => {
  await fixture(async f => {
    await rollout(f.file, detail(100, undefined)); await f.sync();
    assert.equal(total(await f.rows()), 100);
    await clearCursors(f.dir); await f.sync();
    assert.equal(total(await f.rows()), 100);
    updateLedger(f.db, 150); await f.sync();
    assert.equal(total(await f.rows()), 150);
    assert.equal((await f.rows()).find(row => row.collector === 'codex-ledger')?.total_tokens, 50);
  });
});

test('a file scan spanning a counter reset does not keep the older cumulative maximum', async () => {
  await fixture(async f => {
    await f.sync();
    updateLedger(f.db, 40);
    await rollout(f.file, detail(100, 100) + detail(40, 40, '2026-03-01T02:01:00.000Z'));
    await f.sync();
    assert.equal(total(await f.rows()), 240); // ambiguous old gap is retained
    updateLedger(f.db, 60); await f.sync();
    assert.equal(total(await f.rows()), 260); // new-generation missing tail is 20
  });
});
