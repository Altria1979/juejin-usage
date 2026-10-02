import { existsSync, statSync } from 'node:fs';
import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { lock } from 'proper-lockfile';
import type { CursorsFile, QueueBucket } from '../types.js';
import { bucketKey } from '../queue/keys.js';
import { appendBuckets, resetCursorsCache } from '../queue/index.js';

const STATE_FILE = 'codex-ledger-provenance.json';
const JOURNAL_FILE = 'codex-ledger-pending.json';
export interface CodexPreciseInterval {
  from: number;
  to: number;
}
interface GapRange extends CodexPreciseInterval {
  bucket: string;
  generation: number;
  conversations: number;
}
interface ThreadState {
  watermark: number;
  generation: number;
  ledgerTotal: number | null;
  observedDeltas: Record<string, number>;
  /** Earlier generations stay visible, but can never be retired by current counters. */
  gaps: GapRange[];
}
export interface CodexLedgerState {
  version: 1;
  /** Full immutable legacy baselines, plus zero templates for newly tracked keys. */
  baselines: Record<string, QueueBucket>;
  threads: Record<string, ThreadState>;
  seedLegacyThreads: boolean;
  publishBaselines: boolean;
}
interface CodexLedgerJournal {
  version: 1;
  buckets: QueueBucket[];
  codexCursor: NonNullable<CursorsFile['codex']>;
  provenance: CodexLedgerState;
}

function count(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}
function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}
function validBucket(value: unknown): value is QueueBucket {
  if (!record(value) || !['codex', 'every-code'].includes(String(value.source)) ||
      !['model', 'project', 'hour_start'].every(key => typeof value[key] === 'string')) return false;
  if (!Number.isFinite(Date.parse(value.hour_start as string))) return false;
  const tokenFields = [
    'input_tokens', 'output_tokens', 'cached_input_tokens',
    'cache_creation_input_tokens', 'reasoning_output_tokens',
  ];
  return [...tokenFields, 'total_tokens', 'conversation_count'].every(key => count(value[key])) &&
    tokenFields.reduce((sum, key) => sum + (value[key] as number), 0) === value.total_tokens &&
    (value.ledger_unverified_tokens === undefined ||
      (count(value.ledger_unverified_tokens) && value.ledger_unverified_tokens <= (value.total_tokens as number)));
}
function validateState(value: unknown): asserts value is CodexLedgerState {
  if (!record(value) || value.version !== 1 || !record(value.baselines) ||
      !record(value.threads) || typeof value.seedLegacyThreads !== 'boolean' ||
      typeof value.publishBaselines !== 'boolean') {
    throw new Error('Invalid Codex ledger provenance; collection paused');
  }
  for (const [key, row] of Object.entries(value.baselines)) {
    if (!validBucket(row) || row.source !== 'codex' ||
        row.collector !== 'codex-ledger' || key !== bucketKey(row)) {
      throw new Error('Invalid Codex ledger baseline; collection paused');
    }
  }
  for (const thread of Object.values(value.threads)) {
    if (!record(thread) || !count(thread.watermark) || !count(thread.generation) ||
        (thread.ledgerTotal !== null && !count(thread.ledgerTotal)) ||
        !record(thread.observedDeltas) || !Object.values(thread.observedDeltas).every(count) ||
        !Array.isArray(thread.gaps)) {
      throw new Error('Invalid Codex ledger thread; collection paused');
    }
    for (const gap of thread.gaps) {
      if (!record(gap) || !count(gap.from) || !count(gap.to) || gap.from >= gap.to ||
          !count(gap.generation) || gap.generation > thread.generation ||
          !count(gap.conversations) || typeof gap.bucket !== 'string' || !value.baselines[gap.bucket]) {
        throw new Error('Invalid Codex ledger range; collection paused');
      }
    }
  }
}
function validCursor(value: unknown): value is NonNullable<CursorsFile['codex']> {
  if (!record(value) || !record(value.files)) return false;
  for (const file of Object.values(value.files)) {
    // fs.stat exposes Windows file IDs as numbers that can exceed the safe
    // integer range. Inodes are opaque identifiers; offsets and counts are not.
    if (!record(file) || typeof file.inode !== 'number' ||
        !Number.isInteger(file.inode) || file.inode < 0 || !count(file.offset) ||
        (file.tokenCountSeen !== undefined && !count(file.tokenCountSeen))) return false;
    if (file.prevTotal !== undefined && (!record(file.prevTotal) || Object.values(file.prevTotal).some(totals =>
      !record(totals) || Object.values(totals).some(number => number !== undefined && !count(number))))) return false;
  }
  if (value.ledgerTotals !== undefined && (!record(value.ledgerTotals) || Object.values(value.ledgerTotals).some(row => !record(row) || !count(row.tokens)))) return false;
  return value.seenHashes === undefined || (Array.isArray(value.seenHashes) && value.seenHashes.every(key => typeof key === 'string'));
}
function validateJournal(value: unknown): asserts value is CodexLedgerJournal {
  if (!record(value) || value.version !== 1 || !Array.isArray(value.buckets) ||
      !value.buckets.every(validBucket) || !validCursor(value.codexCursor)) {
    throw new Error('Invalid pending Codex ledger transaction; collection paused');
  }
  if (new Set(value.buckets.map(bucketKey)).size !== value.buckets.length) {
    throw new Error('Duplicate pending Codex bucket; collection paused');
  }
  validateState(value.provenance);
}
async function readJson(path: string): Promise<unknown | null> {
  let raw: string;
  try { raw = await readFile(path, 'utf8'); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw error;
  }
  return JSON.parse(raw) as unknown;
}
async function atomicJson(path: string, value: unknown): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const file = await open(temporary, 'wx', 0o600);
  try { await file.writeFile(JSON.stringify(value) + '\n'); await file.sync(); }
  finally { await file.close(); }
  try { await rename(temporary, path); }
  finally { await unlink(temporary).catch(() => undefined); }
}

/** Called under the Codex lock. Queue snapshots are replacements, never deltas. */
async function applyJournal(dataDir: string, journal: CodexLedgerJournal): Promise<void> {
  await appendBuckets(dataDir, journal.buckets, { ensureLineBoundary: true });
  const persisted = await readJson(join(dataDir, 'cursors.json'));
  if (persisted !== null && !record(persisted)) throw new Error('Invalid cursors during Codex recovery; collection paused');
  // The runtime's single-owner runner syncs sources sequentially. Merge its
  // latest persisted non-Codex slots; recovery must not roll those slots back.
  await atomicJson(join(dataDir, 'cursors.json'), { ...(persisted ?? {}), codex: journal.codexCursor });
  resetCursorsCache(dataDir);
  await atomicJson(join(dataDir, STATE_FILE), journal.provenance);
  await unlink(join(dataDir, JOURNAL_FILE));
}
async function recoverUnlocked(dataDir: string): Promise<void> {
  const pending = await readJson(join(dataDir, JOURNAL_FILE));
  if (pending === null) return;
  validateJournal(pending);
  await applyJournal(dataDir, pending);
}
export async function withCodexLedgerLock<T>(dataDir: string, operation: () => Promise<T>): Promise<T> {
  await mkdir(dataDir, { recursive: true });
  const release = await lock(join(dataDir, STATE_FILE), {
    // Ledger SQLite reads can block this process synchronously for 60 seconds.
    // Keep ownership while its heartbeat cannot run.
    realpath: false, stale: 120_000, update: 5_000,
    retries: { retries: 100, factor: 1.2, minTimeout: 10, maxTimeout: 200 },
  });
  try { await recoverUnlocked(dataDir); return await operation(); }
  finally { await release(); }
}

/** Queue/cursor readers call this before exposing a partially committed Codex round. */
export async function recoverCodexLedgerTransaction(dataDir: string): Promise<void> {
  if (!existsSync(join(dataDir, JOURNAL_FILE))) return;
  await withCodexLedgerLock(dataDir, async () => undefined);
}

/** Detect a commit that completed while a multi-month queue read was in progress. */
export async function codexLedgerReadStamp(dataDir: string): Promise<string> {
  await recoverCodexLedgerTransaction(dataDir);
  try {
    const info = statSync(join(dataDir, STATE_FILE), { bigint: true });
    return `${info.ino}:${info.mtimeNs}:${info.size}`;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return 'absent';
    throw error;
  }
}

export async function loadCodexLedgerState(
  dataDir: string,
  existing: QueueBucket[] | (() => Promise<QueueBucket[]>),
  cursors: CursorsFile,
): Promise<CodexLedgerState> {
  const stored = await readJson(join(dataDir, STATE_FILE));
  if (stored !== null) { validateState(stored); return stored; }
  const rows = typeof existing === 'function' ? await existing() : existing;
  const baselines = Object.fromEntries(rows
    .filter(row => row.source === 'codex' && row.collector === 'codex-ledger')
    .map(row => [bucketKey(row), { ...row, ledger_unverified_tokens: row.total_tokens }]));
  const threads: CodexLedgerState['threads'] = Object.create(null) as CodexLedgerState['threads'];
  for (const [id, row] of Object.entries(cursors.codex?.ledgerTotals ?? {})) {
    if (!count(row.tokens)) throw new Error('Invalid legacy Codex watermark; collection paused');
    threads[id] = { watermark: row.tokens, generation: 0, ledgerTotal: null, observedDeltas: {}, gaps: [] };
  }
  return {
    version: 1, baselines, threads, publishBaselines: true,
    seedLegacyThreads: Object.values(baselines).some(row => row.total_tokens > 0),
  };
}

/** Must run inside withCodexLedgerLock; a crash at any subsequent stage is replayable. */
export async function commitCodexLedgerTransaction(
  dataDir: string,
  buckets: QueueBucket[],
  codexCursor: NonNullable<CursorsFile['codex']>,
  provenance: CodexLedgerState,
): Promise<void> {
  const journal: CodexLedgerJournal = { version: 1, buckets, codexCursor, provenance };
  validateJournal(journal);
  await atomicJson(join(dataDir, JOURNAL_FILE), journal);
  await applyJournal(dataDir, journal);
}

export function ledgerSnapshots(state: CodexLedgerState): QueueBucket[] {
  const rows = new Map(Object.entries(state.baselines).map(([key, row]) => [key, { ...row }]));
  for (const thread of Object.values(state.threads)) {
    for (const gap of thread.gaps) {
      const row = rows.get(gap.bucket)!;
      const remaining = gap.to - gap.from;
      row.input_tokens += remaining;
      row.total_tokens += remaining;
      row.conversation_count += gap.conversations;
      if (gap.generation !== thread.generation) row.ledger_unverified_tokens = (row.ledger_unverified_tokens ?? 0) + remaining;
    }
  }
  return [...rows.values()];
}

/** Reconcile one counter generation. A cumulative interval is evidence; a delta alone is not. */
export function reconcileTrackedCodexThread(
  state: CodexLedgerState,
  id: string,
  tokens: number | null,
  intervals: readonly CodexPreciseInterval[],
  lifetime: number | null,
  observed: number,
  alreadyCounted: boolean,
  gapBucket: QueueBucket | null,
  ambiguous: boolean,
  observedDeltas: Readonly<Record<string, number>> = {},
): boolean {
  const known = Object.prototype.hasOwnProperty.call(state.threads, id) ? state.threads[id] : undefined;
  if (!known && (state.seedLegacyThreads || alreadyCounted)) {
    state.threads[id] = {
      watermark: Math.max(tokens ?? 0, lifetime ?? 0, observed),
      generation: 0, ledgerTotal: tokens, observedDeltas: Object.fromEntries(Object.entries(observedDeltas).map(([key, value]) => [`0:${key}`, value])), gaps: [],
    };
    return false;
  }
  const thread = known ?? (state.threads[id] = { watermark: 0, generation: 0, ledgerTotal: null, observedDeltas: {}, gaps: [] });
  const reset = tokens !== null && thread.ledgerTotal !== null && tokens < thread.ledgerTotal;
  if (tokens !== null) thread.ledgerTotal = tokens;
  if (reset || ambiguous) {
    thread.generation += 1;
    thread.watermark = reset ? 0 : thread.watermark;
  }
  // Counter resets cannot identify whether an old file tail belongs to the old
  // or new generation. Preserve all old gaps rather than subtract by guesswork.
  if (!reset && !ambiguous) {
    for (const interval of intervals) {
      thread.gaps = thread.gaps.flatMap(gap => {
        if (gap.generation !== thread.generation || interval.to <= gap.from || interval.from >= gap.to) return [gap];
        const pieces: GapRange[] = [];
        if (gap.from < interval.from) pieces.push({ ...gap, to: interval.from });
        if (interval.to < gap.to) pieces.push({ ...gap, from: interval.to, conversations: pieces.length ? 0 : gap.conversations });
        return pieces;
      });
    }
  }
  // File cursors can be cleared for a range expansion. Remember delta-only
  // positions independently, otherwise a full rescan advances the watermark twice.
  let newlyObserved = 0;
  for (const [event, value] of Object.entries(observedDeltas)) {
    const key = `${thread.generation}:${event}`;
    const previous = thread.observedDeltas[key] ?? 0;
    newlyObserved += Math.max(0, value - previous);
    thread.observedDeltas[key] = Math.max(previous, value);
  }
  const explained = lifetime === null
    ? thread.watermark + newlyObserved
    : Math.max(thread.watermark, lifetime);
  const end = tokens ?? 0;
  const start = Math.min(end, explained);
  let emitted = false;
  if (end > start && gapBucket) {
    const key = bucketKey(gapBucket);
    state.baselines[key] ??= {
      ...gapBucket, input_tokens: 0, output_tokens: 0, cached_input_tokens: 0,
      cache_creation_input_tokens: 0, reasoning_output_tokens: 0,
      total_tokens: 0, conversation_count: 0, ledger_unverified_tokens: 0,
    };
    thread.gaps.push({
      from: start, to: end, bucket: key, generation: thread.generation,
      conversations: thread.watermark === 0 ? 1 : 0,
    });
    emitted = true;
  }
  thread.watermark = Math.max(end, explained);
  return emitted;
}
