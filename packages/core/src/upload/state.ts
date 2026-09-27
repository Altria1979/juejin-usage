import { createHash, randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { lock } from 'proper-lockfile';

import type { IngestBucket } from '../types.js';
import type { IngestEventPayload } from './events.js';
import { ingestBucketKey } from '../queue/keys.js';

export interface BackfillItem {
  key: string;
  attempts: number;
  nextRetryAt: string | null;
  /** Exact local snapshot persisted before its normalized event is sent. */
  snapshot?: IngestBucket;
  /** Canonical wire payload of this snapshot, including normalized conversations/cost. */
  event?: IngestEventPayload;
}

export interface BackfillState {
  items: BackfillItem[];
  /** Product-window floor used the last time we enqueued a full delta. */
  enqueuedSince?: string | null;
}

export interface UploadSlotState {
  buckets: Record<string, string>;
  backfill?: BackfillState;
  /** Set when live ingest fails; next upload forces a full queue scan. */
  needsFullScan?: boolean;
  /** Enqueued repair version; does not mean remote confirmation succeeded. */
  repairVersion?: number;
  lastAttemptAt?: string | null;
  lastConfirmedAt?: string | null;
  lastError?: string | null;
}

export interface UploadStatus {
  state: 'disabled' | 'confirmed' | 'pending' | 'failed';
  pendingBuckets: number;
  lastAttemptAt: string | null;
  lastConfirmedAt: string | null;
  message?: string;
}

/** v2: per-(apiUrl, deviceId) slots so remotes and machines do not share pointers. */
export interface UploadStateFileV2 {
  version: 2;
  remotes: Record<string, { devices: Record<string, UploadSlotState> }>;
}

/** Legacy v1 flat hash map. */
export interface UploadStateFileV1 {
  buckets: Record<string, string>;
}

export type UploadStateFile = UploadStateFileV2;

export function uploadStatePath(dataDir: string): string {
  return join(dataDir, 'upload.state.json');
}

export function normalizeApiUrl(apiUrl: string): string {
  return apiUrl.trim().replace(/\/+$/, '');
}

export function bucketHash(bucket: IngestBucket): string {
  const payload = [
    bucket.input_tokens,
    bucket.output_tokens,
    bucket.cached_input_tokens,
    bucket.cache_creation_input_tokens,
    bucket.reasoning_output_tokens,
    bucket.total_tokens,
    bucket.conversation_count,
    bucket.reported_cost_usd ?? '',
  ].join('|');
  return createHash('sha256').update(payload).digest('hex').slice(0, 16);
}

function emptyV2(): UploadStateFileV2 {
  return { version: 2, remotes: {} };
}

function cloneBackfill(backfill: BackfillState | undefined): BackfillState {
  return {
    items: (backfill?.items ?? []).map((item) => ({
      ...item,
      ...(item.snapshot ? { snapshot: { ...item.snapshot } } : {}),
      ...(item.event
        ? { event: { ...item.event, usage: { ...item.event.usage } } }
        : {}),
    })),
    enqueuedSince: backfill?.enqueuedSince ?? null,
  };
}

function cloneSlot(slot: UploadSlotState | undefined): UploadSlotState {
  return {
    ...slot,
    buckets: { ...(slot?.buckets ?? {}) },
    backfill: cloneBackfill(slot?.backfill),
    ...(slot?.needsFullScan ? { needsFullScan: true } : {}),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function nonnegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}
function optionalDate(value: unknown): boolean {
  return (
    value === undefined ||
    value === null ||
    (typeof value === 'string' && Number.isFinite(Date.parse(value)))
  );
}
const TOKEN_FIELDS = [
  'input_tokens',
  'output_tokens',
  'cached_input_tokens',
  'cache_creation_input_tokens',
  'reasoning_output_tokens',
] as const;
function validCost(value: unknown): boolean {
  return (
    value === undefined ||
    (typeof value === 'number' && Number.isFinite(value) && value >= 0)
  );
}
function validSnapshot(value: unknown): value is IngestBucket {
  return (
    isRecord(value) &&
    ['source', 'model', 'hour_start'].every(
      (key) => typeof value[key] === 'string',
    ) &&
    optionalDate(value.hour_start) &&
    (value.collector === undefined || typeof value.collector === 'string') &&
    [...TOKEN_FIELDS, 'total_tokens', 'conversation_count'].every((key) =>
      nonnegativeInteger(value[key]),
    ) &&
    validCost(value.reported_cost_usd)
  );
}
function validEvent(value: unknown): boolean {
  return (
    isRecord(value) &&
    ['event_id', 'occurred_at', 'integration', 'collector', 'model'].every(
      (key) => typeof value[key] === 'string',
    ) &&
    optionalDate(value.occurred_at) &&
    isRecord(value.usage) &&
    TOKEN_FIELDS.every((key) =>
      nonnegativeInteger((value.usage as Record<string, unknown>)[key]),
    ) &&
    (value.conversations_count === undefined ||
      nonnegativeInteger(value.conversations_count)) &&
    (value.conversation_ref === undefined ||
      typeof value.conversation_ref === 'string') &&
    validCost(value.reported_cost_usd)
  );
}
function validSlot(value: unknown): boolean {
  if (
    !isRecord(value) ||
    !isRecord(value.buckets) ||
    !Object.values(value.buckets).every((hash) => typeof hash === 'string')
  )
    return false;
  if (
    value.needsFullScan !== undefined &&
    typeof value.needsFullScan !== 'boolean'
  )
    return false;
  if (
    value.repairVersion !== undefined &&
    !nonnegativeInteger(value.repairVersion)
  )
    return false;
  if (
    !optionalDate(value.lastAttemptAt) ||
    !optionalDate(value.lastConfirmedAt) ||
    (value.lastError != null && typeof value.lastError !== 'string')
  )
    return false;
  if (value.backfill === undefined) return true;
  if (
    !isRecord(value.backfill) ||
    !Array.isArray(value.backfill.items) ||
    !optionalDate(value.backfill.enqueuedSince)
  )
    return false;
  const seen = new Set<string>();
  for (const item of value.backfill.items) {
    if (
      !isRecord(item) ||
      typeof item.key !== 'string' ||
      seen.has(item.key) ||
      !nonnegativeInteger(item.attempts) ||
      !optionalDate(item.nextRetryAt)
    )
      return false;
    seen.add(item.key);
    if (
      item.snapshot !== undefined &&
      (!validSnapshot(item.snapshot) ||
        ingestBucketKey(item.snapshot) !== item.key)
    )
      return false;
    if (item.event !== undefined && (!item.snapshot || !validEvent(item.event)))
      return false;
  }
  return true;
}
function isV2(parsed: unknown): parsed is UploadStateFileV2 {
  return (
    isRecord(parsed) &&
    parsed.version === 2 &&
    isRecord(parsed.remotes) &&
    Object.values(parsed.remotes).every(
      (remote) =>
        isRecord(remote) &&
        isRecord(remote.devices) &&
        Object.values(remote.devices).every(validSlot),
    )
  );
}

/** Load full file; migrates v1 `{ buckets }` into an empty v2 shell (caller picks slot). */
export async function loadUploadStateFile(
  dataDir: string,
): Promise<UploadStateFileV2> {
  const path = uploadStatePath(dataDir);
  if (!existsSync(path)) return emptyV2();
  try {
    const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
    if (isV2(parsed)) {
      return {
        version: 2,
        remotes: parsed.remotes ?? {},
      };
    }
    // Only a recognized v1 file may migrate. A malformed v2 must not erase pending work.
    if (
      isRecord(parsed) &&
      !('version' in parsed) &&
      isRecord(parsed.buckets) &&
      Object.values(parsed.buckets).every((hash) => typeof hash === 'string')
    ) {
      return emptyV2();
    }
    throw new Error('Unrecognized upload state format');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return emptyV2();
    throw new Error('上传状态读取失败，已暂停上传以保留待确认记录', {
      cause: error,
    });
  }
}

export async function saveUploadStateFile(
  dataDir: string,
  state: UploadStateFileV2,
): Promise<void> {
  await mkdir(dataDir, { recursive: true });
  const destination = uploadStatePath(dataDir);
  const temporary = `${destination}.${process.pid}.${randomUUID()}.tmp`;
  try {
    const handle = await open(temporary, 'wx', 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(state, null, 2)}\n`, 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    for (let attempt = 0; ; attempt++) {
      try {
        await rename(temporary, destination);
        break;
      } catch (error) {
        if (
          attempt >= 8 ||
          !['EPERM', 'EACCES', 'EBUSY'].includes(
            (error as NodeJS.ErrnoException).code ?? '',
          )
        )
          throw error;
        await new Promise((resolve) => setTimeout(resolve, 20 * 2 ** attempt));
      }
    }
  } finally {
    await unlink(temporary).catch(() => undefined);
  }
}

const uploadLocks = new Map<string, Promise<void>>();

/** Serialize live, background and calibration writes, including other processes. */
export async function withUploadLock<T>(
  dataDir: string,
  fn: () => Promise<T>,
): Promise<T> {
  const previous = uploadLocks.get(dataDir) ?? Promise.resolve();
  let releaseGate!: () => void;
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });
  const tail = previous.then(
    () => gate,
    () => gate,
  );
  uploadLocks.set(dataDir, tail);
  await previous.catch(() => undefined);
  let releaseFile: (() => Promise<void>) | undefined;
  try {
    await mkdir(dataDir, { recursive: true });
    releaseFile = await lock(uploadStatePath(dataDir), {
      realpath: false,
      stale: 60_000,
      retries: { retries: 8, minTimeout: 250, maxTimeout: 2_000 },
    });
    return await fn();
  } finally {
    try {
      await releaseFile?.();
    } finally {
      releaseGate();
      if (uploadLocks.get(dataDir) === tail) uploadLocks.delete(dataDir);
    }
  }
}

export function getUploadSlot(
  state: UploadStateFileV2,
  apiUrl: string,
  deviceId: string,
): UploadSlotState {
  const url = normalizeApiUrl(apiUrl);
  const remote = state.remotes[url];
  return cloneSlot(remote?.devices?.[deviceId]);
}

export function setUploadSlot(
  state: UploadStateFileV2,
  apiUrl: string,
  deviceId: string,
  slot: UploadSlotState,
): UploadStateFileV2 {
  const url = normalizeApiUrl(apiUrl);
  const remotes = { ...state.remotes };
  const prevRemote = remotes[url] ?? { devices: {} };
  remotes[url] = {
    devices: {
      ...prevRemote.devices,
      [deviceId]: cloneSlot(slot),
    },
  };
  return { version: 2, remotes };
}

export function clearUploadSlot(
  state: UploadStateFileV2,
  apiUrl: string,
  deviceId: string,
): UploadStateFileV2 {
  return setUploadSlot(state, apiUrl, deviceId, {
    buckets: {},
    backfill: { items: [] },
  });
}

/** @deprecated Prefer getUploadSlot — kept for callers that still expect flat buckets. */
export async function loadUploadState(
  dataDir: string,
): Promise<UploadSlotState> {
  const file = await loadUploadStateFile(dataDir);
  const urls = Object.keys(file.remotes);
  if (urls.length !== 1) return { buckets: {}, backfill: { items: [] } };
  const devices = file.remotes[urls[0]!]?.devices ?? {};
  const ids = Object.keys(devices);
  if (ids.length !== 1) return { buckets: {}, backfill: { items: [] } };
  return getUploadSlot(file, urls[0]!, ids[0]!);
}

/** @deprecated Prefer saveUploadStateFile + setUploadSlot. */
export async function saveUploadState(
  dataDir: string,
  slot: UploadSlotState,
): Promise<void> {
  await saveUploadStateFile(dataDir, {
    version: 2,
    remotes: {
      _legacy: { devices: { _legacy: cloneSlot(slot) } },
    },
  });
}

export function diffUploadBuckets(
  buckets: IngestBucket[],
  state: UploadSlotState,
): { delta: IngestBucket[]; nextState: UploadSlotState } {
  const nextState: UploadSlotState = cloneSlot(state);
  const delta: IngestBucket[] = [];

  for (const bucket of buckets) {
    const key = ingestBucketKey(bucket);
    const hash = bucketHash(bucket);
    if (nextState.buckets[key] === hash) continue;
    delta.push(bucket);
    nextState.buckets[key] = hash;
  }

  return { delta, nextState };
}

export function findUploadDelta(
  buckets: IngestBucket[],
  state: UploadSlotState,
): IngestBucket[] {
  const delta: IngestBucket[] = [];
  for (const bucket of buckets) {
    const key = ingestBucketKey(bucket);
    if (state.buckets[key] === bucketHash(bucket)) continue;
    delta.push(bucket);
  }
  return delta;
}

export function commitBucketHashes(
  state: UploadSlotState,
  buckets: IngestBucket[],
): UploadSlotState {
  const next = cloneSlot(state);
  for (const bucket of buckets) {
    next.buckets[ingestBucketKey(bucket)] = bucketHash(bucket);
  }
  return next;
}
