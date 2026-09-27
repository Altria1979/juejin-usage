import { randomUUID } from 'node:crypto';

import { aggregateForIngest } from '../aggregate.js';
import { resolveLinkedUserId, setLastUploadAt } from '../config.js';
import { appendJsonLog } from '../debug-log.js';
import { uploadLogPath } from '../paths.js';
import { dedupeBuckets, loadBucketsForRange } from '../queue/index.js';
import { ingestBucketKey, monthFromHourStart } from '../queue/keys.js';
import type {
  IngestBucket,
  QueueBucket,
  SyncStatus,
  TudConfig,
} from '../types.js';
import {
  BACKFILL_GAP_MS,
  applyBackfillFailure,
  applyIngestHold,
  earliestRetryMs,
  enqueueBackfillKeys,
  productWindowSinceIso,
  pruneBackfillItems,
  selectDrainBatch,
} from './backfill.js';
import { bucketToIngestEvent } from './events.js';
import { confirmPostedEvents } from './confirmation.js';
import {
  commitBucketHashes,
  findUploadDelta,
  getUploadSlot,
  loadUploadStateFile,
  normalizeApiUrl,
  saveUploadStateFile,
  setUploadSlot,
  bucketHash,
  withUploadLock,
  type UploadStatus,
  type UploadSlotState,
  type UploadStateFileV2,
} from './state.js';
import { maxIso } from './window.js';

const CLIENT_VERSION = 'jusage-1.0.0';

export interface UploadResult {
  uploaded: number;
  accepted: number;
  duplicate: number;
  skipped: number;
  /** HTTP POST count (batches). */
  requestCount: number;
  backfillEnqueued?: number;
}

export interface UploadOptions {
  /** Skip juejin.enabled check (for `jusage upload`). */
  force?: boolean;
  /**
   * Buckets appended in the latest sync. When the current slot already
   * has entries, only these keys are diffed instead of scanning all queue files.
   */
  recentBuckets?: QueueBucket[];
  /** Always scan full queue history (default for `jusage upload`). */
  fullScan?: boolean;
  /** Clear the current (apiUrl, deviceId) slot and re-diff. */
  reconcile?: boolean;
  /** Do not start the background drain loop (tests / caller will drain). */
  skipDrain?: boolean;
}

export interface RemoteUploadWatermark {
  ingestMinOccurredAt: string | null;
  dataThrough: string | null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function postBatch(
  apiUrl: string,
  token: string,
  deviceId: string,
  events: NonNullable<ReturnType<typeof bucketToIngestEvent>>[],
): Promise<{ accepted: number; duplicate: number; reportId: string }> {
  const payload = {
    schema_version: 1,
    client_version: CLIENT_VERSION,
    device_id: deviceId,
    sent_at: new Date().toISOString(),
    events,
  };

  for (let attempt = 0; attempt < 4; attempt++) {
    const res = await fetch(`${apiUrl}/v1/model-usage/reports`, {
      method: 'POST',
      signal: AbortSignal.timeout(30_000),
      headers: {
        'x-user-id': token,
        Authorization: `Bearer ${token}`,
        'Idempotency-Key': randomUUID(),
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    const text = await res.text();
    if (res.status === 429 && attempt < 3) {
      const retryAfter = Number(res.headers.get('Retry-After'));
      const waitMs =
        Number.isFinite(retryAfter) && retryAfter > 0
          ? retryAfter * 1000
          : 2000;
      await sleep(waitMs);
      continue;
    }
    if (!res.ok) {
      throw new Error(`上报失败 ${res.status}: ${text.slice(0, 200)}`);
    }

    let body: {
      success?: boolean;
      message?: string;
      data?: {
        accepted_count?: number;
        duplicate_count?: number;
        report_id?: string;
      };
    };
    try {
      body = JSON.parse(text) as typeof body;
    } catch {
      throw new Error(`上报响应解析失败: ${text.slice(0, 200)}`);
    }

    if (body.success !== true || !body.data) {
      throw new Error(body.message ?? `上报失败 ${res.status}`);
    }

    return {
      accepted: body.data.accepted_count ?? Number.NaN,
      duplicate: body.data.duplicate_count ?? Number.NaN,
      reportId: body.data.report_id ?? '',
    };
  }

  throw new Error('上报失败 429: rate limit exceeded');
}

/**
 * Fetch per-device upload watermark from Server sync-status.
 * On failure, returns nulls: live upload still uses local statsSince;
 * backfill holds until ingestMin is known.
 */
export async function fetchRemoteUploadWatermark(
  apiUrl: string,
  token: string,
  deviceId: string,
): Promise<RemoteUploadWatermark> {
  const url = new URL(`${normalizeApiUrl(apiUrl)}/functions/tud-sync-status`);
  url.searchParams.set('deviceId', deviceId);

  try {
    const res = await fetch(url, {
      signal: AbortSignal.timeout(30_000),
      headers: {
        'x-user-id': token,
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
    });
    if (!res.ok) {
      return { ingestMinOccurredAt: null, dataThrough: null };
    }
    const body = (await res.json()) as {
      success?: boolean;
      data?: SyncStatus & { ingestMinOccurredAt?: string | null };
    };
    if (body.success !== true || !body.data) {
      return { ingestMinOccurredAt: null, dataThrough: null };
    }
    return {
      ingestMinOccurredAt: body.data.ingestMinOccurredAt ?? null,
      dataThrough: body.data.lastUploadAt ?? null,
    };
  } catch {
    return { ingestMinOccurredAt: null, dataThrough: null };
  }
}

function uploadTarget(
  config: TudConfig,
  force?: boolean,
): { apiUrl: string; token: string; deviceId: string } | null {
  if (!force && !config.juejin.enabled) return null;
  const apiUrl = normalizeApiUrl(config.juejin.apiUrl ?? '');
  const token = config.juejin.token?.trim();
  const deviceId = config.deviceId?.trim();
  if (!apiUrl || !deviceId || !token) return null;
  if (!resolveLinkedUserId(deviceId, token)) return null;
  return { apiUrl, token, deviceId };
}

function loadSinceIso(config: TudConfig, nowMs = Date.now()): string {
  const productSince = productWindowSinceIso(nowMs);
  return maxIso(config.statsSince, productSince) ?? productSince;
}

/**
 * Queue rows are per project, but one ingest event covers every project in
 * its (source, collector, model, half-hour). Re-read the touched keys from the
 * queue so an incremental upload sends whole-bucket totals instead of
 * overwriting the server copy with only the projects this sync rewrote.
 */
async function loadTouchedIngestBuckets(
  dataDir: string,
  recentBuckets: QueueBucket[],
): Promise<IngestBucket[]> {
  const keys = new Set(recentBuckets.map((row) => ingestBucketKey(row)));
  const months = [
    ...new Set(recentBuckets.map((row) => monthFromHourStart(row.hour_start))),
  ].sort();
  const queued = await loadBucketsForRange(
    dataDir,
    new Date(0).toISOString(),
    months,
  );
  // recentBuckets were appended last, so they win any key they share.
  const rows = dedupeBuckets([...queued, ...recentBuckets]).filter((row) =>
    keys.has(ingestBucketKey(row)),
  );
  return aggregateForIngest(rows);
}

async function persistSlot(
  dataDir: string,
  file: UploadStateFileV2,
  apiUrl: string,
  deviceId: string,
  slot: UploadSlotState,
): Promise<UploadStateFileV2> {
  const next = setUploadSlot(file, apiUrl, deviceId, slot);
  await saveUploadStateFile(dataDir, next);
  return next;
}

/** Upgrade repair is enqueued even when a legacy hash already equals the full bucket. */
export const UPLOAD_REPAIR_VERSION = 1;

export function enqueueUploadSnapshots(
  slot: UploadSlotState,
  buckets: IngestBucket[],
  deviceId: string,
  since?: string,
): UploadSlotState {
  const next = enqueueBackfillKeys(
    slot,
    buckets.map(ingestBucketKey),
    since,
  ).slot;
  const byKey = new Map(
    buckets.map((bucket) => [ingestBucketKey(bucket), bucket]),
  );
  next.backfill!.items = next.backfill!.items.map((item) => {
    const snapshot = byKey.get(item.key);
    if (!snapshot) return item;
    const changed =
      item.snapshot && bucketHash(item.snapshot) !== bucketHash(snapshot);
    return {
      ...item,
      snapshot: { ...snapshot },
      event: bucketToIngestEvent(snapshot, deviceId) ?? undefined,
      ...(changed ? { attempts: 0, nextRetryAt: null } : {}),
    };
  });
  return next;
}

/** Caller holds withUploadLock. Confirm only the sent value, preserving concurrently collected data. */
export async function settleUploadSnapshots(
  dataDir: string,
  config: TudConfig,
  snapshots: IngestBucket[],
  confirmed: Set<string>,
  error?: string,
  nowMs = Date.now(),
): Promise<number> {
  const apiUrl = normalizeApiUrl(config.juejin.apiUrl!);
  const deviceId = config.deviceId;
  const file = await loadUploadStateFile(dataDir);
  let slot = getUploadSlot(file, apiUrl, deviceId);
  const current = new Map<string, IngestBucket>();
  const months = [
    ...new Set(
      snapshots.map((bucket) => monthFromHourStart(bucket.hour_start)),
    ),
  ];
  for (const bucket of aggregateForIngest(
    await loadBucketsForRange(dataDir, new Date(0).toISOString(), months),
  )) {
    current.set(ingestBucketKey(bucket), bucket);
  }
  const confirmedBuckets = snapshots.filter((bucket) => {
    const event = bucketToIngestEvent(bucket, deviceId);
    return event && confirmed.has(event.event_id);
  });
  slot = commitBucketHashes(slot, confirmedBuckets);
  const sent = new Map(
    snapshots.map((bucket) => [ingestBucketKey(bucket), bucket]),
  );
  const confirmedKeys = new Set(confirmedBuckets.map(ingestBucketKey));
  const items = (slot.backfill?.items ?? []).flatMap((item) => {
    const snapshot = sent.get(item.key);
    if (!snapshot) return [item];
    const latest = current.get(item.key);
    if (latest && bucketHash(latest) !== bucketHash(snapshot)) {
      return [
        {
          ...item,
          snapshot: { ...latest },
          event: bucketToIngestEvent(latest, deviceId) ?? undefined,
          attempts: 0,
          nextRetryAt: null,
        },
      ];
    }
    if (item.snapshot && bucketHash(item.snapshot) !== bucketHash(snapshot))
      return [item];
    if (confirmedKeys.has(item.key)) return [];
    return applyBackfillFailure([item], nowMs);
  });
  slot = {
    ...slot,
    backfill: { ...slot.backfill, items },
    lastError:
      error ??
      (confirmedBuckets.length < snapshots.length
        ? '云端尚未确认全部记录，等待重试'
        : null),
    ...(confirmedBuckets.length
      ? { lastConfirmedAt: new Date(nowMs).toISOString() }
      : {}),
  };
  await persistSlot(dataDir, file, apiUrl, deviceId, slot);
  if (confirmedBuckets.length > 0) await setLastUploadAt(dataDir, config);
  return confirmedBuckets.length;
}

interface PendingRound extends DrainRoundResult {
  accepted: number;
  duplicate: number;
  requestCount: number;
  error?: Error;
}

/** All lanes use the same durable tasks, floor checks, and confirmation rules. Caller holds the lock. */
async function processPending(
  dataDir: string,
  config: TudConfig,
  target: NonNullable<ReturnType<typeof uploadTarget>>,
  nowMs: number,
): Promise<PendingRound> {
  const { apiUrl, token, deviceId } = target;
  let file = await loadUploadStateFile(dataDir);
  let slot = getUploadSlot(file, apiUrl, deviceId);
  const productSince = productWindowSinceIso(nowMs);
  const pruned = pruneBackfillItems(slot.backfill?.items ?? [], productSince);
  slot = { ...slot, backfill: { ...slot.backfill, items: pruned.kept } };
  const empty = {
    idle: true,
    waitMs: 0,
    posted: 0,
    held: 0,
    accepted: 0,
    duplicate: 0,
    requestCount: 0,
  };
  if (pruned.kept.length === 0) {
    slot.lastError = null;
    await persistSlot(dataDir, file, apiUrl, deviceId, slot);
    return empty;
  }
  const watermark = await fetchRemoteUploadWatermark(apiUrl, token, deviceId);
  const selected = selectDrainBatch(pruned.kept, {
    ingestMinIso: watermark.ingestMinOccurredAt,
    productSinceIso: productSince,
    nowMs,
  });
  const rows = aggregateForIngest(
    await loadBucketsForRange(dataDir, productSince),
  );
  const byKey = new Map(
    rows.map((bucket) => [ingestBucketKey(bucket), bucket]),
  );
  const snapshots = selected.send.flatMap((item) => {
    const bucket = byKey.get(item.key) ?? item.snapshot;
    return bucket ? [bucket] : [];
  });
  slot = enqueueUploadSnapshots(slot, snapshots, deviceId);
  const heldKeys = new Set(selected.hold.map((item) => item.key));
  const missingKeys = new Set(
    selected.send
      .filter((item) => !byKey.has(item.key) && !item.snapshot)
      .map((item) => item.key),
  );
  slot.backfill!.items = slot.backfill!.items.map((item) =>
    heldKeys.has(item.key) || missingKeys.has(item.key)
      ? applyIngestHold([item], nowMs)[0]!
      : item,
  );
  const prepared = new Map(
    slot.backfill!.items.map((item) => [item.key, item.event]),
  );
  const events = snapshots
    .map((bucket) => prepared.get(ingestBucketKey(bucket)))
    .filter((event) => event !== undefined);
  if (snapshots.length > 0) slot.lastAttemptAt = new Date(nowMs).toISOString();
  // This write must succeed before any remote mutation begins.
  file = await persistSlot(dataDir, file, apiUrl, deviceId, slot);
  if (events.length === 0) {
    if (snapshots.length)
      await settleUploadSnapshots(
        dataDir,
        config,
        snapshots,
        new Set(),
        '存在无法上报的记录',
        nowMs,
      );
    const retry = earliestRetryMs(slot.backfill!.items, nowMs);
    return {
      ...empty,
      idle: false,
      waitMs: Math.max(1_000, (retry ?? nowMs + 60_000) - nowMs),
      held: heldKeys.size + missingKeys.size,
    };
  }
  let accepted = 0;
  let duplicate = 0;
  let error: Error | undefined;
  let confirmed = new Set<string>();
  try {
    const result = await postBatch(apiUrl, token, deviceId, events);
    accepted = Number.isFinite(result.accepted) ? result.accepted : 0;
    duplicate = Number.isFinite(result.duplicate) ? result.duplicate : 0;
    confirmed = await confirmPostedEvents(target, events, result);
  } catch (cause) {
    error = cause instanceof Error ? cause : new Error(String(cause));
  }
  const posted = await settleUploadSnapshots(
    dataDir,
    config,
    snapshots,
    confirmed,
    error?.message,
    nowMs,
  );
  await appendJsonLog(uploadLogPath(dataDir), {
    event: error
      ? 'live_failed_enqueued_backfill'
      : posted < events.length
        ? 'backfill_retry'
        : 'batch',
    accepted,
    duplicate,
    posted,
    pending: events.length - posted,
    ...(error ? { error: error.message } : {}),
  });
  if (error) {
    file = await loadUploadStateFile(dataDir);
    slot = { ...getUploadSlot(file, apiUrl, deviceId), needsFullScan: true };
    await persistSlot(dataDir, file, apiUrl, deviceId, slot);
  }
  return {
    idle: false,
    waitMs: BACKFILL_GAP_MS,
    posted,
    held: selected.hold.length,
    accepted,
    duplicate,
    requestCount: 1,
    error,
  };
}

export async function uploadToServer(
  dataDir: string,
  config: TudConfig,
  options?: UploadOptions,
): Promise<UploadResult | null> {
  const target = uploadTarget(config, options?.force);
  if (!target) return null;
  const result = await withUploadLock(dataDir, async () => {
    const { apiUrl, deviceId } = target;
    const file = await loadUploadStateFile(dataDir);
    let slot = getUploadSlot(file, apiUrl, deviceId);
    if (options?.reconcile)
      slot = { ...slot, buckets: {}, needsFullScan: true };
    const loadSince = loadSinceIso(config);
    const watermark = await fetchRemoteUploadWatermark(
      target.apiUrl,
      target.token,
      target.deviceId,
    );
    const remoteEmpty =
      Object.keys(slot.buckets).length > 0 &&
      Boolean(watermark.ingestMinOccurredAt) &&
      !watermark.dataThrough;
    const repair =
      (slot.repairVersion ?? 0) < UPLOAD_REPAIR_VERSION || remoteEmpty;
    const expanded =
      !slot.backfill?.enqueuedSince ||
      Date.parse(loadSince) < Date.parse(slot.backfill.enqueuedSince);
    const incremental =
      !repair &&
      !expanded &&
      !options?.fullScan &&
      !slot.needsFullScan &&
      options?.recentBuckets !== undefined;
    const loaded = incremental
      ? await loadTouchedIngestBuckets(dataDir, options!.recentBuckets!)
      : aggregateForIngest(await loadBucketsForRange(dataDir, loadSince));
    const delta = repair ? loaded : findUploadDelta(loaded, slot);
    const before = slot.backfill?.items.length ?? 0;
    slot = enqueueUploadSnapshots(
      slot,
      delta,
      deviceId,
      incremental ? undefined : loadSince,
    );
    slot.repairVersion = UPLOAD_REPAIR_VERSION;
    slot.needsFullScan = false;
    await persistSlot(dataDir, file, apiUrl, deviceId, slot);
    await appendJsonLog(uploadLogPath(dataDir), {
      event: 'start',
      mode: incremental ? 'incremental' : 'full',
      repair,
      ...(options?.recentBuckets?.length === 0 && !delta.length
        ? { reason: 'no_recent_buckets' }
        : {}),
    });
    const round = await processPending(dataDir, config, target, Date.now());
    if (round.error) throw round.error;
    return {
      uploaded: round.posted,
      accepted: round.accepted,
      duplicate: round.duplicate,
      skipped: 0,
      requestCount: round.requestCount,
      backfillEnqueued: Math.max(0, slot.backfill!.items.length - before),
    };
  });
  if (
    !options?.skipDrain &&
    (await getUploadStatus(dataDir, config)).pendingBuckets > 0
  )
    kickBackfillDrain(dataDir, () => config);
  return result;
}

export interface DrainRoundResult {
  idle: boolean;
  waitMs: number;
  posted: number;
  held: number;
}

export async function drainBackfillRound(
  dataDir: string,
  config: TudConfig,
  opts?: { nowMs?: number; force?: boolean },
): Promise<DrainRoundResult> {
  const target = uploadTarget(config, opts?.force);
  if (!target) return { idle: true, waitMs: 0, posted: 0, held: 0 };
  return withUploadLock(dataDir, () =>
    processPending(dataDir, config, target, opts?.nowMs ?? Date.now()),
  );
}

export async function getUploadStatus(
  dataDir: string,
  config: TudConfig,
): Promise<UploadStatus> {
  const blank = {
    pendingBuckets: 0,
    lastAttemptAt: null,
    lastConfirmedAt: null,
  };
  if (!config.juejin.enabled) return { ...blank, state: 'disabled' };
  const target = uploadTarget(config);
  if (!target)
    return { ...blank, state: 'failed', message: '云端同步未关联或缺少凭据' };
  try {
    const slot = getUploadSlot(
      await loadUploadStateFile(dataDir),
      target.apiUrl,
      target.deviceId,
    );
    const pendingBuckets = slot.backfill?.items.length ?? 0;
    return {
      state: slot.lastError
        ? 'failed'
        : pendingBuckets > 0 || !slot.lastConfirmedAt
          ? 'pending'
          : 'confirmed',
      pendingBuckets,
      lastAttemptAt: slot.lastAttemptAt ?? null,
      lastConfirmedAt: slot.lastConfirmedAt ?? null,
      ...(slot.lastError
        ? { message: slot.lastError }
        : pendingBuckets
          ? { message: '等待云端接收、窗口开放或重试' }
          : {}),
    };
  } catch {
    return { ...blank, state: 'failed', message: '上传状态读取失败' };
  }
}

interface DrainHandle {
  dataDir: string;
  getConfig: () => TudConfig;
  running: boolean;
  loop: Promise<void> | null;
}

let drainHandle: DrainHandle | null = null;

async function runDrainLoop(handle: DrainHandle): Promise<void> {
  while (handle.running) {
    const config = handle.getConfig();
    const result = await drainBackfillRound(handle.dataDir, config);
    if (!handle.running) return;
    if (result.idle) return;
    await sleep(result.waitMs);
  }
}

/** Start / continue background backfill drain for this dataDir. */
export function kickBackfillDrain(
  dataDir: string,
  getConfig: () => TudConfig,
): void {
  if (drainHandle && drainHandle.dataDir === dataDir && drainHandle.loop) {
    return;
  }
  stopBackfillDrain();
  const handle: DrainHandle = {
    dataDir,
    getConfig,
    running: true,
    loop: null,
  };
  drainHandle = handle;
  handle.loop = runDrainLoop(handle)
    .catch((error) => {
      console.warn(
        '云端补传暂停:',
        error instanceof Error ? error.message : error,
      );
    })
    .finally(() => {
      if (drainHandle === handle) {
        handle.loop = null;
        handle.running = false;
      }
    });
}

export function stopBackfillDrain(): void {
  if (!drainHandle) return;
  drainHandle.running = false;
  drainHandle = null;
}

/** Drain until the queue is empty or only future-retry / ingest-hold items remain. */
export async function drainBackfillUntilIdle(
  dataDir: string,
  config: TudConfig,
  opts?: { maxRounds?: number; nowMs?: number; force?: boolean },
): Promise<void> {
  const maxRounds = opts?.maxRounds ?? 10_000;
  for (let i = 0; i < maxRounds; i += 1) {
    const result = await drainBackfillRound(dataDir, config, {
      nowMs: opts?.nowMs,
      force: opts?.force,
    });
    if (result.idle) return;
    if (result.posted === 0 && result.waitMs > BACKFILL_GAP_MS) return;
    if (result.waitMs > 0) await sleep(result.waitMs);
  }
}

/**
 * Upload slots that already ran a full scan in this process. The first upload
 * after start re-diffs the whole queue, which also repairs buckets an older
 * client uploaded with only some of their projects.
 */
const fullScannedSlots = new Set<string>();

export async function maybeUploadAfterSync(
  dataDir: string,
  config: TudConfig,
  recentBuckets?: QueueBucket[],
): Promise<void> {
  try {
    const target = uploadTarget(config);
    let fullScan = false;
    let slotId: string | null = null;
    if (target) {
      slotId = `${dataDir}|${target.apiUrl}|${target.deviceId}`;
      const file = await loadUploadStateFile(dataDir);
      const slot = getUploadSlot(file, target.apiUrl, target.deviceId);
      fullScan = Boolean(slot.needsFullScan) || !fullScannedSlots.has(slotId);
    }
    await uploadToServer(dataDir, config, {
      recentBuckets: fullScan ? undefined : recentBuckets,
      fullScan,
    });
    if (slotId && fullScan) fullScannedSlots.add(slotId);
  } catch (err) {
    console.warn('云端上报失败:', err instanceof Error ? err.message : err);
    kickBackfillDrain(dataDir, () => config);
  }
}
