import type { IngestEventPayload } from './events.js';
import { normalizeApiUrl } from './state.js';

export interface RemoteEvent extends Omit<
  IngestEventPayload,
  'reported_cost_usd'
> {
  reported_cost_usd?: number | null;
}

function conversationCount(value: number | undefined): number {
  return Math.max(1, value ?? 1);
}

/** Stable identity and transmitted values matter; the API may shift occurred_at by +8h. */
export function eventsMatch(
  sent: IngestEventPayload,
  remote: RemoteEvent,
): boolean {
  if (sent.event_id !== remote.event_id || !remote.usage) return false;
  if (
    sent.integration !== remote.integration ||
    sent.collector !== remote.collector ||
    sent.model !== remote.model
  )
    return false;
  if (
    conversationCount(sent.conversations_count) !==
    conversationCount(remote.conversations_count)
  )
    return false;
  for (const field of [
    'input_tokens',
    'cached_input_tokens',
    'cache_creation_input_tokens',
    'output_tokens',
    'reasoning_output_tokens',
  ] as const) {
    if (
      !Number.isFinite(remote.usage[field]) ||
      sent.usage[field] !== remote.usage[field]
    )
      return false;
  }
  const cost = sent.reported_cost_usd ?? null;
  const remoteCost = remote.reported_cost_usd ?? null;
  return cost === null
    ? remoteCost === null
    : remoteCost !== null &&
        Number.isFinite(remoteCost) &&
        Math.abs(cost - remoteCost) < 1e-9;
}

export interface RemoteEventsResult {
  events: RemoteEvent[];
  ingestMinOccurredAt: string | null;
  complete: boolean;
}

/**
 * Avoid the server cursor path: read calendar-sized ranges and bisect full pages.
 * A capped or unsplittable range is explicitly incomplete, never proof of absence.
 */
export async function readRemoteEvents(
  apiUrl: string,
  token: string,
  deviceId: string,
  from: string,
  to: string,
): Promise<RemoteEventsResult> {
  const start = Date.parse(from);
  const end = Date.parse(to);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start)
    throw new Error('Invalid event readback range');
  const ranges: Array<[number, number]> = [];
  for (let t = start; t < end; t += 86_400_000)
    ranges.push([t, Math.min(t + 86_400_000, end)]);
  const byId = new Map<string, RemoteEvent>();
  let ingestMinOccurredAt: string | null = null;
  let requests = 0;
  let complete = true;
  while (ranges.length > 0) {
    if (requests++ >= 256) {
      complete = false;
      break;
    }
    const [low, high] = ranges.shift()!;
    const url = new URL(
      `${normalizeApiUrl(apiUrl)}/functions/tud-usage-device-events`,
    );
    url.searchParams.set('deviceId', deviceId);
    url.searchParams.set('from', new Date(low).toISOString());
    url.searchParams.set('to', new Date(high).toISOString());
    url.searchParams.set('limit', '500');
    const res = await fetch(url, {
      signal: AbortSignal.timeout(30_000),
      headers: {
        'x-user-id': token,
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
    });
    if (!res.ok) throw new Error(`云端记录读取失败: HTTP ${res.status}`);
    const body = (await res.json()) as {
      success?: boolean;
      data?: {
        events?: RemoteEvent[];
        next_cursor?: string | null;
        ingest_min_occurred_at?: string | null;
      };
    };
    if (body.success !== true || !Array.isArray(body.data?.events))
      throw new Error('云端记录响应无效');
    ingestMinOccurredAt =
      body.data.ingest_min_occurred_at ?? ingestMinOccurredAt;
    for (const event of body.data.events)
      if (typeof event.event_id === 'string') byId.set(event.event_id, event);
    if (body.data.next_cursor || body.data.events.length >= 500) {
      if (high - low <= 1) complete = false;
      else {
        const middle = Math.floor((low + high) / 2);
        ranges.unshift([low, middle], [middle, high]);
      }
    }
  }
  return { events: [...byId.values()], ingestMinOccurredAt, complete };
}

export async function confirmPostedEvents(
  target: { apiUrl: string; token: string; deviceId: string },
  sent: IngestEventPayload[],
  counts: { accepted: number; duplicate: number },
): Promise<Set<string>> {
  if (sent.length === 0) return new Set();
  if (
    Number.isSafeInteger(counts.accepted) &&
    counts.accepted === sent.length &&
    counts.duplicate === 0
  ) {
    return new Set(sent.map((event) => event.event_id));
  }
  // Include neighboring hours for legacy timestamp presentation. Never derive an ID from returned time.
  const times = sent.map((event) => Date.parse(event.occurred_at));
  const remote = await readRemoteEvents(
    target.apiUrl,
    target.token,
    target.deviceId,
    new Date(Math.min(...times) - 43_200_000).toISOString(),
    new Date(Math.max(...times) + 43_200_000 + 1).toISOString(),
  );
  const byId = new Map(remote.events.map((event) => [event.event_id, event]));
  return new Set(
    sent
      .filter((event) => {
        const stored = byId.get(event.event_id);
        return stored != null && eventsMatch(event, stored);
      })
      .map((event) => event.event_id),
  );
}
