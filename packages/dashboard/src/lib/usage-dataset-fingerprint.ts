import type { UsageDataset } from './api';

/**
 * Compare every input used by the projected dashboard. Equal grand totals do
 * not imply equal source fees, ledger attribution, or completeness evidence.
 * Keep the serialized content rather than a lossy sum/hash so a quality-only
 * update cannot reuse stale React data.
 */
export function fingerprintUsageDataset(
  dataset: UsageDataset,
  rangeDays: number,
): string {
  return JSON.stringify([
    rangeDays,
    dataset.summary,
    dataset.dailyRows,
    dataset.hourlyRows,
    dataset.modelRows,
    dataset.projectRows,
    dataset.syncStatus?.statsSince ?? '',
    dataset.syncStatus?.lastSyncAt ?? '',
  ]);
}
