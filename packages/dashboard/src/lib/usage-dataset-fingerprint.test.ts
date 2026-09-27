import assert from 'node:assert/strict';
import test from 'node:test';
import { emptyCostBreakdown, emptyLocalMetrics } from '@juejin-opensource/jusage-core/local-metrics';
import { fingerprintUsageDataset } from './usage-dataset-fingerprint.ts';
import { fingerprintUsageDataset as fingerprintDesktopDataset } from '../../../../apps/desktop/src/renderer/lib/usage-dataset-fingerprint.ts';
import { buildDashboardDataFromDataset, stabilizeDashboardData } from '../../../../apps/desktop/src/renderer/lib/dashboard-data.ts';
import { localDateNow } from './stats-timezone.ts';
import type { UsageDataset } from './api.ts';
import type {} from '../../../../apps/desktop/src/renderer/global.d.ts';

function fixture(): UsageDataset {
  const costBreakdown = { ...emptyCostBreakdown(), ledgerEstimatedCostUsd: 1, ledgerTokens: 100, unverifiedLedgerTokens: 100 };
  const localMetrics = { ...emptyLocalMetrics(), uncachedInputTokens: 100, cacheReadTokens: null, cacheWriteTokens: null };
  const model = { model: 'gpt-5', source: 'codex', tokens: 100, costUsd: 1, pct: 100, costBreakdown, localMetrics };
  const source = { ...model, models: [model] };
  return JSON.parse(JSON.stringify({
    summary: { totalTokens: 100, todayTokens: 100, totalCostUsd: 1, todayCostUsd: 1, statsSince: localDateNow(), bySource: [source], costBreakdown, localMetrics },
    syncStatus: null,
    dailyRows: [{ date: localDateNow(), tokens: 100, costUsd: 1, models: {}, costBreakdown, localMetrics, sources: [source], modelBreakdown: [model], projects: [{ project: 'demo', tokens: 100, costUsd: 1, models: {}, modelBreakdown: [model], costBreakdown, localMetrics }] }],
    hourlyRows: [{ date: localDateNow(), hour: 0, source: 'codex', tokens: 100, costUsd: 1, inputTokens: 100, cachedInputTokens: 0, outputTokens: 0, costBreakdown, localMetrics }],
    modelRows: [model],
    projectRows: [{ project: 'demo', tokens: 100, costUsd: 1, pct: 100, models: [model], costBreakdown, localMetrics }],
  })) as UsageDataset;
}

const mutations: Array<[string, (data: UsageDataset) => void]> = [
  ['summary attribution', (data) => { data.summary.costBreakdown = { ...emptyCostBreakdown(), detailedEstimatedCostUsd: 1 }; }],
  ['daily unverified amount', (data) => { data.dailyRows[0]!.costBreakdown = { ...data.dailyRows[0]!.costBreakdown!, unverifiedLedgerTokens: 0 }; }],
  ['cache completeness', (data) => { data.hourlyRows[0]!.localMetrics = { ...emptyLocalMetrics(), uncachedInputTokens: 100, cacheHitRate: 0 }; }],
  ['optional metadata arrival', (data) => { delete data.dailyRows[0]!.localMetrics; }],
  ['daily source attribution', (data) => { data.dailyRows[0]!.sources![0]!.costBreakdown = emptyCostBreakdown(); }],
  ['daily model fee', (data) => { data.dailyRows[0]!.modelBreakdown![0]!.costUsd = .5; }],
  ['daily project model attribution', (data) => { data.dailyRows[0]!.projects![0]!.modelBreakdown![0]!.costBreakdown = emptyCostBreakdown(); }],
  ['range model fee', (data) => { data.modelRows[0]!.costUsd = .5; }],
  ['range project attribution', (data) => { data.projectRows[0]!.costBreakdown = emptyCostBreakdown(); }],
  ['range project model completeness', (data) => { data.projectRows[0]!.models[0]!.localMetrics = emptyLocalMetrics(); }],
];

for (const [name, fingerprint] of [
  ['dashboard', fingerprintUsageDataset],
  ['desktop', fingerprintDesktopDataset],
] as const) {
  test(`${name} fingerprint notices quality and distribution changes without total or sync changes`, () => {
    for (const [label, mutate] of mutations) {
      const data = fixture();
      const before = fingerprint(data, 1);
      mutate(data);
      assert.equal(data.summary.totalTokens, 100);
      assert.equal(data.summary.totalCostUsd, 1);
      assert.notEqual(fingerprint(data, 1), before, label);
    }
  });

  test(`${name} fingerprint stays stable for an unchanged dataset and distinguishes range switches`, () => {
    const data = fixture();
    assert.equal(fingerprint(data, 1), fingerprint(structuredClone(data), 1));
    assert.notEqual(fingerprint(data, 1), fingerprint(data, 7));
  });
}

test('desktop stabilization retains unchanged references but applies quality-only updates', () => {
  const data = fixture();
  const previous = buildDashboardDataFromDataset(data, 1);
  const unchanged = buildDashboardDataFromDataset(structuredClone(data), 1);
  assert.equal(stabilizeDashboardData(previous, unchanged), previous);

  data.dailyRows[0]!.costBreakdown = { ...data.dailyRows[0]!.costBreakdown!, unverifiedLedgerTokens: 0 };
  const next = buildDashboardDataFromDataset(data, 1);
  const stable = stabilizeDashboardData(previous, next);
  assert.notEqual(stable, previous);
  assert.equal(stable.summary.costBreakdown?.unverifiedLedgerTokens, 0);
  assert.equal(stable.toolModelUsage, previous.toolModelUsage);
});
