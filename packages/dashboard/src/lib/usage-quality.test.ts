import assert from 'node:assert/strict';
import test from 'node:test';
import { buildDashboardDataFromDataset, buildFilledHourlyForDate, projectDashboardForDate } from './dashboard-data.ts';
import { filterHeatmapDaysBySources, filterProjectRowsBySources, filterTrendRowsBySources, summarizeTrendRows } from './usage-filter.ts';
import { localDateNow } from './stats-timezone.ts';
import { emptyCostBreakdown, emptyLocalMetrics } from '@juejin-opensource/jusage-core/local-metrics';
import { dailyModelKey } from '@juejin-opensource/jusage-core/daily-model-key';
import { mergeUsageQuality } from './usage-quality.ts';
import { buildSummaryFromRange } from './time-range.ts';
import { buildDashboardDataFromDataset as buildDesktopData, projectDashboardForDate as projectDesktopDate } from '../../../../apps/desktop/src/renderer/lib/dashboard-data.ts';
import { filterTrendRowsBySources as filterDesktopRows, summarizeTrendRows as summarizeDesktopRows } from '../../../../apps/desktop/src/renderer/lib/usage-filter.ts';
import type { UsageDataset } from './api.ts';
import type {} from '../../../../apps/desktop/src/renderer/global.d.ts';

const detailed = { detailedEstimatedCostUsd: 2, ledgerEstimatedCostUsd: 0, ledgerTokens: 0, unverifiedLedgerTokens: 0 };
const ledger = { detailedEstimatedCostUsd: 0, ledgerEstimatedCostUsd: 8, ledgerTokens: 200, unverifiedLedgerTokens: 200 };
const total = { ...ledger, detailedEstimatedCostUsd: 2 };
const known = { ...emptyLocalMetrics(), uncachedInputTokens: 10, cacheReadTokens: 90, requestCount: 1, knownRequestCount: 1, cacheHitRate: .9 };
const unknown = { ...emptyLocalMetrics(), uncachedInputTokens: 200, cacheReadTokens: null, cacheWriteTokens: null, requestCount: null };
function fixture(): UsageDataset {
  const date = localDateNow();
  return {
    summary: { totalTokens: 300, totalCostUsd: 10, todayTokens: 300, todayCostUsd: 10, statsSince: date, bySource: [], costBreakdown: total },
    syncStatus: null,
    dailyRows: [{ date, tokens: 300, costUsd: 10, models: { [dailyModelKey('claude', 'gpt-5')]:100, [dailyModelKey('codex', 'gpt-5')]:200 }, costBreakdown: total,
      localMetrics: {...unknown, uncachedInputTokens:210}, sources:[
        {source:'claude',tokens:100,costUsd:2,localMetrics:known,costBreakdown:detailed},
        {source:'codex',tokens:200,costUsd:8,localMetrics:unknown,costBreakdown:ledger},
      ] }],
    hourlyRows:[
      {date,hour:0,source:'claude',tokens:100,costUsd:2,inputTokens:10,outputTokens:0,cachedInputTokens:90,localMetrics:known,costBreakdown:detailed},
      {date,hour:0,source:'codex',tokens:200,costUsd:8,inputTokens:200,outputTokens:0,cachedInputTokens:0,localMetrics:unknown,costBreakdown:ledger},
    ],
    modelRows:[{model:'gpt-5',source:'claude',tokens:100,costUsd:2,pct:33,costBreakdown:detailed,localMetrics:known},
      {model:'gpt-5',source:'codex',tokens:200,costUsd:8,pct:67,costBreakdown:ledger,localMetrics:unknown}],projectRows:[],
  };
}

test('daily/hourly/date projections preserve cost attribution and incomplete cache evidence', () => {
  const data = fixture();
  const view = buildDashboardDataFromDataset(data, 1);
  assert.deepEqual(view.summary.costBreakdown, total);
  assert.equal(view.summary.localMetrics?.cacheHitRate, null);
  const hourly = summarizeTrendRows({dailyRows:[], hourlyRows:buildFilledHourlyForDate(data.hourlyRows, localDateNow()),hourly:true});
  assert.deepEqual(hourly.costBreakdown, total);
  assert.equal(hourly.localMetrics?.cacheHitRate, null);
  assert.deepEqual(projectDashboardForDate(view,localDateNow()).summary.costBreakdown,total);
});

test('source filtering uses actual costs rather than token share and keeps known cache evidence', () => {
  const data = fixture();
  const view = buildDashboardDataFromDataset(data, 1);
  const selected = filterTrendRowsBySources({dailyRows:view.rangeDailyUsage,hourlyRows:view.todayHourlyUsage,
    hourlyApiRows:data.hourlyRows,hourlyDate:localDateNow(),heatmapDays:view.heatmapDays,
    modelRows:data.modelRows,toolRows:view.toolModelUsage,selectedSources:['claude']});
  for (const hourly of [false,true]) {
    const summary = summarizeTrendRows({...selected,hourly});
    assert.equal(summary.totalCostUsd, 2);
    assert.deepEqual(summary.costBreakdown,detailed);
    assert.equal(summary.localMetrics?.cacheHitRate,.9);
  }
});

test('old server missing optional metadata stays unknown', () => {
  const data = fixture();
  for(const row of data.dailyRows) {delete row.costBreakdown; delete row.localMetrics; delete row.sources;}
  for(const row of data.hourlyRows) {delete row.costBreakdown; delete row.localMetrics;}
  const view=buildDashboardDataFromDataset(data,1);
  assert.equal(view.summary.costBreakdown,undefined);
  assert.equal(view.summary.localMetrics,undefined);
});

test('one legacy row keeps mixed daily and hourly metadata unknown in either order', () => {
  for (const reverse of [false, true]) {
    const data = fixture();
    const legacyDay = { ...data.dailyRows[0]!, date: localDateNow().replace(/.$/, '0') };
    delete legacyDay.costBreakdown;
    delete legacyDay.localMetrics;
    const days = [data.dailyRows[0]!, legacyDay];
    if (reverse) days.reverse();
    const summary = buildSummaryFromRange(days, data.modelRows, '');
    assert.equal(summary.costBreakdown, undefined);
    assert.equal(summary.localMetrics, undefined);

    delete data.hourlyRows[1]!.costBreakdown;
    delete data.hourlyRows[1]!.localMetrics;
    if (reverse) data.hourlyRows.reverse();
    const hourly = summarizeTrendRows({
      dailyRows: [], hourlyRows: buildFilledHourlyForDate(data.hourlyRows, localDateNow()), hourly: true,
    });
    assert.equal(hourly.costBreakdown, undefined);
    assert.equal(hourly.localMetrics, undefined);
  }
});

test('filled empty hours are known zeros and explicit incomplete rates are not reconstructed', () => {
  const empty = buildFilledHourlyForDate([], localDateNow(), 0)[0]!;
  assert.deepEqual(empty.costBreakdown, emptyCostBreakdown());
  assert.deepEqual(empty.localMetrics, emptyLocalMetrics());
  assert.equal(mergeUsageQuality([
    empty, { localMetrics: { ...known, cacheHitRate: null }, costBreakdown: detailed },
  ]).localMetrics?.cacheHitRate, null);
  assert.equal(mergeUsageQuality([
    empty, { localMetrics: known, costBreakdown: detailed },
  ]).localMetrics?.cacheHitRate, .9);
});

test('date and project drilldowns preserve actual per-model fees and quality after filtering', () => {
  const data = fixture();
  const day = data.dailyRows[0]!;
  day.modelBreakdown = data.modelRows;
  day.projects = [{
    project: 'same-project', tokens: 300, models: day.models, costUsd: 10,
    modelBreakdown: data.modelRows, localMetrics: day.localMetrics, costBreakdown: total,
  }];
  const view = projectDashboardForDate(buildDashboardDataFromDataset(data, 1), day.date);
  assert.equal(view.modelRows.find((row) => row.source === 'claude')?.costUsd, 2);
  assert.equal(view.modelRows.find((row) => row.source === 'codex')?.costUsd, 8);
  const detailedTool = view.toolModelUsage.find((row) => row.source === 'claude')!;
  assert.deepEqual(detailedTool.costBreakdown, detailed);
  assert.deepEqual(detailedTool.models[0]!.costBreakdown, detailed);
  const project = view.projectModelUsage[0]!;
  assert.deepEqual(project.costBreakdown, total);
  assert.equal(project.models.find((row) => row.source === 'claude')?.costUsd, 2);
  const filtered = filterProjectRowsBySources([project], ['claude'])[0]!;
  assert.equal(filtered.costUsd, 2);
  assert.deepEqual(filtered.costBreakdown, detailed);
  assert.equal(filtered.localMetrics?.cacheHitRate, .9);
  const heatmapDay = filterHeatmapDaysBySources([day], ['claude'], data.modelRows)[0]!;
  assert.equal(heatmapDay.costUsd, 2);
  assert.deepEqual(heatmapDay.costBreakdown, detailed);
});

test('desktop date and source projections retain the same fee and cache evidence', () => {
  const data = fixture();
  const day = data.dailyRows[0]!;
  day.modelBreakdown = data.modelRows;
  day.projects = [{
    project: 'same-project', tokens: 300, models: day.models, costUsd: 10,
    modelBreakdown: data.modelRows, localMetrics: day.localMetrics, costBreakdown: total,
  }];
  const view = projectDesktopDate(buildDesktopData(data, 1), day.date);
  assert.deepEqual(view.summary.costBreakdown, total);
  assert.equal(view.summary.localMetrics?.cacheHitRate, null);
  assert.equal(view.projectModelUsage[0]?.models.find((row) => row.source === 'claude')?.costUsd, 2);
  const filtered = filterDesktopRows({
    dailyRows: view.rangeDailyUsage, hourlyRows: view.todayHourlyUsage,
    hourlyApiRows: data.hourlyRows, hourlyDate: day.date, heatmapDays: view.heatmapDays,
    modelRows: data.modelRows, toolRows: view.toolModelUsage, selectedSources: ['claude'],
  });
  for (const hourly of [false, true]) {
    const summary = summarizeDesktopRows({ ...filtered, hourly });
    assert.equal(summary.totalCostUsd, 2);
    assert.deepEqual(summary.costBreakdown, detailed);
    assert.equal(summary.localMetrics?.cacheHitRate, .9);
  }
});
