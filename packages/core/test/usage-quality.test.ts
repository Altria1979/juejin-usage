import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  aggregateDaily,
  aggregateHourly,
  aggregateModelBreakdown,
  aggregateUsageSummary,
  aggregateForIngest,
} from '../src/aggregate.js';
import { AggregateCache } from '../src/aggregate-cache.js';
import { metricsFromBucket, localEvidence } from '../src/local-metrics.js';
import { computeRowCost } from '../src/pricing/index.js';
import type { QueueBucket } from '../src/types.js';

function fixtures(): QueueBucket[] {
  const row: QueueBucket = {
    source: 'codex',
    model: 'gpt-5',
    project: 'alpha',
    hour_start: new Date().toISOString(),
    input_tokens: 10,
    cached_input_tokens: 90,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    reasoning_output_tokens: 0,
    total_tokens: 100,
    conversation_count: 1,
    local_metrics: localEvidence(1),
  };
  return [
    row,
    {
      ...row,
      project: 'beta',
      collector: 'codex-ledger',
      input_tokens: 200,
      cached_input_tokens: 0,
      total_tokens: 200,
      local_metrics: undefined,
    },
    {
      ...row,
      collector: 'codex-ledger',
      input_tokens: 50,
      cached_input_tokens: 0,
      total_tokens: 50,
      ledger_unverified_tokens: 0,
      local_metrics: undefined,
    },
  ];
}

test('cost attribution preserves totals and distinguishes historical ledger evidence through aggregates', () => {
  const rows = fixtures();
  const summary = aggregateUsageSummary(rows, '1970-01-01');
  assert.equal(summary.totalTokens, 350);
  assert.equal(summary.costBreakdown?.ledgerTokens, 250);
  assert.equal(summary.costBreakdown?.unverifiedLedgerTokens, 200);
  assert.equal(
    summary.costBreakdown?.detailedEstimatedCostUsd,
    computeRowCost(rows[0]!),
  );
  assert.equal(
    summary.costBreakdown?.ledgerEstimatedCostUsd,
    computeRowCost(rows[1]!) + computeRowCost(rows[2]!),
  );
  assert.deepEqual(summary.todayCostBreakdown, summary.costBreakdown);
  assert.deepEqual(summary.bySource[0]?.costBreakdown, summary.costBreakdown);
  assert.deepEqual(
    summary.bySource[0]?.models[0]?.costBreakdown,
    summary.costBreakdown,
  );
  const daily = aggregateDaily(rows, 1, '1970-01-01').days[0]!;
  assert.deepEqual(daily.costBreakdown, summary.costBreakdown);
  assert.deepEqual(daily.sources?.[0]?.costBreakdown, summary.costBreakdown);
  assert.equal(
    daily.projects?.find((p) => p.project === 'beta')?.costBreakdown
      ?.unverifiedLedgerTokens,
    200,
  );
  assert.deepEqual(
    daily.modelBreakdown?.[0]?.costBreakdown,
    summary.costBreakdown,
  );
  assert.equal(
    daily.projects?.find((p) => p.project === 'beta')?.modelBreakdown?.[0]
      ?.costBreakdown?.ledgerTokens,
    200,
  );
  assert.deepEqual(
    aggregateHourly(rows, 1, '1970-01-01').hours[0]?.costBreakdown,
    summary.costBreakdown,
  );
  const breakdown = aggregateModelBreakdown(rows, 1, '1970-01-01');
  assert.deepEqual(breakdown.models[0]?.costBreakdown, summary.costBreakdown);
  assert.equal(
    breakdown.projects.find((p) => p.project === 'alpha')?.costBreakdown
      ?.ledgerTokens,
    50,
  );
  assert.equal(
    breakdown.projects.find((p) => p.project === 'beta')?.models[0]
      ?.costBreakdown?.unverifiedLedgerTokens,
    200,
  );
});

test('uncategorized ledger cannot claim a known zero cache fraction or requests', () => {
  const row = fixtures()[1]!;
  const metrics = metricsFromBucket(row);
  assert.equal(metrics.cacheReadTokens, null);
  assert.equal(metrics.cacheWriteTokens, null);
  assert.equal(metrics.cacheHitRate, null);
  assert.equal(metrics.requestCount, null);
  assert.equal(metrics.knownRequestCount, 0);
  assert.equal(
    aggregateUsageSummary(fixtures(), '1970-01-01').localMetrics?.cacheHitRate,
    null,
  );
  assert(!('ledger_unverified_tokens' in aggregateForIngest([row])[0]!));
});

test('sealed history and freshly aggregated usage expose the same attribution', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'jusage-quality-'));
  try {
    const rows = fixtures().map((row) => ({
      ...row,
      hour_start: new Date(Date.now() - 86400000).toISOString(),
    }));
    const cache = new AggregateCache(dir);
    await cache.rebuildFromRows(rows);
    const direct = aggregateUsageSummary(rows, '1970-01-01');
    const cached = cache.getUsageSummary(rows, '1970-01-01');
    assert.equal(cached.costBreakdown?.ledgerTokens, 250);
    assert.deepEqual(cached.costBreakdown, direct.costBreakdown);
    assert.deepEqual(cached.bySource, direct.bySource);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
