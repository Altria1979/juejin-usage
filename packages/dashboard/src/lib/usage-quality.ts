import {
  emptyCostBreakdown,
  emptyLocalMetrics,
  mergeLocalMetrics,
  mergeOptionalCostBreakdowns,
  type CostBreakdown,
  type LocalUsageMetrics,
} from '@juejin-opensource/jusage-core/local-metrics';

export interface UsageQuality {
  costBreakdown?: CostBreakdown;
  localMetrics?: LocalUsageMetrics;
}

export function emptyUsageQuality(): UsageQuality {
  return { costBreakdown: emptyCostBreakdown(), localMetrics: emptyLocalMetrics() };
}

/** Missing metadata poisons only its own aggregate; it is never a known zero. */
export function mergeUsageQuality(rows: readonly UsageQuality[]): UsageQuality {
  const costBreakdown = mergeOptionalCostBreakdowns(rows.map((row) => row.costBreakdown));
  const parts = rows.map((row) => row.localMetrics);
  const localMetrics = parts.every((part): part is LocalUsageMetrics => part !== undefined)
    ? mergeLocalMetrics(parts)
    : undefined;
  // A server may explicitly withhold the rate despite supplying partial counts.
  // Empty filled chart slots do not contribute uncertainty to a nonempty hour.
  if (localMetrics && parts.some((part) => part?.cacheHitRate === null && (
    part.cacheReadTokens === null || part.cacheWriteTokens === null ||
    part.uncachedInputTokens + part.cacheReadTokens + part.cacheWriteTokens > 0
  ))) localMetrics.cacheHitRate = null;
  return {
    ...(costBreakdown ? { costBreakdown } : {}),
    ...(localMetrics ? { localMetrics } : {}),
  };
}
