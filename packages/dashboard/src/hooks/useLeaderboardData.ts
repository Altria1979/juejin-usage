import { useCallback, useEffect, useRef, useState } from 'react';
import { useJuejinAuth } from '@/hooks/JuejinAuthContext';
import {
  fetchLeaderboardOverview,
  isCliBackend,
  type LeaderboardFilters,
  type LeaderboardOverviewResponse,
  type LeaderboardRange,
} from '@/lib/api';
import { isMockDataEnabled } from '@/lib/env';
import { createMockLeaderboardOverview } from '@/lib/leaderboard-mock-data';
import {
  canRefreshLeaderboard,
  createLeaderboardRequestQueue,
} from '@/lib/leaderboard-refresh';

interface LeaderboardDataState {
  data: LeaderboardOverviewResponse | null;
  loading: boolean;
  refreshing: boolean;
  error: string | null;
}

/** Poll only while the page is visible, for both web and local CLI. */
const POLL_MS = 10_000;

function requestKey(range: LeaderboardRange, filters: LeaderboardFilters) {
  return `${range}|${filters.tool ?? ''}|${filters.model ?? ''}`;
}

export function useLeaderboardData(
  range: LeaderboardRange,
  filters: LeaderboardFilters = {},
) {
  const mockEnabled = isMockDataEnabled();
  const cliBackend = isCliBackend();
  const { authStatus, userId } = useJuejinAuth();
  const authLoading = !cliBackend && !mockEnabled && authStatus === 'loading';
  const requestsRef = useRef(createLeaderboardRequestQueue<LeaderboardOverviewResponse>());
  const [revision, setRevision] = useState(0);
  const lastKeyRef = useRef<string | null>(null);
  const manualReloadRef = useRef(false);
  const [state, setState] = useState<LeaderboardDataState>({
    data: null,
    loading: true,
    refreshing: false,
    error: null,
  });

  const reload = useCallback((options?: { silent?: boolean }) => {
    manualReloadRef.current = options?.silent !== true;
    setRevision((current) => current + 1);
  }, []);

  useEffect(() => {
    const requests = requestsRef.current;
    const key = requestKey(range, filters);

    // Server: wait for user/get to settle so authenticated calls can send user_id.
    if (authLoading) {
      setState((current) => ({
        ...current,
        loading: current.data == null,
        refreshing: false,
        error: null,
      }));
      return () => {
        requests.invalidate();
      };
    }

    // Skeleton only when empty. Filter/range switches keep prior rows visible
    // (refreshing). Same-key polls stay silent to avoid periodic layout jumps.
    // Manual reloads always surface the refreshing state so the retry button
    // gives feedback even when the key is unchanged.
    const isManualReload = manualReloadRef.current;
    manualReloadRef.current = false;
    setState((current) => {
      if (current.data == null) {
        return {
          ...current,
          loading: true,
          refreshing: false,
          error: null,
        };
      }
      const isFilterChange =
        lastKeyRef.current != null && lastKeyRef.current !== key;
      if (isFilterChange || isManualReload) {
        return {
          ...current,
          loading: false,
          refreshing: true,
          error: null,
        };
      }
      return current;
    });

    requests.schedule({
      canStart: () => !document.hidden,
      load: () => mockEnabled
        ? Promise.resolve(createMockLeaderboardOverview(range, filters))
        : fetchLeaderboardOverview(range, undefined, filters),
      onSuccess: (data) => {
        lastKeyRef.current = key;
        setState({ data, loading: false, refreshing: false, error: null });
      },
      onError: (error: unknown) => {
        setState((current) => ({
          data: current.data,
          loading: false,
          refreshing: false,
          error: error instanceof Error ? error.message : '排行榜加载失败',
        }));
      },
    });

    return () => {
      requests.invalidate();
    };
  }, [range, revision, mockEnabled, authLoading, authStatus, userId, filters.model, filters.tool]);

  // Neither backend pushes leaderboard updates to the page.
  useEffect(() => {
    const refreshIfReady = () => {
      if (!canRefreshLeaderboard({
        hidden: document.hidden,
        authLoading,
        inFlight: requestsRef.current.isRunning(),
      })) return;
      reload({ silent: true });
    };
    const intervalId = window.setInterval(refreshIfReady, POLL_MS);
    window.addEventListener('focus', refreshIfReady);
    document.addEventListener('visibilitychange', refreshIfReady);

    return () => {
      window.clearInterval(intervalId);
      window.removeEventListener('focus', refreshIfReady);
      document.removeEventListener('visibilitychange', refreshIfReady);
    };
  }, [reload, authLoading]);

  return {
    ...state,
    reload,
  };
}
