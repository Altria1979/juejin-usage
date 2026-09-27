export function canRefreshLeaderboard(input: {
  hidden: boolean;
  authLoading: boolean;
  inFlight: boolean;
}): boolean {
  return !input.hidden && !input.authLoading && !input.inFlight;
}

interface LeaderboardRequest<T> {
  canStart: () => boolean;
  load: () => Promise<T>;
  onSuccess: (value: T) => void;
  onError: (error: unknown) => void;
}

/** Keep one request in flight and retain only the latest filter selection. */
export function createLeaderboardRequestQueue<T>() {
  let inFlight = false;
  let generation = 0;
  let pending: LeaderboardRequest<T> | null = null;

  function flush() {
    if (inFlight || !pending || !pending.canStart()) return;
    const request = pending;
    const startedGeneration = generation;
    pending = null;
    inFlight = true;

    void Promise.resolve()
      .then(request.load)
      .then(
        (value) => {
          if (generation === startedGeneration) request.onSuccess(value);
        },
        (error: unknown) => {
          if (generation === startedGeneration) request.onError(error);
        },
      )
      .finally(() => {
        inFlight = false;
        flush();
      });
  }

  return {
    schedule(request: LeaderboardRequest<T>) {
      generation += 1;
      pending = request;
      flush();
    },
    invalidate() {
      generation += 1;
      pending = null;
    },
    isRunning: () => inFlight,
  };
}
