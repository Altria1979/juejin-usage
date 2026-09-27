import assert from 'node:assert/strict';
import test from 'node:test';
import {
  canRefreshLeaderboard,
  createLeaderboardRequestQueue,
} from './leaderboard-refresh.ts';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

test('foreground refresh permits either backend and skips hidden, auth-loading or busy pages', () => {
  const ready = { hidden: false, authLoading: false, inFlight: false };
  assert.equal(canRefreshLeaderboard(ready), true);
  for (const blocker of ['hidden', 'authLoading', 'inFlight'] as const) {
    assert.equal(canRefreshLeaderboard({ ...ready, [blocker]: true }), false);
  }
});

test('a slow request cannot overwrite newer filters and only the latest queued selection is fetched', async () => {
  const queue = createLeaderboardRequestQueue<string>();
  const first = deferred<string>();
  const latest = deferred<string>();
  const requested: string[] = [];
  const displayed: string[] = [];
  const errors: unknown[] = [];
  const request = (key: string, promise: Promise<string>) => ({
    canStart: () => true,
    load: () => {
      requested.push(key);
      return promise;
    },
    onSuccess: (value: string) => displayed.push(value),
    onError: (error: unknown) => errors.push(error),
  });

  queue.schedule(request('today', first.promise));
  await settle();
  queue.invalidate();
  queue.schedule(request('week', Promise.resolve('week')));
  queue.invalidate();
  queue.schedule(request('month', latest.promise));
  assert.deepEqual(requested, ['today']);
  assert.equal(queue.isRunning(), true);

  first.resolve('stale today');
  await settle();
  assert.deepEqual(requested, ['today', 'month']);
  assert.deepEqual(displayed, []);
  latest.resolve('latest month');
  await settle();
  assert.deepEqual(displayed, ['latest month']);
  assert.deepEqual(errors, []);
  assert.equal(queue.isRunning(), false);
});

test('queued filters stay dormant while hidden and resume with the current selection on focus', async () => {
  const queue = createLeaderboardRequestQueue<string>();
  const first = deferred<string>();
  let hidden = false;
  const requested: string[] = [];
  const displayed: string[] = [];
  const request = (key: string, promise: Promise<string>) => ({
    canStart: () => !hidden,
    load: () => {
      requested.push(key);
      return promise;
    },
    onSuccess: (value: string) => displayed.push(value),
    onError: () => assert.fail('unexpected load error'),
  });

  queue.schedule(request('today', first.promise));
  await settle();
  hidden = true;
  queue.schedule(request('week', Promise.resolve('week')));
  first.resolve('stale today');
  await settle();
  assert.deepEqual(requested, ['today']);
  assert.deepEqual(displayed, []);

  hidden = false;
  queue.schedule(request('month', Promise.resolve('month')));
  await settle();
  assert.deepEqual(requested, ['today', 'month']);
  assert.deepEqual(displayed, ['month']);
});

test('an invalidated error does not replace data or prevent the next request', async () => {
  const queue = createLeaderboardRequestQueue<string>();
  const first = deferred<string>();
  const displayed: string[] = [];
  const errors: unknown[] = [];
  queue.schedule({
    canStart: () => true,
    load: () => first.promise,
    onSuccess: (value) => displayed.push(value),
    onError: (error) => errors.push(error),
  });
  await settle();
  queue.invalidate();
  queue.schedule({
    canStart: () => true,
    load: () => Promise.resolve('latest'),
    onSuccess: (value) => displayed.push(value),
    onError: (error) => errors.push(error),
  });
  first.reject(new Error('old filter request failed'));
  await settle();
  assert.deepEqual(displayed, ['latest']);
  assert.deepEqual(errors, []);
});

test('unmount or auth loading invalidates both the active result and a queued fetch', async () => {
  const queue = createLeaderboardRequestQueue<string>();
  const first = deferred<string>();
  const displayed: string[] = [];
  let calls = 0;
  const request = {
    canStart: () => true,
    load: () => { calls += 1; return first.promise; },
    onSuccess: (value: string) => displayed.push(value),
    onError: () => assert.fail('unexpected load error'),
  };
  queue.schedule(request);
  await settle();
  queue.schedule(request);
  queue.invalidate();
  first.resolve('stale');
  await settle();
  assert.equal(calls, 1);
  assert.deepEqual(displayed, []);
  assert.equal(queue.isRunning(), false);
});
