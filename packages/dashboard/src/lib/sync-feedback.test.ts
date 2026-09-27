import assert from 'node:assert/strict';
import test from 'node:test';
import { syncFeedback } from './sync-feedback.ts';
test('local completion never implies remote receipt when disabled, missing, pending or failed', () => {
  const times = {
    pendingBuckets: 3,
    lastAttemptAt: null,
    lastConfirmedAt: null,
  };
  assert.equal(syncFeedback().title, '本地同步完成');
  assert.equal(
    syncFeedback({ ...times, state: 'disabled' }).title,
    '本地同步完成',
  );
  assert.match(
    syncFeedback({ ...times, state: 'pending' }).title,
    /待补报（3 组）/,
  );
  assert.match(syncFeedback({ ...times, state: 'failed' }).title, /上传失败/);
  assert.match(
    syncFeedback({ ...times, state: 'confirmed' }).title,
    /云端已接收（排行榜可能稍后刷新）/,
  );
});
