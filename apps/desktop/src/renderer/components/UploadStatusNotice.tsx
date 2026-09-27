import { useEffect, useState } from 'react';
import type { UploadStatus } from '@juejin-opensource/jusage-core';
import { fetchSyncStatus } from '@/lib/api';
import { DATA_SYNCED_EVENT } from '@/lib/shell-events';

/** Reads persisted upload state, including work performed by the desktop worker. */
export function UploadStatusNotice() {
  const [status, setStatus] = useState<UploadStatus>();
  useEffect(() => {
    let disposed = false;
    let inFlight = false;
    const refresh = async () => {
      if (disposed || inFlight || document.visibilityState === 'hidden') return;
      inFlight = true;
      try {
        const result = await fetchSyncStatus();
        if (!disposed) setStatus(result.upload);
      } catch {
        if (!disposed) setStatus(undefined);
      } finally {
        inFlight = false;
      }
    };
    void refresh();
    const timer = window.setInterval(() => void refresh(), 10_000);
    window.addEventListener('focus', refresh);
    window.addEventListener(DATA_SYNCED_EVENT, refresh);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      disposed = true;
      window.clearInterval(timer);
      window.removeEventListener('focus', refresh);
      window.removeEventListener(DATA_SYNCED_EVENT, refresh);
      document.removeEventListener('visibilitychange', refresh);
    };
  }, []);
  const labels = {
    disabled: '未启用云端同步',
    confirmed: '云端已接收',
    pending: '云端待补报',
    failed: '云端上传失败',
  };
  return (
    <div
      className="grid gap-1 border-t border-border pt-2 text-xs"
      aria-live="polite"
    >
      <div className="flex justify-between gap-3">
        <span className="text-muted">云端同步</span>
        <span>{status ? labels[status.state] : '暂无状态'}</span>
      </div>
      {status && status.state !== 'disabled' && (
        <>
          <p className="text-muted">
            待处理 {status.pendingBuckets} 组；最近确认{' '}
            {status.lastConfirmedAt
              ? new Date(status.lastConfirmedAt).toLocaleString()
              : '暂无'}
          </p>
          {status.message && <p className="text-muted">{status.message}</p>}
          <p className="text-muted">
            已接收表示用量已确认，排行榜可能稍后刷新。
          </p>
        </>
      )}
    </div>
  );
}
