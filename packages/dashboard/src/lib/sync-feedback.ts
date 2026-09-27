import type { UploadStatus } from '@juejin-opensource/jusage-core';

/** Local success and remote receipt are separate outcomes. */
export function syncFeedback(upload?: UploadStatus): {
  title: string;
  variant: 'success' | 'warning';
} {
  switch (upload?.state) {
    case 'confirmed':
      return {
        title: '本地同步完成，云端已接收（排行榜可能稍后刷新）',
        variant: 'success',
      };
    case 'pending':
      return {
        title: `本地同步完成，云端待补报（${upload.pendingBuckets} 组）`,
        variant: 'warning',
      };
    case 'failed':
      return {
        title: '本地同步完成，云端上传失败，将保留待补报数据',
        variant: 'warning',
      };
    default:
      return { title: '本地同步完成', variant: 'success' };
  }
}
