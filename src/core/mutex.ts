/**
 * 异步互斥锁：让并发提交的请求在进入提交临界区时串行化。
 * Node 单线程下同步代码本身不可抢占，但 ingest 是 async 接口，
 * 显式加锁可以保证“查重 -> 取号 -> 写事件/索引/遥测 -> 审计”整体原子，
 * 未来临界区内出现 await 也不会被重试请求穿插。
 */
export class AsyncMutex {
  private queue: Promise<void> = Promise.resolve();

  runExclusive<T>(fn: () => T | Promise<T>): Promise<T> {
    const result = this.queue.then(() => fn());
    // 单个任务失败不允许污染后续任务的等待链
    this.queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
