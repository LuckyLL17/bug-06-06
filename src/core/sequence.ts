/**
 * 服务实例内单调递增的序列号分配器。
 * 只要通过 next() 取号，就保证号段在本实例内唯一且可排序；
 * 调用方必须在串行临界区内调用，避免并发取号交叉。
 */
export class SequenceAllocator {
  private current = 0;

  next(): number {
    return ++this.current;
  }

  get value(): number {
    return this.current;
  }
}
