/**
 * 进程内共享的单调序列分配器。
 *
 * 单线程事件循环下同步自增是原子的，因此并发请求不会拿到重复序号；
 * 序号只增不减，只有成功提交的事件才占用序号，保证实例内唯一且可排序。
 * 若未来多实例部署，需替换为共享存储的原子自增（如 Redis INCR）。
 */
export class SequenceAllocator {
  private current = 0;
  next(): number { return ++this.current; }
  peek(): number { return this.current; }
}
