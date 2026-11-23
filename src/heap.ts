/** Small binary heap. `before(a, b)` returns true when `a` should come out first. */
export class Heap<T> {
  items: T[] = [];
  constructor(private readonly before: (a: T, b: T) => boolean) {}

  get size(): number {
    return this.items.length;
  }

  peek(): T | undefined {
    return this.items[0];
  }

  push(x: T): void {
    const a = this.items;
    a.push(x);
    let i = a.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.before(a[i], a[p])) break;
      [a[i], a[p]] = [a[p], a[i]];
      i = p;
    }
  }

  pop(): T | undefined {
    const a = this.items;
    if (a.length === 0) return undefined;
    const top = a[0];
    const last = a.pop() as T;
    if (a.length > 0) {
      a[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  /** Keep only items matching `keep`, then restore heap order. */
  rebuild(keep: (x: T) => boolean): void {
    this.items = this.items.filter(keep);
    for (let i = (this.items.length >> 1) - 1; i >= 0; i--) this.siftDown(i);
  }

  private siftDown(start: number): void {
    const a = this.items;
    let i = start;
    for (;;) {
      const l = 2 * i + 1;
      const r = l + 1;
      let m = i;
      if (l < a.length && this.before(a[l], a[m])) m = l;
      if (r < a.length && this.before(a[r], a[m])) m = r;
      if (m === i) return;
      [a[i], a[m]] = [a[m], a[i]];
      i = m;
    }
  }
}
