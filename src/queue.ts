import { QueueClearedError, QueueFullError, TimeoutError, WaitTimeoutError } from "./errors";
import { Heap } from "./heap";

export interface TaskContext {
  /** Aborted when the task times out, is aborted by its `signal`, or the queue calls `abortAll()`. Pass it to `fetch`. */
  signal: AbortSignal;
  /** 1 for the first try, 2 for the first retry, and so on. */
  attempt: number;
  id: number;
  group?: string;
  key?: string;
}
export type Task<T> = (ctx: TaskContext) => T | Promise<T>;

/** At most `limit` task starts in any rolling window of `per` milliseconds. */
export interface RateLimit {
  limit: number;
  per: number;
}

export interface RetryOptions {
  /** Extra attempts after the first. Default 0 (no retries). */
  retries?: number;
  /** Delay before the first retry in ms. Default 250. */
  minDelay?: number;
  /** Upper bound for the delay in ms. Default 30000. */
  maxDelay?: number;
  /** Backoff multiplier. Default 2. */
  factor?: number;
  /** Randomize each delay between 50% and 100% of its value. Default true. */
  jitter?: boolean;
  /** Return false to stop retrying. Errors with `retryable === false` are never retried by default. */
  shouldRetry?: (error: unknown, attempt: number) => boolean;
}

export interface GroupOptions {
  concurrency?: number;
  rate?: RateLimit;
}

export interface QueueOptions {
  /** Max tasks running at once. Default Infinity. */
  concurrency?: number;
  /** Global rate limit across all groups. */
  rate?: RateLimit;
  /** Start processing immediately. Default true. */
  autoStart?: boolean;
  /** Default per-attempt timeout in ms. */
  timeout?: number;
  /** Default retry policy (a number means `{ retries: n }`). */
  retry?: RetryOptions | number;
  /** Named lanes with their own concurrency and rate limits (in addition to the global ones). */
  groups?: Record<string, GroupOptions>;
  /** Max tasks waiting (not running). Default unlimited. */
  maxSize?: number;
  /** What to do when `maxSize` is reached. "reject" refuses the new task; "drop-lowest" evicts the lowest-priority waiting task if the new one outranks it. Default "reject". */
  overflow?: "reject" | "drop-lowest";
  /** Default max time in ms a task may wait before it first starts. */
  maxWait?: number;
  /** When a task fails with a `retryAfter` (ms), pause new work for that long. Default true. */
  cooldownOnRetryAfter?: boolean;
}

export interface AddOptions {
  /** Higher runs first. Default 0. Ties run in the order they were added. */
  priority?: number;
  /** Lane name; the lane's limits from `QueueOptions.groups` apply. */
  group?: string;
  /** While a task with this key is waiting or running, adding the same key returns the same promise. */
  key?: string;
  signal?: AbortSignal;
  timeout?: number;
  retry?: RetryOptions | number;
  maxWait?: number;
}

export interface QueueStats {
  added: number;
  completed: number;
  failed: number;
  aborted: number;
  /** Removed before starting: queue full, cleared, or waited too long. */
  dropped: number;
  retried: number;
  avgWaitMs: number;
  maxWaitMs: number;
  avgRunMs: number;
  maxRunMs: number;
}

export interface QueueEvents {
  add: { id: number; priority: number; group?: string; key?: string };
  active: { id: number; attempt: number; group?: string };
  completed: { id: number; attempt: number; durationMs: number; result: unknown };
  retry: { id: number; attempt: number; delayMs: number; error: unknown };
  error: { id: number; attempt: number; error: unknown };
  dropped: { id: number; reason: unknown };
  cooldown: { until: number; group?: string };
  empty: undefined;
  idle: undefined;
}

interface ResolvedRetry {
  retries: number;
  minDelay: number;
  maxDelay: number;
  factor: number;
  jitter: boolean;
  shouldRetry: (error: unknown, attempt: number) => boolean;
}

class RateWindow {
  private starts: number[] = [];
  private head = 0;
  constructor(readonly limit: number, readonly per: number) {}
  private prune(now: number): void {
    while (this.head < this.starts.length && this.starts[this.head] <= now - this.per) this.head++;
    if (this.head > 64 && this.head * 2 > this.starts.length) {
      this.starts = this.starts.slice(this.head);
      this.head = 0;
    }
  }
  canStart(now: number): boolean {
    this.prune(now);
    return this.starts.length - this.head < this.limit;
  }
  /** Earliest time a start slot frees up. Only meaningful when the window is full. */
  nextAt(now: number): number {
    this.prune(now);
    return this.starts[this.head] + this.per;
  }
  record(now: number): void {
    this.starts.push(now);
  }
}

interface Lane {
  name: string | undefined;
  heap: Heap<Job>;
  /** Waiting (not cancelled) jobs in this lane. */
  count: number;
  active: number;
  concurrency: number;
  rate?: RateWindow;
  cooldownUntil: number;
}

interface Job {
  id: number;
  seq: number;
  priority: number;
  fn: Task<unknown>;
  lane: Lane;
  group?: string;
  key?: string;
  timeout?: number;
  retry: ResolvedRetry;
  attempt: number;
  addedAt: number;
  started: boolean;
  state: "waiting" | "delayed" | "running" | "done";
  cancelled: boolean;
  aborted: boolean;
  userSignal?: AbortSignal;
  onUserAbort?: () => void;
  current?: AbortController;
  waitTimer?: ReturnType<typeof setTimeout>;
  delayTimer?: ReturnType<typeof setTimeout>;
  promise: Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
}

const before = (a: Job, b: Job): boolean => (a.priority !== b.priority ? a.priority > b.priority : a.seq < b.seq);

function checkPositive(name: string, value: number | undefined, allowInfinity = false): void {
  if (value === undefined) return;
  if (typeof value !== "number" || Number.isNaN(value) || value <= 0 || (!allowInfinity && !Number.isFinite(value))) {
    throw new TypeError(`${name} must be a positive number`);
  }
}

function checkRate(name: string, rate: RateLimit | undefined): void {
  if (!rate) return;
  if (!Number.isInteger(rate.limit) || rate.limit < 1) throw new TypeError(`${name}.limit must be a positive integer`);
  checkPositive(`${name}.per`, rate.per);
}

const defaultShouldRetry = (error: unknown): boolean =>
  !(typeof error === "object" && error !== null && (error as { retryable?: unknown }).retryable === false);

function resolveRetry(base: RetryOptions | number | undefined, over: RetryOptions | number | undefined): ResolvedRetry {
  const norm = (r: RetryOptions | number | undefined): RetryOptions => (typeof r === "number" ? { retries: r } : (r ?? {}));
  const o = { ...norm(base), ...norm(over) };
  return {
    retries: o.retries ?? 0,
    minDelay: o.minDelay ?? 250,
    maxDelay: o.maxDelay ?? 30_000,
    factor: o.factor ?? 2,
    jitter: o.jitter ?? true,
    shouldRetry: o.shouldRetry ?? defaultShouldRetry,
  };
}

function backoff(r: ResolvedRetry, attempt: number): number {
  const exp = Math.min(r.maxDelay, r.minDelay * r.factor ** (attempt - 1));
  return r.jitter ? exp / 2 + Math.random() * (exp / 2) : exp;
}

function retryAfterOf(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const v = (error as { retryAfter?: unknown }).retryAfter;
  return typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : undefined;
}

export class Queue {
  private readonly lanes = new Map<string | undefined, Lane>();
  private readonly running = new Set<Job>();
  private readonly delayed = new Set<Job>();
  private readonly keys = new Map<string, Promise<unknown>>();
  private readonly listeners = new Map<string, Set<(payload: never) => void>>();
  private idleWaiters: (() => void)[] = [];
  private emptyWaiters: (() => void)[] = [];
  private sizeWaiters: { limit: number; resolve: () => void }[] = [];

  private readonly options: QueueOptions;
  private readonly rate?: RateWindow;
  private concurrencyLimit: number;
  private paused: boolean;
  private pumping = false;
  private active = 0;
  private waitingCount = 0;
  private cooldownUntil = 0;
  private wakeTimer?: ReturnType<typeof setTimeout>;
  private wakeAt = Infinity;
  private nextId = 1;
  private idleEmitted = true;
  private sawWaiting = false;

  private readonly counters = { added: 0, completed: 0, failed: 0, aborted: 0, dropped: 0, retried: 0 };
  private readonly timing = { waitTotal: 0, waitN: 0, waitMax: 0, runTotal: 0, runN: 0, runMax: 0 };

  constructor(options: QueueOptions = {}) {
    checkPositive("concurrency", options.concurrency, true);
    checkPositive("timeout", options.timeout);
    checkPositive("maxWait", options.maxWait);
    checkPositive("maxSize", options.maxSize, true);
    checkRate("rate", options.rate);
    for (const [name, g] of Object.entries(options.groups ?? {})) {
      checkPositive(`groups.${name}.concurrency`, g.concurrency, true);
      checkRate(`groups.${name}.rate`, g.rate);
    }
    this.options = options;
    this.concurrencyLimit = options.concurrency ?? Infinity;
    this.paused = options.autoStart === false;
    if (options.rate) this.rate = new RateWindow(options.rate.limit, options.rate.per);
  }

  /** Waiting tasks, including those sleeping before a retry. */
  get size(): number {
    return this.waitingCount + this.delayed.size;
  }
  /** Tasks running right now. */
  get pending(): number {
    return this.active;
  }
  get isPaused(): boolean {
    return this.paused;
  }
  get concurrency(): number {
    return this.concurrencyLimit;
  }
  setConcurrency(n: number): void {
    checkPositive("concurrency", n, true);
    this.concurrencyLimit = n;
    this.pump();
  }

  on<K extends keyof QueueEvents>(event: K, listener: (payload: QueueEvents[K]) => void): () => void {
    let set = this.listeners.get(event);
    if (!set) this.listeners.set(event, (set = new Set()));
    set.add(listener as (payload: never) => void);
    return () => void set.delete(listener as (payload: never) => void);
  }

  private emit<K extends keyof QueueEvents>(event: K, payload: QueueEvents[K]): void {
    const set = this.listeners.get(event);
    if (!set) return;
    for (const l of [...set]) {
      try {
        (l as (p: QueueEvents[K]) => void)(payload);
      } catch {
        /* listeners must not break the queue */
      }
    }
  }

  add<T>(fn: Task<T>, options: AddOptions = {}): Promise<T> {
    if (options.key !== undefined) {
      const existing = this.keys.get(options.key);
      if (existing) return existing as Promise<T>;
    }
    checkPositive("timeout", options.timeout);
    checkPositive("maxWait", options.maxWait);
    const signal = options.signal;
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("Aborted"));

    const priority = options.priority ?? 0;
    if (this.options.maxSize !== undefined && this.waitingCount >= this.options.maxSize) {
      const lowest = this.options.overflow === "drop-lowest" ? this.findLowest() : undefined;
      if (lowest && priority > lowest.priority) {
        this.removePending(lowest, new QueueFullError("Dropped: queue full and a higher-priority task arrived"));
      } else {
        this.counters.dropped++;
        const err = new QueueFullError();
        this.emit("dropped", { id: -1, reason: err });
        return Promise.reject(err);
      }
    }

    let resolve!: (v: unknown) => void;
    let reject!: (e: unknown) => void;
    const promise = new Promise<unknown>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    const lane = this.laneFor(options.group);
    const job: Job = {
      id: this.nextId++,
      seq: this.counters.added,
      priority,
      fn,
      lane,
      group: options.group,
      key: options.key,
      timeout: options.timeout ?? this.options.timeout,
      retry: resolveRetry(this.options.retry, options.retry),
      attempt: 0,
      addedAt: Date.now(),
      started: false,
      state: "waiting",
      cancelled: false,
      aborted: false,
      userSignal: signal,
      promise,
      resolve,
      reject,
    };
    this.counters.added++;
    if (job.key !== undefined) this.keys.set(job.key, promise);
    if (signal) {
      job.onUserAbort = () => this.handleUserAbort(job);
      signal.addEventListener("abort", job.onUserAbort, { once: true });
    }
    const maxWait = options.maxWait ?? this.options.maxWait;
    if (maxWait !== undefined) {
      job.waitTimer = setTimeout(() => {
        if (!job.started && job.state !== "done") this.removePending(job, new WaitTimeoutError(maxWait));
      }, maxWait);
    }

    this.idleEmitted = false;
    this.enqueue(job);
    this.emit("add", { id: job.id, priority, group: job.group, key: job.key });
    this.pump();
    return promise as Promise<T>;
  }

  /** Add several tasks with shared options. Resolves to all results in order. */
  addAll<T>(tasks: Task<T>[], options?: AddOptions): Promise<T[]> {
    return Promise.all(tasks.map((t) => this.add(t, options)));
  }

  /** Run `fn` for every item through the queue. Results keep the input order. */
  map<I, O>(items: Iterable<I>, fn: (item: I, index: number, ctx: TaskContext) => O | Promise<O>, options?: AddOptions): Promise<O[]> {
    let i = 0;
    const out: Promise<O>[] = [];
    for (const item of items) {
      const index = i++;
      out.push(this.add((ctx) => fn(item, index, ctx), options));
    }
    return Promise.all(out);
  }

  /** Stop starting new tasks. Running tasks continue. */
  pause(): void {
    this.paused = true;
  }
  start(): void {
    this.paused = false;
    this.pump();
  }

  /** Reject every task that has not started (including ones waiting to retry). Returns how many. */
  clear(): number {
    const victims: Job[] = [];
    for (const lane of this.lanes.values()) for (const j of lane.heap.items) if (!j.cancelled) victims.push(j);
    for (const j of this.delayed) victims.push(j);
    for (const j of victims) this.removePending(j, new QueueClearedError());
    return victims.length;
  }

  /** `clear()` and also abort the signal of every running task. */
  abortAll(reason: unknown = new Error("Queue aborted")): number {
    const n = this.clear();
    for (const j of this.running) {
      j.aborted = true;
      j.current?.abort(reason);
    }
    return n;
  }

  /** Make all new work wait `ms` (for one group, or everything). Useful when you learn you are being throttled. */
  cooldown(ms: number, group?: string): void {
    const until = Date.now() + ms;
    if (group === undefined) this.cooldownUntil = Math.max(this.cooldownUntil, until);
    else {
      const lane = this.laneFor(group);
      lane.cooldownUntil = Math.max(lane.cooldownUntil, until);
    }
    this.emit("cooldown", { until, group });
    if (this.waitingCount > 0) this.scheduleWake(until);
  }

  /** Resolves when nothing is waiting or running. */
  onIdle(): Promise<void> {
    if (this.size === 0 && this.active === 0) return Promise.resolve();
    return new Promise((res) => this.idleWaiters.push(res));
  }
  /** Resolves when nothing is waiting (tasks may still be running). */
  onEmpty(): Promise<void> {
    if (this.size === 0) return Promise.resolve();
    return new Promise((res) => this.emptyWaiters.push(res));
  }
  /** Resolves when fewer than `limit` tasks are waiting. Handy for backpressure when producing work in a loop. */
  onSizeLessThan(limit: number): Promise<void> {
    if (this.size < limit) return Promise.resolve();
    return new Promise((resolve) => this.sizeWaiters.push({ limit, resolve }));
  }

  stats(): QueueStats {
    const t = this.timing;
    return {
      ...this.counters,
      avgWaitMs: t.waitN ? t.waitTotal / t.waitN : 0,
      maxWaitMs: t.waitMax,
      avgRunMs: t.runN ? t.runTotal / t.runN : 0,
      maxRunMs: t.runMax,
    };
  }

  // ---------------------------------------------------------------- internals

  private laneFor(name: string | undefined): Lane {
    let lane = this.lanes.get(name);
    if (!lane) {
      const cfg = name === undefined ? undefined : this.options.groups?.[name];
      lane = {
        name,
        heap: new Heap<Job>(before),
        count: 0,
        active: 0,
        concurrency: cfg?.concurrency ?? Infinity,
        rate: cfg?.rate ? new RateWindow(cfg.rate.limit, cfg.rate.per) : undefined,
        cooldownUntil: 0,
      };
      this.lanes.set(name, lane);
    }
    return lane;
  }

  private enqueue(job: Job): void {
    job.state = "waiting";
    job.cancelled = false;
    job.lane.heap.push(job);
    job.lane.count++;
    this.waitingCount++;
  }

  private findLowest(): Job | undefined {
    let lowest: Job | undefined;
    for (const lane of this.lanes.values()) {
      for (const j of lane.heap.items) {
        if (j.cancelled) continue;
        if (!lowest || j.priority < lowest.priority || (j.priority === lowest.priority && j.seq > lowest.seq)) lowest = j;
      }
    }
    return lowest;
  }

  /** Remove a job that has not started (waiting or sleeping before a retry) and reject it. */
  private removePending(job: Job, error: unknown): void {
    if (job.state === "waiting") {
      job.cancelled = true;
      job.lane.count--;
      this.waitingCount--;
      const heap = job.lane.heap;
      if (heap.size > 64 && job.lane.count * 2 < heap.size) heap.rebuild((j) => !j.cancelled);
    } else if (job.state === "delayed") {
      if (job.delayTimer !== undefined) clearTimeout(job.delayTimer);
      this.delayed.delete(job);
    } else {
      return;
    }
    this.counters.dropped++;
    this.emit("dropped", { id: job.id, reason: error });
    this.settle(job, false, error);
    this.pump();
  }

  private handleUserAbort(job: Job): void {
    const reason = job.userSignal?.reason ?? new Error("Aborted");
    job.aborted = true;
    if (job.state === "running") job.current?.abort(reason);
    else if (job.state !== "done") {
      this.removePending(job, reason);
      this.counters.dropped--;
      this.counters.aborted++;
    }
  }

  private settle(job: Job, ok: boolean, value: unknown): void {
    job.state = "done";
    if (job.waitTimer !== undefined) clearTimeout(job.waitTimer);
    if (job.delayTimer !== undefined) clearTimeout(job.delayTimer);
    if (job.userSignal && job.onUserAbort) job.userSignal.removeEventListener("abort", job.onUserAbort);
    if (job.key !== undefined && this.keys.get(job.key) === job.promise) this.keys.delete(job.key);
    if (ok) job.resolve(value);
    else job.reject(value);
  }

  private scheduleWake(at: number): void {
    if (at === Infinity) return;
    if (this.wakeTimer !== undefined && this.wakeAt <= at) return;
    if (this.wakeTimer !== undefined) clearTimeout(this.wakeTimer);
    this.wakeAt = at;
    this.wakeTimer = setTimeout(() => {
      this.wakeTimer = undefined;
      this.wakeAt = Infinity;
      this.pump();
    }, Math.max(0, at - Date.now()));
  }

  private pump(): void {
    this.runPump();
    this.notify();
  }

  private runPump(): void {
    if (this.pumping || this.paused) return;
    this.pumping = true;
    try {
      for (;;) {
        if (this.active >= this.concurrencyLimit) return;
        const now = Date.now();
        if (now < this.cooldownUntil) {
          if (this.waitingCount > 0) this.scheduleWake(this.cooldownUntil);
          return;
        }
        if (this.rate && !this.rate.canStart(now)) {
          if (this.waitingCount > 0) this.scheduleWake(this.rate.nextAt(now));
          return;
        }
        let best: Job | undefined;
        let wake = Infinity;
        for (const lane of this.lanes.values()) {
          if (lane.count === 0) continue;
          if (now < lane.cooldownUntil) {
            wake = Math.min(wake, lane.cooldownUntil);
            continue;
          }
          if (lane.active >= lane.concurrency) continue;
          if (lane.rate && !lane.rate.canStart(now)) {
            wake = Math.min(wake, lane.rate.nextAt(now));
            continue;
          }
          while (lane.heap.size > 0 && (lane.heap.peek() as Job).cancelled) lane.heap.pop();
          const top = lane.heap.peek();
          if (top && (!best || before(top, best))) best = top;
        }
        if (!best) {
          if (wake !== Infinity) this.scheduleWake(wake);
          return;
        }
        best.lane.heap.pop();
        best.lane.count--;
        this.waitingCount--;
        this.startJob(best, now);
      }
    } finally {
      this.pumping = false;
    }
  }

  private startJob(job: Job, now: number): void {
    this.active++;
    job.lane.active++;
    this.rate?.record(now);
    job.lane.rate?.record(now);
    this.running.add(job);
    job.state = "running";
    job.attempt++;
    if (!job.started) {
      job.started = true;
      if (job.waitTimer !== undefined) clearTimeout(job.waitTimer);
      const waited = now - job.addedAt;
      this.timing.waitTotal += waited;
      this.timing.waitN++;
      this.timing.waitMax = Math.max(this.timing.waitMax, waited);
    }
    this.emit("active", { id: job.id, attempt: job.attempt, group: job.group });
    void this.execute(job, now);
  }

  private async execute(job: Job, startedAt: number): Promise<void> {
    const ctl = new AbortController();
    job.current = ctl;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const ctx: TaskContext = { signal: ctl.signal, attempt: job.attempt, id: job.id, group: job.group, key: job.key };
    let ok = false;
    let value: unknown;
    let error: unknown;
    try {
      value = await new Promise<unknown>((resolve, reject) => {
        ctl.signal.addEventListener("abort", () => reject(ctl.signal.reason), { once: true });
        if (job.timeout !== undefined) {
          const ms = job.timeout;
          timer = setTimeout(() => ctl.abort(new TimeoutError(ms)), ms);
        }
        // Do not call resolve(thenable): that would lock this promise to the task's and make timeout/abort a no-op.
        let result: Promise<unknown>;
        try {
          result = Promise.resolve(job.fn(ctx));
        } catch (e) {
          reject(e);
          return;
        }
        result.then(resolve, reject);
      });
      ok = true;
    } catch (e) {
      error = e;
    }
    if (timer !== undefined) clearTimeout(timer);
    job.current = undefined;
    const runMs = Date.now() - startedAt;
    this.active--;
    job.lane.active--;
    this.running.delete(job);
    this.timing.runTotal += runMs;
    this.timing.runN++;
    this.timing.runMax = Math.max(this.timing.runMax, runMs);

    if (ok) {
      this.counters.completed++;
      this.emit("completed", { id: job.id, attempt: job.attempt, durationMs: runMs, result: value });
      this.settle(job, true, value);
    } else {
      this.onFailure(job, error);
    }
    this.pump();
  }

  private onFailure(job: Job, error: unknown): void {
    if (job.aborted) {
      this.counters.aborted++;
      this.emit("error", { id: job.id, attempt: job.attempt, error });
      this.settle(job, false, error);
      return;
    }
    const retryAfter = retryAfterOf(error);
    if (retryAfter !== undefined && this.options.cooldownOnRetryAfter !== false) this.cooldown(retryAfter, job.group);
    const canRetry = job.attempt <= job.retry.retries && job.retry.shouldRetry(error, job.attempt);
    if (!canRetry) {
      this.counters.failed++;
      this.emit("error", { id: job.id, attempt: job.attempt, error });
      this.settle(job, false, error);
      return;
    }
    const delayMs = retryAfter ?? backoff(job.retry, job.attempt);
    this.counters.retried++;
    this.emit("retry", { id: job.id, attempt: job.attempt, delayMs, error });
    job.state = "delayed";
    this.delayed.add(job);
    job.delayTimer = setTimeout(() => {
      job.delayTimer = undefined;
      if (job.state !== "delayed") return;
      this.delayed.delete(job);
      this.enqueue(job);
      this.pump();
    }, delayMs);
  }

  private notify(): void {
    const size = this.size;
    if (size > 0) this.sawWaiting = true;
    else {
      if (this.emptyWaiters.length) {
        const w = this.emptyWaiters;
        this.emptyWaiters = [];
        w.forEach((r) => r());
      }
      if (this.sawWaiting) {
        this.sawWaiting = false;
        this.emit("empty", undefined);
      }
    }
    if (size === 0 && this.active === 0) {
      if (this.idleWaiters.length) {
        const w = this.idleWaiters;
        this.idleWaiters = [];
        w.forEach((r) => r());
      }
      if (!this.idleEmitted) {
        this.idleEmitted = true;
        this.emit("idle", undefined);
      }
    }
    if (this.sizeWaiters.length) {
      const still: typeof this.sizeWaiters = [];
      for (const w of this.sizeWaiters) (size < w.limit ? w.resolve : () => still.push(w))();
      this.sizeWaiters = still;
    }
  }
}
