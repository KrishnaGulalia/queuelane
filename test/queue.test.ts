import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  Queue,
  QueueClearedError,
  QueueFullError,
  RateLimitError,
  TimeoutError,
  WaitTimeoutError,
  checkResponse,
  parseRetryAfter,
} from "../src/index";

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("basics", () => {
  it("runs tasks and returns results, sync or async", async () => {
    const q = new Queue({ concurrency: 2 });
    expect(await q.add(() => 1)).toBe(1);
    expect(await q.add(async () => "two")).toBe("two");
  });

  it("rejects when a task throws synchronously or asynchronously", async () => {
    const q = new Queue();
    await expect(q.add(() => { throw new Error("sync"); })).rejects.toThrow("sync");
    await expect(q.add(async () => { throw new Error("async"); })).rejects.toThrow("async");
  });

  it("never exceeds the concurrency limit", async () => {
    const q = new Queue({ concurrency: 3 });
    let active = 0;
    let max = 0;
    await q.addAll(
      Array.from({ length: 20 }, () => async () => {
        max = Math.max(max, ++active);
        await sleep(2);
        active--;
      }),
    );
    expect(max).toBe(3);
  });

  it("validates options", () => {
    expect(() => new Queue({ concurrency: 0 })).toThrow(TypeError);
    expect(() => new Queue({ rate: { limit: 0, per: 1000 } })).toThrow(TypeError);
    expect(() => new Queue({ rate: { limit: 1, per: -5 } })).toThrow(TypeError);
  });

  it("map keeps input order", async () => {
    const q = new Queue({ concurrency: 4 });
    const out = await q.map([5, 1, 3, 2], async (n) => {
      await sleep(n);
      return n * 10;
    });
    expect(out).toEqual([50, 10, 30, 20]);
  });
});

describe("priority", () => {
  it("runs higher priority first and FIFO within a priority", async () => {
    const q = new Queue({ concurrency: 1, autoStart: false });
    const order: string[] = [];
    const add = (name: string, priority: number) => q.add(() => void order.push(name), { priority });
    const all = [add("low1", 0), add("high1", 5), add("low2", 0), add("high2", 5), add("mid", 2)];
    q.start();
    await Promise.all(all);
    expect(order).toEqual(["high1", "high2", "mid", "low1", "low2"]);
  });
});

describe("pause, clear and idle", () => {
  it("autoStart:false waits for start(); pause stops new starts", async () => {
    const q = new Queue({ autoStart: false });
    let ran = false;
    const p = q.add(() => void (ran = true));
    await tick();
    expect(ran).toBe(false);
    expect(q.size).toBe(1);
    q.start();
    await p;
    expect(ran).toBe(true);
    q.pause();
    let ran2 = false;
    void q.add(() => void (ran2 = true));
    await tick();
    expect(ran2).toBe(false);
    expect(q.isPaused).toBe(true);
  });

  it("clear rejects waiting tasks but leaves running ones alone", async () => {
    const q = new Queue({ concurrency: 1 });
    const running = q.add(async () => { await sleep(10); return "done"; });
    const waiting = q.add(() => "never");
    const caught = waiting.catch((e) => e);
    await tick();
    expect(q.clear()).toBe(1);
    expect(await caught).toBeInstanceOf(QueueClearedError);
    expect(await running).toBe("done");
  });

  it("onIdle, onEmpty and onSizeLessThan resolve at the right moments", async () => {
    const q = new Queue({ concurrency: 1 });
    const events: string[] = [];
    q.on("empty", () => events.push("empty"));
    q.on("idle", () => events.push("idle"));
    void q.add(() => sleep(5));
    void q.add(() => sleep(5));
    void q.add(() => sleep(5));
    await q.onSizeLessThan(2);
    expect(q.size).toBeLessThan(2);
    await q.onEmpty();
    expect(q.size).toBe(0);
    await q.onIdle();
    expect(q.pending).toBe(0);
    expect(events).toEqual(["empty", "idle"]);
  });

  it("setConcurrency changes throughput on the fly", async () => {
    const q = new Queue({ concurrency: 1 });
    let active = 0;
    let max = 0;
    const work = () => async () => {
      max = Math.max(max, ++active);
      await sleep(5);
      active--;
    };
    const first = q.addAll(Array.from({ length: 4 }, work));
    q.setConcurrency(4);
    await first;
    expect(max).toBeGreaterThan(1);
  });
});

describe("rate limiting and groups (fake timers)", () => {
  beforeEach(() => void vi.useFakeTimers());
  afterEach(() => void vi.useRealTimers());

  it("enforces a rolling-window rate limit", async () => {
    const q = new Queue({ rate: { limit: 2, per: 1000 } });
    const t0 = Date.now();
    const starts: number[] = [];
    const ps = Array.from({ length: 5 }, () => q.add(() => void starts.push(Date.now() - t0)));
    await vi.advanceTimersByTimeAsync(5000);
    await Promise.all(ps);
    expect(starts).toEqual([0, 0, 1000, 1000, 2000]);
  });

  it("applies group concurrency while other groups keep flowing", async () => {
    const q = new Queue({ concurrency: 10, groups: { slow: { concurrency: 1 } } });
    let slowActive = 0;
    let slowMax = 0;
    let fastRan = 0;
    const slow = Array.from({ length: 3 }, () =>
      q.add(async () => {
        slowMax = Math.max(slowMax, ++slowActive);
        await sleep(100);
        slowActive--;
      }, { group: "slow" }),
    );
    const fast = Array.from({ length: 3 }, () => q.add(() => void fastRan++));
    await vi.advanceTimersByTimeAsync(0);
    expect(fastRan).toBe(3);
    await vi.advanceTimersByTimeAsync(500);
    await Promise.all([...slow, ...fast]);
    expect(slowMax).toBe(1);
  });

  it("applies a per-group rate limit independent of the global one", async () => {
    const q = new Queue({ groups: { api: { rate: { limit: 1, per: 500 } } } });
    const t0 = Date.now();
    const api: number[] = [];
    const other: number[] = [];
    const ps = [
      q.add(() => void api.push(Date.now() - t0), { group: "api" }),
      q.add(() => void api.push(Date.now() - t0), { group: "api" }),
      q.add(() => void other.push(Date.now() - t0)),
      q.add(() => void other.push(Date.now() - t0)),
    ];
    await vi.advanceTimersByTimeAsync(2000);
    await Promise.all(ps);
    expect(api).toEqual([0, 500]);
    expect(other).toEqual([0, 0]);
  });

  it("retries with exponential backoff", async () => {
    const q = new Queue({ retry: { retries: 2, minDelay: 100, factor: 2, jitter: false } });
    const t0 = Date.now();
    const attempts: number[] = [];
    const p = q.add(({ attempt }) => {
      attempts.push(Date.now() - t0);
      if (attempt < 3) throw new Error("flaky");
      return "ok";
    });
    await vi.advanceTimersByTimeAsync(1000);
    expect(await p).toBe("ok");
    expect(attempts).toEqual([0, 100, 300]);
    expect(q.stats().retried).toBe(2);
  });

  it("gives up after the retry budget and rejects with the last error", async () => {
    const q = new Queue({ retry: { retries: 1, minDelay: 10, jitter: false } });
    const p = q.add(({ attempt }) => { throw new Error(`fail ${attempt}`); });
    const caught = p.catch((e) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(((await caught) as Error).message).toBe("fail 2");
    expect(q.stats().failed).toBe(1);
  });

  it("does not retry when shouldRetry says no or the error is not retryable", async () => {
    const q = new Queue({ retry: { retries: 5, minDelay: 1, jitter: false, shouldRetry: (e) => (e as Error).message !== "fatal" } });
    let calls = 0;
    const p = q.add(() => { calls++; throw new Error("fatal"); }).catch((e) => e);
    await vi.advanceTimersByTimeAsync(100);
    await p;
    expect(calls).toBe(1);

    const q2 = new Queue({ retry: 5 });
    let calls2 = 0;
    const p2 = q2.add(() => { calls2++; throw Object.assign(new Error("nope"), { retryable: false }); }).catch((e) => e);
    await vi.advanceTimersByTimeAsync(100);
    await p2;
    expect(calls2).toBe(1);
  });

  it("a retryAfter error cools the whole queue down and is used as the retry delay", async () => {
    const q = new Queue({ concurrency: 1, retry: { retries: 1, jitter: false } });
    const t0 = Date.now();
    const started: Record<string, number> = {};
    const a = q.add(({ attempt }) => {
      started[`a${attempt}`] = Date.now() - t0;
      if (attempt === 1) throw new RateLimitError("slow down", { status: 429, retryAfter: 500 });
      return "a";
    });
    const b = q.add(() => void (started.b = Date.now() - t0));
    await vi.advanceTimersByTimeAsync(2000);
    await Promise.all([a, b]);
    expect(started.a1).toBe(0);
    expect(started.a2).toBe(500);
    expect(started.b).toBeGreaterThanOrEqual(500);
  });

  it("times out a task, aborts its signal, and can retry after a timeout", async () => {
    const q = new Queue({ timeout: 100, retry: { retries: 1, minDelay: 50, jitter: false } });
    let aborted = 0;
    const t0 = Date.now();
    const times: number[] = [];
    const p = q.add(({ signal }) => {
      times.push(Date.now() - t0);
      signal.addEventListener("abort", () => aborted++);
      return new Promise<never>(() => {});
    });
    const caught = p.catch((e) => e);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await caught).toBeInstanceOf(TimeoutError);
    expect(aborted).toBe(2);
    expect(times).toEqual([0, 150]);
  });

  it("rejects tasks that wait longer than maxWait", async () => {
    const q = new Queue({ concurrency: 1 });
    void q.add(() => new Promise<void>((r) => setTimeout(r, 1000)));
    const waiting = q.add(() => "late", { maxWait: 200 }).catch((e) => e);
    await vi.advanceTimersByTimeAsync(300);
    expect(await waiting).toBeInstanceOf(WaitTimeoutError);
    expect(q.stats().dropped).toBe(1);
    await vi.advanceTimersByTimeAsync(1000);
  });

  it("cooldown() pauses new work for a while", async () => {
    const q = new Queue();
    q.cooldown(400);
    const t0 = Date.now();
    let at = -1;
    const p = q.add(() => void (at = Date.now() - t0));
    await vi.advanceTimersByTimeAsync(1000);
    await p;
    expect(at).toBe(400);
  });
});

describe("abort signals", () => {
  it("aborting a waiting task rejects it and it never runs", async () => {
    const q = new Queue({ concurrency: 1 });
    void q.add(() => sleep(20));
    const ctl = new AbortController();
    let ran = false;
    const p = q.add(() => void (ran = true), { signal: ctl.signal }).catch((e) => e);
    ctl.abort(new Error("stop"));
    expect(((await p) as Error).message).toBe("stop");
    await q.onIdle();
    expect(ran).toBe(false);
    expect(q.stats().aborted).toBe(1);
  });

  it("aborting a running task fires the task's signal and is not retried", async () => {
    const q = new Queue({ retry: 3 });
    const ctl = new AbortController();
    let calls = 0;
    const p = q.add(({ signal }) => {
      calls++;
      return new Promise<never>((_, rej) => signal.addEventListener("abort", () => rej(signal.reason)));
    }, { signal: ctl.signal }).catch((e) => e);
    await tick();
    ctl.abort(new Error("user cancelled"));
    expect(((await p) as Error).message).toBe("user cancelled");
    expect(calls).toBe(1);
  });

  it("an already-aborted signal rejects immediately", async () => {
    const q = new Queue();
    const ctl = new AbortController();
    ctl.abort();
    await expect(q.add(() => 1, { signal: ctl.signal })).rejects.toBeDefined();
    expect(q.size).toBe(0);
  });

  it("abortAll clears waiting tasks and aborts running ones", async () => {
    const q = new Queue({ concurrency: 1 });
    const running = q.add(({ signal }) => new Promise<never>((_, rej) => signal.addEventListener("abort", () => rej(signal.reason)))).catch((e) => e);
    const waiting = q.add(() => 1).catch((e) => e);
    await tick();
    q.abortAll(new Error("shutdown"));
    expect(((await running) as Error).message).toBe("shutdown");
    expect(await waiting).toBeInstanceOf(QueueClearedError);
    await q.onIdle();
  });
});

describe("dedupe and overflow", () => {
  it("shares one run between callers using the same key while it is pending", async () => {
    const q = new Queue({ concurrency: 1 });
    let runs = 0;
    const task = async () => {
      runs++;
      await sleep(5);
      return runs;
    };
    const [a, b, c] = await Promise.all([q.add(task, { key: "k" }), q.add(task, { key: "k" }), q.add(task, { key: "other" })]);
    expect(a).toBe(b);
    expect(runs).toBe(2);
    expect(c).toBe(2);
    await q.add(task, { key: "k" });
    expect(runs).toBe(3);
  });

  it("maxSize rejects new tasks by default", async () => {
    const q = new Queue({ concurrency: 1, maxSize: 1 });
    void q.add(() => sleep(10));
    void q.add(() => 1);
    await expect(q.add(() => 2)).rejects.toBeInstanceOf(QueueFullError);
    await q.onIdle();
  });

  it("drop-lowest evicts a lower-priority task for a higher-priority one, otherwise rejects", async () => {
    const q = new Queue({ concurrency: 1, maxSize: 2, overflow: "drop-lowest", autoStart: false });
    const low = q.add(() => "low", { priority: 1 }).catch((e) => e);
    const mid = q.add(() => "mid", { priority: 2 });
    const high = q.add(() => "high", { priority: 9 });
    expect(await low).toBeInstanceOf(QueueFullError);
    await expect(q.add(() => "tiny", { priority: 0 })).rejects.toBeInstanceOf(QueueFullError);
    q.start();
    expect(await Promise.all([mid, high])).toEqual(["mid", "high"]);
  });
});

describe("events and stats", () => {
  it("emits add, active, completed, retry and error", async () => {
    const q = new Queue({ retry: { retries: 1, minDelay: 1, jitter: false } });
    const seen: string[] = [];
    for (const e of ["add", "active", "completed", "retry", "error"] as const) q.on(e, () => seen.push(e));
    await q.add(({ attempt }) => { if (attempt === 1) throw new Error("x"); });
    await q.add(() => { throw new Error("final"); }).catch(() => {});
    expect(seen).toContain("retry");
    expect(seen.filter((s) => s === "add")).toHaveLength(2);
    expect(seen.filter((s) => s === "completed")).toHaveLength(1);
    expect(seen.filter((s) => s === "error")).toHaveLength(1);
  });

  it("a throwing listener cannot break the queue, and unsubscribe works", async () => {
    const q = new Queue();
    const off = q.on("completed", () => { throw new Error("listener bug"); });
    expect(await q.add(() => "fine")).toBe("fine");
    off();
  });

  it("reports counts and timings", async () => {
    const q = new Queue({ concurrency: 1 });
    await q.addAll([() => sleep(5), () => sleep(5)]);
    await q.add(() => { throw new Error("x"); }).catch(() => {});
    const s = q.stats();
    expect(s.added).toBe(3);
    expect(s.completed).toBe(2);
    expect(s.failed).toBe(1);
    expect(s.avgRunMs).toBeGreaterThan(0);
    expect(s.maxWaitMs).toBeGreaterThanOrEqual(0);
  });
});

describe("http helpers", () => {
  it("parseRetryAfter handles seconds, dates, and junk", () => {
    expect(parseRetryAfter("2")).toBe(2000);
    expect(parseRetryAfter("1.5")).toBe(1500);
    expect(parseRetryAfter(3)).toBe(3000);
    const now = Date.parse("2026-01-01T00:00:00Z");
    expect(parseRetryAfter("Thu, 01 Jan 2026 00:00:10 GMT", now)).toBe(10_000);
    expect(parseRetryAfter("Wed, 31 Dec 2025 23:59:00 GMT", now)).toBe(0);
    expect(parseRetryAfter("soon")).toBeUndefined();
    expect(parseRetryAfter(null)).toBeUndefined();
    expect(parseRetryAfter("")).toBeUndefined();
  });

  it("checkResponse passes good responses and throws RateLimitError on 429/503", () => {
    const ok = new Response("fine", { status: 200 });
    expect(checkResponse(ok)).toBe(ok);
    const limited = new Response("no", { status: 429, headers: { "retry-after": "3" } });
    try {
      checkResponse(limited);
      throw new Error("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(RateLimitError);
      expect((e as RateLimitError).retryAfter).toBe(3000);
      expect((e as RateLimitError).status).toBe(429);
    }
    expect(() => checkResponse(new Response("busy", { status: 503 }))).toThrow(RateLimitError);
    expect(checkResponse(new Response("nf", { status: 404 })).status).toBe(404);
  });
});

describe("stress", () => {
  it("1,000 mixed tasks with priorities, groups and random failures all settle within every limit", async () => {
    let seed = 12345;
    const rand = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 2 ** 32);
    const q = new Queue({ concurrency: 7, groups: { a: { concurrency: 2 }, b: { concurrency: 3 } }, retry: { retries: 2, minDelay: 1, maxDelay: 2, jitter: false } });
    let active = 0;
    let maxActive = 0;
    const groupActive: Record<string, number> = { a: 0, b: 0 };
    const groupMax: Record<string, number> = { a: 0, b: 0 };
    const results = await Promise.allSettled(
      Array.from({ length: 1000 }, (_, i) => {
        const group = i % 3 === 0 ? "a" : i % 3 === 1 ? "b" : undefined;
        return q.add(async () => {
          active++;
          maxActive = Math.max(maxActive, active);
          if (group) {
            groupActive[group]++;
            groupMax[group] = Math.max(groupMax[group], groupActive[group]);
          }
          await Promise.resolve();
          if (rand() < 0.01) await sleep(1);
          active--;
          if (group) groupActive[group]--;
          if (rand() < 0.05) throw new Error("random failure");
          return i;
        }, { group, priority: Math.floor(rand() * 5) });
      }),
    );
    expect(maxActive).toBeLessThanOrEqual(7);
    expect(groupMax.a).toBeLessThanOrEqual(2);
    expect(groupMax.b).toBeLessThanOrEqual(3);
    expect(results).toHaveLength(1000);
    expect(results.filter((r) => r.status === "fulfilled").length).toBeGreaterThan(950);
    await q.onIdle();
    expect(q.size).toBe(0);
    expect(q.pending).toBe(0);
  });
});
