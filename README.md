# queuelane

[![npm](https://img.shields.io/npm/v/queuelane)](https://www.npmjs.com/package/queuelane)
[![CI](https://github.com/KrishnaGulalia/queuelane/actions/workflows/ci.yml/badge.svg)](https://github.com/KrishnaGulalia/queuelane/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/queuelane)](LICENSE)
![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)

**An async task queue for rate-limited APIs and heavy jobs. No Redis, no dependencies.**

Run thousands of async tasks without getting throttled, banned or overwhelmed. `queuelane` combines concurrency limits, rolling rate limits, per-API lanes, retries with backoff and automatic cooldown on `429`/`503` in one small, typed library.

```ts
import { Queue, checkResponse } from "queuelane";

const queue = new Queue({
  concurrency: 4,                      // at most 4 running at once
  rate: { limit: 60, per: 60_000 },    // and at most 60 starts per minute
  timeout: 30_000,                     // each attempt gets 30 s
  retry: { retries: 3 },               // retry failures with exponential backoff
});

const results = await queue.map(urls, async (url, i, { signal }) => {
  const res = checkResponse(await fetch(url, { signal })); // 429/503 => backs off automatically
  return res.json();
});
```

- **Zero dependencies.** Only `setTimeout`, `Promise` and `AbortController`.
- **Runs anywhere:** Node 20+, Bun, Deno, edge runtimes and browsers.
- **ESM and CommonJS**, with TypeScript types included.
- **Fast:** about 2x faster than `p-queue` on plain tasks and about 9x faster with priorities (see [Performance](#performance)).

## Contents

- [Install](#install)
- [Why queuelane](#why-queuelane)
- [Feature map](#feature-map)
- [Usage](#usage)
- [Recipes](#recipes)
- [Architecture](#architecture)
- [Performance](#performance)
- [queuelane vs p-queue](#queuelane-vs-p-queue)
- [Errors](#errors)
- [Good to know](#good-to-know)
- [Development](#development)

## Install

```bash
npm i queuelane
```

```ts
import { Queue } from "queuelane";          // ESM
const { Queue } = require("queuelane");     // CommonJS
```

## Why queuelane

Calling an API in a loop with `Promise.all` gets you rate-limited. A plain concurrency limiter doesn't know about requests per minute, retries, or the `Retry-After` header the server just sent you. Most people end up gluing together a limiter, a retry helper and a timeout wrapper, and the pieces don't know about each other.

`queuelane` keeps all of that in one scheduler, so they cooperate:

- A retry waits for the rate-limit window instead of bursting past it.
- A `429` with `Retry-After` pauses the whole lane, not just the failing task.
- A slow API never blocks work for a different API.
- A timeout frees the slot immediately instead of leaving it stuck.

## Feature map

| You want | Option |
|---|---|
| Max N at once | `concurrency` |
| Max N per time window | `rate: { limit, per }` |
| Different limits for different APIs | `groups: { openai: { concurrency: 2, rate: {...} }, ... }` + `add(fn, { group })` |
| Important work first | `add(fn, { priority })` (higher first, FIFO within a priority) |
| Retry flaky calls | `retry: { retries, minDelay, maxDelay, factor, jitter, shouldRetry }` |
| Give up on hung calls | `timeout` (per attempt), `maxWait` (time spent waiting in the queue) |
| Cancel work | `add(fn, { signal })`, `queue.clear()`, `queue.abortAll()` |
| Back off when throttled | throw `RateLimitError` / use `checkResponse(res)` |
| Skip duplicate work | `add(fn, { key })` shares one run between callers |
| Don't let the queue grow forever | `maxSize` + `overflow: "reject" \| "drop-lowest"` |
| Know what happened | `queue.on("retry" \| "error" \| ...)`, `queue.stats()` |

## Usage

### Adding tasks

```ts
const value = await queue.add(async ({ signal, attempt }) => doWork(signal), {
  priority: 10,        // default 0
  group: "openai",     // lane with its own limits
  key: "user:42",      // dedupe while this key is waiting or running
  timeout: 5_000,      // overrides the queue default
  retry: 2,            // shorthand for { retries: 2 }
  maxWait: 60_000,     // reject if it can't start within a minute
  signal,              // an AbortSignal you control
});

await queue.addAll([taskA, taskB]);               // results in order
await queue.map(items, async (item, index) => …); // results keep input order
```

Every task receives `{ signal, attempt, id, group, key }`. Pass `signal` to `fetch` so timeouts and aborts actually cancel the request.

### Lanes (per-group limits)

```ts
const queue = new Queue({
  concurrency: 10,
  groups: {
    openai: { concurrency: 2, rate: { limit: 60, per: 60_000 } },
    stripe: { concurrency: 5 },
  },
});

queue.add(callOpenAI, { group: "openai" });
queue.add(callStripe, { group: "stripe" });
queue.add(somethingElse); // only the global limits apply
```

A slow lane never blocks the others: tasks in other lanes keep running while one lane waits on its limit. Use `groupDefaults` to give every group you didn't list the same limits.

### Retries and rate-limit cooldown

Retries are off by default. When enabled, delays grow as `minDelay * factor^(attempt-1)` up to `maxDelay`, with jitter by default.

If a task fails with a numeric `retryAfter` (milliseconds), the queue also **pauses new work for that long** (for that lane, or everything if the task has no group) and uses it as the retry delay.

```ts
import { RateLimitError, checkResponse } from "queuelane";

// Option 1: let checkResponse do it for fetch (429 and 503, honours the Retry-After header)
const res = checkResponse(await fetch(url));

// Option 2: throw it yourself from any SDK error
throw new RateLimitError("slow down", { status: 429, retryAfter: 2000 });

// Option 3: control it directly
queue.cooldown(5000);
queue.cooldown(5000, "openai");
```

Errors with `retryable: false` are never retried, and you can add your own check with `shouldRetry(error, attempt)`.

### Pausing, draining and backpressure

```ts
queue.pause(); queue.start();
await queue.onIdle();            // nothing waiting or running
await queue.onEmpty();           // nothing waiting
await queue.onSizeLessThan(100); // producer-side backpressure
queue.clear();                   // reject everything that hasn't started (QueueClearedError)
queue.abortAll(reason);          // clear + abort running tasks' signals
queue.setConcurrency(8);         // change on the fly
```

### Events and stats

```ts
queue.on("retry", ({ id, attempt, delayMs, error }) => log(/* ... */));
// add, active, completed, retry, error, dropped, cooldown, empty, idle

queue.stats();
// { added, completed, failed, aborted, dropped, retried, avgWaitMs, maxWaitMs, avgRunMs, maxRunMs }
```

Listeners that throw are ignored so they can't break the queue.

## Recipes

### Bulk-call an LLM API without getting throttled

```ts
import { Queue, checkResponse } from "queuelane";

const queue = new Queue({
  concurrency: 2,
  rate: { limit: 15, per: 60_000 },
  timeout: 60_000,
  retry: { retries: 4, minDelay: 1_000, maxDelay: 30_000 },
});

const answers = await queue.map(prompts, async (prompt, _i, { signal }) => {
  const res = checkResponse(
    await fetch(API_URL, { method: "POST", body: JSON.stringify({ prompt }), signal })
  );
  return res.json();
});
```

### Several APIs, one queue

```ts
const queue = new Queue({
  concurrency: 10,
  groups: {
    gemini: { concurrency: 2, rate: { limit: 15, per: 60_000 } },
    github: { concurrency: 5, rate: { limit: 5_000, per: 3_600_000 } },
  },
});

await Promise.all([
  queue.map(prompts, askGemini, { group: "gemini" }),
  queue.map(repos, fetchRepo, { group: "github" }),
]);
```

### Urgent work jumps the line

```ts
queue.add(backgroundSync, { priority: 0 });
queue.add(userClickedSave, { priority: 100 }); // starts before any waiting priority-0 task
```

### Stop a producer from outrunning the queue

```ts
const queue = new Queue({ concurrency: 5 });

for await (const row of hugeStream) {
  await queue.onSizeLessThan(1_000); // wait here if 1,000 tasks are already waiting
  void queue.add(() => process(row));
}
await queue.onIdle();
```

### Don't do the same work twice

```ts
// Both callers share one run, and both get the same result.
const [a, b] = await Promise.all([
  queue.add(() => fetchUser(42), { key: "user:42" }),
  queue.add(() => fetchUser(42), { key: "user:42" }),
]);
```

## Architecture

```
                        queue.add(fn, { priority, group, key, ... })
                                          │
                                  ┌───────▼────────┐
                                  │  dedupe by key │  same key waiting/running → share one run
                                  └───────┬────────┘
                                          │
                                  ┌───────▼────────┐
                                  │ maxSize check  │  reject, or drop the lowest priority
                                  └───────┬────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
      ┌───────▼───────┐           ┌───────▼───────┐           ┌───────▼───────┐
      │  lane: openai │           │  lane: stripe │           │ default lane  │
      │  binary heap  │           │  binary heap  │           │  binary heap  │
      │  own limits   │           │  own limits   │           │  global only  │
      └───────┬───────┘           └───────┬───────┘           └───────┬───────┘
              └───────────────────────────┼───────────────────────────┘
                                          │
                              ┌───────────▼────────────┐
                              │       scheduler        │
                              │  for each lane, can it │
                              │  start the next task?  │
                              │   • global concurrency │
                              │   • lane concurrency   │
                              │   • rolling rate limit │
                              │   • cooldown / paused  │
                              └───────────┬────────────┘
                                          │ start
                              ┌───────────▼────────────┐
                              │      task runner       │
                              │  AbortSignal + timeout │
                              └───────────┬────────────┘
                              ok ─────────┴───────── error
                               │                       │
                          resolve()        retryable? ─┴─ no → reject()
                                                │ yes
                                  backoff delay (+ Retry-After)
                                  and lane cooldown if throttled
                                                │
                                         back into its lane
```

**Design decisions**

| Piece | How it works | Why |
|---|---|---|
| **Waiting tasks** | One binary heap per lane, ordered by priority, then insertion order | Inserting and removing stays cheap with long queues, and equal priorities stay FIFO |
| **Lanes** | Each group has its own queue, concurrency limit and rate limit, on top of the global limits | A throttled API only slows its own lane |
| **Rate limit** | Rolling window of recent start times, on by default (not fixed buckets) | No burst at the window boundary, with nothing to opt into. In a test, starts 6 and 7 waited exactly until the first two starts left the 10 s window |
| **Scheduler** | Scans lanes, not individual tasks, and starts work as soon as a slot frees | No idle gaps: in a test, task 8 started the moment a slot freed at 11.4 s |
| **Retries** | Exponential backoff with jitter, via a `retryAfter` hint when the server gives one | Retries don't stampede, and they respect what the server told you |
| **Cooldown** | A task failing with `retryAfter` pauses its lane (or the whole queue) | One `429` protects every task behind it, not just the one that failed |
| **Timeouts** | Per attempt, and a timeout frees the slot at once | A hung request can't hold a concurrency slot forever |
| **Runtime surface** | Only `setTimeout`, `Promise` and `AbortController` | It runs on Node, Bun, Deno, edge runtimes and browsers with no polyfills |
| **Time** | The scheduler uses an injectable clock in tests | The retry, backoff and rate-limit logic is tested deterministically, without real waiting |

## Performance

100,000 no-op tasks, concurrency 10, best of 5 on Node 22. Two full runs gave the same picture. The p-queue side was the latest release at the time of the run (installed with `npm i --no-save p-queue`). Reproduce with `npm i --no-save p-queue && node bench/bench.mjs`, and check your version with `npm ls p-queue`.

| Scenario | p-queue | queuelane |
|---|---|---|
| Plain tasks | 512–586 ms | **235 ms** |
| 10 priority levels | 3,189–3,232 ms | **352–380 ms** |

**Why it's faster.** Waiting tasks live in a binary heap, so a priority insert costs O(log n) no matter how long the queue is. `p-queue` kept slower on the priority case in this benchmark, which is what you'd expect from keeping a sorted list with inserts in the middle.

**Where it slows down.** Every group is its own lane, and each scheduling step scans the lanes:

| 100,000 tasks spread over | queuelane |
|---|---|
| 3 groups | 262–300 ms |
| 100 groups | 363–392 ms |
| 1,000 groups | 1,602–1,743 ms |

Dozens or hundreds of groups are fine. If you want a group per user or per tenant with thousands of them, the scan cost starts to show.

Real workloads are dominated by what your tasks do (network, CPU). Read these numbers as "the queue won't be your bottleneck", not as a reason to switch by itself.

## queuelane vs p-queue

[`p-queue`](https://github.com/sindresorhus/p-queue) is the established general-purpose choice. `queuelane` is aimed at the case where the thing you're queueing calls rate-limited APIs.

| | p-queue | queuelane |
|---|---|---|
| Concurrency and priorities | ✅ | ✅ |
| Per-task timeout and `AbortSignal` | ✅ | ✅ |
| Rate limiting | ✅ fixed window by default, sliding window with `strict: true` | ✅ rolling window by default |
| Backpressure (`onSizeLessThan`) | ✅ | ✅ |
| Per-API lanes with their own limits | ❌ | ✅ |
| Built-in retries with backoff | ❌ (pair it with `p-retry`) | ✅ |
| Cooldown from `Retry-After` / 429 | ❌ | ✅ |
| Dedupe by key | ❌ | ✅ |
| Hard cap on queue size (`maxSize`, overflow policy) | ❌ | ✅ |
| Pluggable queue implementation (`queueClass`) | ✅ | ❌ |
| Module formats | ESM only | ESM and CommonJS |
| Priority speed on long queues | slower in the benchmark above | faster in the benchmark above |

*Checked against p-queue's README and type definitions (v9.x) on 23 Nov 2026. p-queue describes itself as feature complete, so its feature set is stable.*

## Errors

`TimeoutError`, `WaitTimeoutError`, `QueueFullError`, `QueueClearedError`, `RateLimitError`; all but `RateLimitError` extend `QueueError`. A task aborted through its own `signal` rejects with that signal's reason.

## Good to know

- **Timeouts free the slot immediately.** If your task ignores `signal`, it may keep running in the background after a timeout. Pass `signal` to `fetch` and other cancellable APIs.
- **Dedupe shares a result, not a lifetime.** A caller's `signal` on a deduped `add` does not cancel the shared run.
- **It's in-memory.** There is no persistence; the queue disappears with the process. That's the point (no Redis), but don't use it for work that must survive a crash.
- **Scheduling cost.** Each scheduling step scans the lanes (one per group), not individual tasks, so thousands of waiting tasks are fine; thousands of *different groups* is not what it's for.

## Development

```bash
git clone https://github.com/KrishnaGulalia/queuelane.git
cd queuelane
npm install
npm test            # unit tests, including fake-clock tests for retry and rate limiting
npm run build       # builds ESM + CJS into dist/
```

CI runs on Node 20, 22 and 24.

## License

MIT