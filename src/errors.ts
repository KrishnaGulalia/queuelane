/** Base class for errors raised by the queue itself. */
export class QueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueueError";
  }
}

/** A task ran longer than its `timeout`. */
export class TimeoutError extends QueueError {
  readonly ms: number;
  constructor(ms: number) {
    super(`Task timed out after ${ms}ms`);
    this.name = "TimeoutError";
    this.ms = ms;
  }
}

/** A task waited in the queue longer than its `maxWait` without starting. */
export class WaitTimeoutError extends QueueError {
  readonly ms: number;
  constructor(ms: number) {
    super(`Task waited more than ${ms}ms in the queue without starting`);
    this.name = "WaitTimeoutError";
    this.ms = ms;
  }
}

/** The queue was full (`maxSize`) and the task was rejected or dropped. */
export class QueueFullError extends QueueError {
  constructor(message = "Queue is full") {
    super(message);
    this.name = "QueueFullError";
  }
}

/** The task was removed by `queue.clear()` before it started. */
export class QueueClearedError extends QueueError {
  constructor() {
    super("Queue was cleared");
    this.name = "QueueClearedError";
  }
}

/**
 * Throw this (or any error with a numeric `retryAfter` in milliseconds) from a task to tell the queue
 * the upstream service is rate limiting you. The queue pauses new work for that long and retries.
 */
export class RateLimitError extends Error {
  readonly status?: number;
  /** Milliseconds to wait before trying again, if the server said so. */
  readonly retryAfter?: number;
  constructor(message: string, details: { status?: number; retryAfter?: number } = {}) {
    super(message);
    this.name = "RateLimitError";
    this.status = details.status;
    this.retryAfter = details.retryAfter;
  }
}
