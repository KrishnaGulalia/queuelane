import { RateLimitError } from "./errors";

/** Parse an HTTP `Retry-After` header (seconds or an HTTP date) into milliseconds. */
export function parseRetryAfter(value: string | number | null | undefined, now: number = Date.now()): number | undefined {
  if (value === null || value === undefined || value === "") return undefined;
  if (typeof value === "number") return Number.isFinite(value) && value >= 0 ? value * 1000 : undefined;
  const text = value.trim();
  if (/^\d+(\.\d+)?$/.test(text)) return Number(text) * 1000;
  const date = Date.parse(text);
  if (Number.isNaN(date)) return undefined;
  return Math.max(0, date - now);
}

/**
 * Pass a `fetch` Response through. Throws `RateLimitError` for 429 and 503 so the queue backs off,
 * honouring the `Retry-After` header when the server sent one.
 */
export function checkResponse(res: Response): Response {
  if (res.status === 429 || res.status === 503) {
    throw new RateLimitError(`HTTP ${res.status}`, {
      status: res.status,
      retryAfter: parseRetryAfter(res.headers.get("retry-after")),
    });
  }
  return res;
}
