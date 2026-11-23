// Live check: run with  GEMINI_API_KEY=... node examples/gemini.mjs
// Sends 8 real requests through the queue: 2 at a time, at most 5 starts per 10 seconds,
// retrying 429/503 with backoff. Prints when each request started and finished.
import { Queue, checkResponse } from "../dist/index.js";

const key = process.env.GEMINI_API_KEY;
if (!key) throw new Error("Set GEMINI_API_KEY first (export it in your terminal, never paste it anywhere)");
const MODEL = process.env.GEMINI_MODEL ?? "gemini-3.5-flash-lite";

const queue = new Queue({
  concurrency: 2,
  rate: { limit: 5, per: 10_000 },
  timeout: 30_000,
  retry: { retries: 3, minDelay: 1000 },
});

const t0 = Date.now();
const at = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`.padStart(6);
queue.on("active", (e) => console.log(`${at()}  start    #${e.id} (attempt ${e.attempt})`));
queue.on("retry", (e) => console.log(`${at()}  retry    #${e.id} in ${e.delayMs}ms: ${e.error.message}`));
queue.on("cooldown", (e) => console.log(`${at()}  cooldown until +${Math.round((e.until - Date.now()) / 1000)}s`));

const prompts = Array.from({ length: 8 }, (_, i) => `Give me one short fun fact about the number ${i + 1}.`);

const answers = await queue.map(prompts, async (prompt, i, { signal }) => {
  const res = checkResponse(
    await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent`, {
      method: "POST",
      signal,
      headers: { "content-type": "application/json", "x-goog-api-key": key },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }),
    }),
  );
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  const json = await res.json();
  const text = json.candidates?.[0]?.content?.parts?.[0]?.text?.trim() ?? "(no text)";
  console.log(`${at()}  done     #${i + 1}`);
  return text;
});

console.log("\n" + answers.map((a, i) => `${i + 1}. ${a.replace(/\s+/g, " ").slice(0, 90)}`).join("\n"));
console.log("\nstats:", queue.stats());
