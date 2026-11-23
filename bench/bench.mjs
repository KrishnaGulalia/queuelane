import PQueue from "p-queue";
import { Queue } from "../dist/index.js";

async function time(name, fn) {
  // warm up, then take the best of 5
  await fn();
  let best = Infinity;
  for (let i = 0; i < 5; i++) {
    const t = performance.now();
    await fn();
    best = Math.min(best, performance.now() - t);
  }
  console.log(`${name.padEnd(46)} ${best.toFixed(0).padStart(6)} ms`);
}

const N = 100_000;
const noop = () => {};

await time(`p-queue    ${N} no-op tasks, concurrency 10`, async () => {
  const q = new PQueue({ concurrency: 10 });
  for (let i = 0; i < N; i++) q.add(noop);
  await q.onIdle();
});
await time(`queuelane  ${N} no-op tasks, concurrency 10`, async () => {
  const q = new Queue({ concurrency: 10 });
  for (let i = 0; i < N; i++) void q.add(noop);
  await q.onIdle();
});
await time(`p-queue    ${N} tasks, random priority`, async () => {
  const q = new PQueue({ concurrency: 10 });
  for (let i = 0; i < N; i++) q.add(noop, { priority: i % 10 });
  await q.onIdle();
});
await time(`queuelane  ${N} tasks, random priority`, async () => {
  const q = new Queue({ concurrency: 10 });
  for (let i = 0; i < N; i++) void q.add(noop, { priority: i % 10 });
  await q.onIdle();
});
await time(`queuelane  ${N} tasks, 3 groups with limits`, async () => {
  const q = new Queue({ concurrency: 10, groups: { a: { concurrency: 2 }, b: { concurrency: 3 } } });
  for (let i = 0; i < N; i++) void q.add(noop, { group: i % 3 === 0 ? "a" : i % 3 === 1 ? "b" : undefined });
  await q.onIdle();
});
