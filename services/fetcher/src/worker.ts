import Redis from "ioredis";
import { fetchWeather } from "./fetcher";
import { Location } from "./locations";
import { RateLimiter } from "./rate-limiter";

const QUEUE_KEY   = "weather:locations:queue";
const STREAM_KEY  = "weather:raw";
const CYCLE_KEY   = "weather:cycle:id";
const CYCLE_START = "weather:cycle:start_ms";
const NUM_WORKERS = 50;
// Upper bound on stream length. XADD trims the oldest entries past this (approximately),
// so Redis memory stays flat. 100k entries is a few hours of backlog at 8 req/s.
const STREAM_MAXLEN = parseInt(process.env.STREAM_MAXLEN ?? "100000");

// In-memory per-second analytics, keyed by "cycleId:secondOffset"
const analyticsMap = new Map<string, { ok: number; fail: number; timeout: number; latencies: number[] }>();
let activeCycleId = 0;
let cycleStartMs  = 0;

function getBucket(cycleId: number, secondOffset: number) {
  const key = `${cycleId}:${secondOffset}`;
  if (!analyticsMap.has(key)) {
    analyticsMap.set(key, { ok: 0, fail: 0, timeout: 0, latencies: [] });
  }
  return analyticsMap.get(key)!;
}

async function runWorker(id: number, redis: Redis, blocking: Redis, limiter: RateLimiter): Promise<void> {
  console.log(`[worker-${id}] started`);

  while (true) {
    try {
      const item = await blocking.brpop(QUEUE_KEY, 5);
      if (!item) continue;

      const [cycleIdStr, startMsStr] = await redis.mget(CYCLE_KEY, CYCLE_START);
      const cycleId = parseInt(cycleIdStr ?? "0");
      if (cycleId !== activeCycleId) {
        activeCycleId = cycleId;
        cycleStartMs  = parseInt(startMsStr ?? String(Date.now()));
      }

      await limiter.acquire();

      const fetchStart = Date.now();
      const location: Location = JSON.parse(item[1]);
      const result = await fetchWeather(location);
      const latencyMs = Date.now() - fetchStart;

      const secondOffset = Math.floor((fetchStart - cycleStartMs) / 1000);
      const bucket = getBucket(activeCycleId, secondOffset);
      bucket.ok++;
      bucket.latencies.push(latencyMs);

      await redis.xadd(
        STREAM_KEY, "MAXLEN", "~", STREAM_MAXLEN, "*",
        "city_name",         result.city_name,
        "latitude",          String(result.latitude),
        "longitude",         String(result.longitude),
        "temperature",       String(result.temperature),
        "weather_condition", result.weather_condition,
        "recorded_at",       result.recorded_at,
      );

      console.log(`[worker-${id}] ✓ ${result.city_name.padEnd(20)} ${String(result.temperature).padEnd(6)}°C  ${result.weather_condition.padEnd(22)}  [${latencyMs}ms]`);

    } catch (err: any) {
      const secondOffset = Math.floor((Date.now() - cycleStartMs) / 1000);
      const bucket = getBucket(activeCycleId, secondOffset);

      if (err?.response?.status === 429) {
        await limiter.notifyThrottled();
        bucket.fail++;
      } else if (err?.code === "ECONNABORTED" || err?.message?.includes("timeout")) {
        bucket.timeout++;
      } else {
        bucket.fail++;
      }

      console.error(`[worker-${id}] ✗ ${err.message}`);
    }
  }
}

function startAnalyticsReporter(): void {
  let lastReportedSecond = -1;

  setInterval(() => {
    if (activeCycleId === 0 || cycleStartMs === 0) return;

    const currentSecond = Math.floor((Date.now() - cycleStartMs) / 1000);
    if (currentSecond === lastReportedSecond) return;

    const reportSecond = currentSecond - 1;
    if (reportSecond < 0) return;

    const bucket  = analyticsMap.get(`${activeCycleId}:${reportSecond}`);
    const ok      = bucket?.ok      ?? 0;
    const fail    = bucket?.fail    ?? 0;
    const timeout = bucket?.timeout ?? 0;
    const lats    = bucket?.latencies ?? [];
    const avg     = lats.length ? Math.round(lats.reduce((a, b) => a + b, 0) / lats.length) : 0;
    const p99     = lats.length ? lats.sort((a, b) => a - b)[Math.floor(lats.length * 0.99)] : 0;

    let totalOk = 0, totalFail = 0;
    for (let s = 0; s <= reportSecond; s++) {
      const b = analyticsMap.get(`${activeCycleId}:${s}`);
      if (b) { totalOk += b.ok; totalFail += b.fail + b.timeout; }
    }

    const status = (fail > 0 || timeout > 0) ? `\x1b[33m⚠\x1b[0m` : `\x1b[32m✓\x1b[0m`;

    process.stdout.write(
      `${status} Cycle#${activeCycleId} t+${String(reportSecond).padStart(2)}s` +
      `  this_sec: ${String(ok).padStart(3)}✓ ${String(fail).padStart(2)}✗ ${String(timeout).padStart(2)}⏱` +
      `  total: ${String(totalOk).padStart(4)}/${String(totalOk + totalFail).padStart(4)}` +
      `  latency avg:${String(avg).padStart(4)}ms p99:${String(p99).padStart(5)}ms\n`
    );

    lastReportedSecond = currentSecond;
  }, 500);
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function startWorkers(redis: Redis, limiter: RateLimiter): Promise<void> {
  console.log(`[workers] starting ${NUM_WORKERS} workers`);
  startAnalyticsReporter();
  // BRPOP blocks the connection it runs on. The pops get their own connection so the
  // scheduler, the rate limiter and XADD never wait behind an empty-queue BRPOP.
  const blocking = redis.duplicate();
  const workers = Array.from({ length: NUM_WORKERS }, (_, i) => runWorker(i + 1, redis, blocking, limiter));
  await Promise.all(workers);
}
