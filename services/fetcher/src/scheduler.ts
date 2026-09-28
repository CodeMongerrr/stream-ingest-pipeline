import cron from "node-cron";
import Redis from "ioredis";
import { ALL_LOCATIONS } from "./locations";

const QUEUE_KEY   = "weather:locations:queue";
const CYCLE_KEY   = "weather:cycle:id";
const CYCLE_START = "weather:cycle:start_ms";
const CYCLE_LOCK  = "weather:cycle:lock";

export async function enqueueLocations(redis: Redis): Promise<void> {
  // Every fetcher replica runs this cron. The first replica to claim the current
  // minute enqueues the cycle and the others skip it, so N replicas share one cycle.
  const minute  = Math.floor(Date.now() / 60_000);
  const claimed = await redis.set(`${CYCLE_LOCK}:${minute}`, "1", "EX", 120, "NX");
  if (!claimed) return;

  const cycleId = await redis.incr(CYCLE_KEY);
  const startMs = Date.now();

  const pipeline = redis.pipeline();
  pipeline.set(CYCLE_START, String(startMs));
  pipeline.del(QUEUE_KEY);
  for (const loc of ALL_LOCATIONS) {
    pipeline.lpush(QUEUE_KEY, JSON.stringify(loc));
  }
  await pipeline.exec();

  console.log(`\n${"━".repeat(56)}`);
  console.log(` [scheduler] Cycle #${cycleId} started — ${ALL_LOCATIONS.length} locations enqueued`);
  console.log(`${"━".repeat(56)}\n`);
}

export async function startScheduler(redis: Redis): Promise<void> {
  await enqueueLocations(redis);

  // node-cron 3 polls about once a second and slowly drifts. When a poll jumps over
  // second :00 the tick is silently skipped unless recoverMissedExecutions is set.
  cron.schedule("* * * * *", () => {
    enqueueLocations(redis).catch(err =>
      console.error("[scheduler] enqueue error:", err)
    );
  }, { recoverMissedExecutions: true });

  console.log("[scheduler] started — enqueuing every 60 seconds");
}
