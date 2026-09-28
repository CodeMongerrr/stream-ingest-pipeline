import Redis from "ioredis";
import { startConsumer, WeatherRecord } from "./consumer";
import { InfluxWriter } from "./influx-writer";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

async function main() {
  const redis = new Redis(REDIS_URL);

  redis.on("connect", () => console.log("[redis] connected"));
  redis.on("error",   (err) => console.error("[redis] error:", err));

  // InfluxDB writer. The consumer flushes it after every stream batch and only then XACKs.
  const writer = new InfluxWriter();

  // Graceful shutdown — flush any buffered points before exiting
  const shutdown = async () => {
    console.log("\n[main] shutting down...");
    await writer.close();
    await redis.quit();
    process.exit(0);
  };
  process.on("SIGINT",  shutdown);
  process.on("SIGTERM", shutdown);

  // Read from Redis stream → write to InfluxDB → wait for the write → XACK
  await startConsumer(redis, {
    write(record: WeatherRecord) {
      writer.write(record);
      console.log(
        `[processor] ✓ ${record.city_name.padEnd(22)}` +
        ` ${String(record.temperature).padEnd(6)}°C` +
        `  ${record.weather_condition.padEnd(22)}` +
        `  → influx`
      );
    },
    flush: () => writer.flush(),
  });
}

main().catch(err => {
  console.error("[main] fatal:", err.message);
  process.exit(1);
});
