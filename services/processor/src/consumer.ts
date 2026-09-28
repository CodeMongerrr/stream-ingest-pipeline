import Redis from "ioredis";

const STREAM_KEY    = "weather:raw";
const GROUP_NAME    = "processor-group";
const CONSUMER_NAME = "processor-1";
const BATCH_SIZE    = 50;
const BLOCK_MS      = 5000;
const RETRY_MS      = 1000;

export interface WeatherRecord {
  id:                string;
  city_name:         string;
  latitude:          number;
  longitude:         number;
  temperature:       number;
  weather_condition: string;
  recorded_at:       string;
}

/** Where records go. write() buffers, flush() resolves only once the sink has durably accepted them. */
export interface RecordSink {
  write(record: WeatherRecord): void;
  flush(): Promise<void>;
}

type StreamReply = [string, [string, string[] | null][]][] | null;

async function ensureGroup(redis: Redis): Promise<void> {
  try {
    // "0" (not "$") so the group also delivers entries the fetcher wrote
    // before the processor started for the first time.
    await redis.xgroup("CREATE", STREAM_KEY, GROUP_NAME, "0", "MKSTREAM");
    console.log(`[consumer] created consumer group "${GROUP_NAME}"`);
  } catch (err: any) {
    if (err.message?.includes("BUSYGROUP")) {
      console.log(`[consumer] consumer group "${GROUP_NAME}" already exists`);
    } else {
      throw err;
    }
  }
}

function parseMessage(id: string, fields: string[]): WeatherRecord {
  const map: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    map[fields[i]] = fields[i + 1];
  }
  return {
    id,
    city_name:         map.city_name         ?? "unknown",
    latitude:          parseFloat(map.latitude   ?? "0"),
    longitude:         parseFloat(map.longitude  ?? "0"),
    temperature:       parseFloat(map.temperature ?? "0"),
    weather_condition: map.weather_condition ?? "unknown",
    recorded_at:       map.recorded_at       ?? new Date().toISOString(),
  };
}

/**
 * Writes one batch, waits until InfluxDB has accepted it, then acknowledges it.
 * Returns false when the write failed, in which case nothing is acknowledged
 * and the whole batch stays in this consumer's pending list.
 */
async function processBatch(
  redis: Redis,
  messages: [string, string[] | null][],
  sink: RecordSink
): Promise<boolean> {
  const done: string[] = [];
  let buffered = 0;

  for (const [id, fields] of messages) {
    // A pending entry whose data was trimmed from the stream comes back with no fields.
    if (!fields) { done.push(id); continue; }

    const record = parseMessage(id, fields);
    if (Number.isNaN(Date.parse(record.recorded_at))) {
      // InfluxDB would reject this point on every retry, so drop it instead of blocking the stream.
      console.error(`[consumer] dropping ${id}, bad recorded_at "${record.recorded_at}"`);
      done.push(id);
      continue;
    }
    sink.write(record);
    done.push(id);
    buffered++;
  }

  if (buffered > 0) {
    try {
      await sink.flush();
    } catch (err: any) {
      console.error(`[consumer] write failed, ${messages.length} messages stay pending: ${err.message}`);
      return false;
    }
  }

  // XACK only after InfluxDB accepted the points. A crash before this line leaves the
  // batch pending, and the replay rewrites the same points (same tags and timestamp).
  if (done.length > 0) await redis.xack(STREAM_KEY, GROUP_NAME, ...done);
  return true;
}

export async function startConsumer(redis: Redis, sink: RecordSink): Promise<void> {
  await ensureGroup(redis);

  // "0" replays this consumer's pending entries, meaning ones delivered before a crash
  // or a failed write and never acknowledged. ">" reads entries never delivered before.
  // Start by draining pending, and go back to it whenever a write fails.
  let cursor: "0" | ">" = "0";
  let recovered = 0;

  while (true) {
    const reply = (cursor === "0"
      ? await redis.xreadgroup("GROUP", GROUP_NAME, CONSUMER_NAME, "COUNT", BATCH_SIZE, "STREAMS", STREAM_KEY, "0")
      : await redis.xreadgroup("GROUP", GROUP_NAME, CONSUMER_NAME, "COUNT", BATCH_SIZE, "BLOCK", BLOCK_MS, "STREAMS", STREAM_KEY, ">")
    ) as StreamReply;

    const messages = reply?.[0]?.[1] ?? [];

    if (messages.length === 0) {
      if (cursor === "0") {
        if (recovered > 0) console.log(`[consumer] recovered ${recovered} pending messages`);
        console.log(`[consumer] listening on stream "${STREAM_KEY}"...`);
        cursor = ">";
        recovered = 0;
      }
      continue;
    }

    const ok = await processBatch(redis, messages, sink);
    if (ok) {
      if (cursor === "0") recovered += messages.length;
    } else {
      cursor = "0";
      await sleep(RETRY_MS);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
