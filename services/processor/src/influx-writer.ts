import { InfluxDB, WriteApi, Point } from "@influxdata/influxdb-client";

export interface WeatherPoint {
  city_name:         string;
  latitude:          number;
  longitude:         number;
  temperature:       number;
  weather_condition: string;
  recorded_at:       string;
}

const INFLUX_URL    = process.env.INFLUX_URL    ?? "http://localhost:8086";
const INFLUX_TOKEN  = process.env.INFLUX_TOKEN  ?? "my-super-secret-token";
const INFLUX_ORG    = process.env.INFLUX_ORG    ?? "weather_org";
const INFLUX_BUCKET = process.env.INFLUX_BUCKET ?? "weather_bucket";

export class InfluxWriter {
  private readonly writeApi: WriteApi;

  constructor() {
    const client = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
    // The consumer calls flush() after each stream batch and XACKs only when it resolves,
    // so the client neither flushes on a timer nor keeps its own retry buffer. The Redis
    // pending list is the single retry queue.
    this.writeApi = client.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, "ms", {
      flushInterval: 0,
      batchSize: 100,
      maxRetries: 0,
    });
    console.log(`[influx] connected → ${INFLUX_URL} | org: ${INFLUX_ORG} | bucket: ${INFLUX_BUCKET}`);
  }

  write(point: WeatherPoint): void {
    const p = new Point("weather")
      .tag("city_name",         point.city_name)
      .tag("weather_condition", point.weather_condition)
      .floatField("temperature", point.temperature)
      .floatField("latitude",    point.latitude)
      .floatField("longitude",   point.longitude)
      .timestamp(Date.parse(point.recorded_at));

    this.writeApi.writePoint(p);
  }

  /** Sends every buffered point now. Resolves once InfluxDB accepted them, rejects if the write failed. */
  async flush(): Promise<void> {
    await this.writeApi.flush();
  }

  async close(): Promise<void> {
    await this.writeApi.close();
    console.log("[influx] connection closed");
  }
}
