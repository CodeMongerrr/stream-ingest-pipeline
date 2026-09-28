# Changelog

All notable changes to this project are documented here. The format follows Keep a Changelog and the project uses Semantic Versioning.

## [1.0.0](https://github.com/CodeMongerrr/stream-ingest-pipeline/releases/tag/v1.0.0) - 2026-09-29

### Fixed
- Acknowledge stream entries only after InfluxDB accepts the batch, so a crash can no longer drop acknowledged records
- Create the consumer group from the start of the stream so entries written before the first processor start are delivered
- Replay pending entries with a one second backoff after a failed write
- Give blocking pops their own Redis connection so an empty queue cannot stall the limiter, scheduler and stream writes
- Recover cron ticks that node-cron skips when its poll drifts past second zero
- Let exactly one fetcher replica enqueue each minute
- Set `imagePullPolicy` to `IfNotPresent` so locally loaded images start in kind and minikube

### Added
- `STREAM_MAXLEN` to bound the stream
- `USE_MOCK` toggle in Docker Compose
- GitHub Actions CI for install, typecheck, build and Docker build
- MIT license

### Changed
- Node 22 base images
- Dependency updates from `npm audit fix`
- Compose publishes InfluxDB and metrics on localhost only and no longer publishes Redis
- README rewritten around the pipeline, with weather described as the demo source

### Security
- Cleared all high severity npm advisories in the fetcher

## [0.1.0](https://github.com/CodeMongerrr/stream-ingest-pipeline/tree/07dc417) - 2026-02-23

### Added
- Fetcher with a cron scheduler, 50 async workers and an atomic Redis Lua token bucket at 8 requests per second with a cooldown after a 429
- Processor reading a Redis Stream through a consumer group and writing to InfluxDB with source timestamps
- Prometheus metrics and a health endpoint
- Docker Compose stack and Kubernetes manifests
