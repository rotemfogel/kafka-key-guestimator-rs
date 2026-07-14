# kafka-key-guestimator

Samples OTLP metrics traffic from a Kafka topic (or a JSONL dump) and recommends the best Kafka message key for balanced partition distribution.

It works by collecting every distinct set of OTLP resource attribute keys observed in the sample, then simulating how each candidate key would distribute messages across partitions using Kafka's Murmur2 hash. The candidate with the lowest standard deviation across partitions wins.

## Build

```bash
cargo build --release
```

Requires a C toolchain (for `rdkafka`'s bundled librdkafka). The build links libz and zstd statically.

## Usage

**From a JSONL file (no Kafka needed):**
```bash
cargo run --release -- --from-file messages.jsonl --partitions 32 --top 10
```

**Against a live Kafka topic:**
```bash
cargo run --release -- \
  --topic my-topic \
  --brokers localhost:9092 \
  --max-messages 50000 \
  --partitions 32
```

Every flag has a matching `KAFKA_*` environment variable:

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--from-file` | `KAFKA_FROM_FILE` | — | JSONL file instead of live topic |
| `--topic` | `KAFKA_TOPIC` | — | Kafka topic to consume |
| `--brokers` | `KAFKA_BROKERS` | — | Bootstrap brokers |
| `--group-id` | `KAFKA_GROUP_ID` | auto | Consumer group (random UUID if unset) |
| `--offset` | `KAFKA_OFFSET` | `latest` | Start offset (`latest` or `earliest`) |
| `--max-messages` | `KAFKA_MAX_MESSAGES` | `100000` | Messages to sample |
| `--idle-timeout` | `KAFKA_IDLE_TIMEOUT` | `60.0` | Seconds of silence before stopping |
| `--workers` | `KAFKA_WORKERS` | `1` | Parallel consumer tasks |
| `--top` | `KAFKA_TOP` | `10` | Candidate schemas to evaluate |
| `--partitions` | `KAFKA_PARTITIONS` | `32` | Target partition count to simulate |
| `--output-json` | `KAFKA_OUTPUT_JSON` | false | Emit JSON instead of a table |
| `--out` | `KAFKA_OUT` | stdout | Write output to a file |
| `--csv-dir` | `KAFKA_CSV_DIR` | `.` | Directory for the timestamped CSV |
| `--log-level` | `KAFKA_LOG_LEVEL` | `INFO` | Tracing log level |

## Output

Table output (default):
```
Recommended key: [2] host.name|service.name
  messages=84321  std_dev=12.45  max=3102  min=2871

Rank  Key                                       Count    StdDev     Max     Min  Rec
-------------------------------------------------------------------------------------
   1  service.name                              91204     843.21   12043     214
   2  host.name|service.name                    84321      12.45    3102    2871  *
   ...
```

JSON output (`--output-json`):
```json
{
  "total_messages": 100000,
  "n_partitions": 32,
  "recommended_key": "host.name|service.name",
  "recommended_rank": 2,
  "candidates": [...]
}
```

A timestamped CSV is always written to `--csv-dir` with per-partition counts for each candidate.

## Telemetry

OTLP traces and metrics are exported automatically when `OTEL_EXPORTER_OTLP_ENDPOINT` is set:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 cargo run --release -- ...
```

A local observability stack (Jaeger, Prometheus, Grafana, Loki) is available via Docker Compose:

```bash
docker compose up -d
# Grafana: http://localhost:3000
# Jaeger:  http://localhost:16686
# Prometheus: http://localhost:9090
```

## Input format

The JSONL file and Kafka messages must be OTLP `ExportMetricsServiceRequest` JSON objects, one per line. Lines that are blank, whitespace-only, invalid UTF-8, or malformed JSON are silently skipped.

Example line:
```json
{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"gateway"}},{"key":"host.name","value":{"stringValue":"host-01"}}]},"scopeMetrics":[...]}]}
```
