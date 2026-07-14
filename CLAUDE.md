# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                        # debug build
cargo build --release              # release build
cargo test                         # run all tests
cargo test partitioning            # run tests in a specific module
cargo test partitioning::tests::test_name  # run a single test
cargo run -- --help                # show CLI usage
cargo clippy -- -D warnings        # lint
cargo fmt                          # format
```

Run from a JSONL file (no Kafka needed):
```bash
cargo run -- --from-file messages.jsonl --partitions 32 --top 10
```

Run against a live Kafka topic:
```bash
cargo run -- --topic my-topic --brokers localhost:9092 --max-messages 50000
```

Every CLI flag has a matching `KAFKA_*` env var (e.g. `KAFKA_TOPIC`, `KAFKA_BROKERS`). OTLP export activates automatically when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Architecture

The tool samples OTLP metrics traffic from Kafka (or a JSONL dump) and recommends the best Kafka message key for balanced partition distribution.

**Data flow:**

1. **Input** — `reader.rs` reads JSONL from disk; `consumer.rs` consumes from Kafka using `rdkafka`. Multiple parallel worker tasks each own their own `SchemaCounts` with no shared mutable state; `max_messages` budget is split evenly across workers.

2. **Parsing** — `otlp_parser.rs` deserializes each line as an OTLP `ExportMetricsServiceRequest` and extracts resource attribute maps (one per `ResourceMetrics` block). Invalid UTF-8 and malformed JSON are silently skipped.

3. **Analysis** — `analyzer.rs` (`SchemaCounts`) groups messages by *attribute schema* (sorted pipe-delimited attribute key names, e.g. `host.name|service.name`) and counts unique key value tuples per schema. Worker results are merged with `SchemaCounts::merge_all`.

4. **Partitioning simulation** — `partitioning.rs` encodes each unique value tuple as compact JSON (sorted by key, matching Python's `json.dumps(..., separators=(",",":"))`) then runs Kafka's Murmur2 hash to assign it to a partition. `simulate_counted_distribution` produces per-partition message counts and std_dev — the primary ranking metric.

5. **Reporting** — `reporter.rs` ranks candidate schemas by message volume (top-N), simulates distribution for each, picks the recommendation by lowest std_dev (tie-broken by max partition count), and outputs a table, JSON, and a timestamped CSV.

6. **Telemetry** — `telemetry.rs` wires up `tracing-subscriber` (always) and OTLP traces + metrics (when `OTEL_EXPORTER_OTLP_ENDPOINT` is set). The `TelemetryGuard` returned from `init()` must live for the duration of `main` — it flushes providers on drop.

**Key design constraint:** the Murmur2 implementation in `partitioning.rs` deliberately matches the Python `kafka-python` library's algorithm (including its remainder cascade). Don't "fix" it to a cleaner implementation — the hash must produce identical partition assignments.
