mod analyzer;
mod consumer;
mod otlp_parser;
mod partitioning;
mod reader;
mod reporter;
mod telemetry;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use uuid::Uuid;

use analyzer::SchemaCounts;
use otlp_parser::parse_resource_attrs;
use reporter::{build_report, format_json, format_table, write_csv};

extern crate jemallocator;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;
const PROGRESS_INTERVAL: usize = 1_000;
#[derive(Parser)]
#[command(
    name = "kafka-key-guestimator",
    about = "Identify the optimal Kafka message key by analysing OTLP traffic."
)]
struct Cli {
    /// JSONL file to read instead of a live topic
    #[arg(long, env = "KAFKA_FROM_FILE", group = "input")]
    from_file: Option<PathBuf>,

    /// Kafka topic to consume
    #[arg(long, env = "KAFKA_TOPIC", group = "input")]
    topic: Option<String>,

    #[arg(long, env = "KAFKA_BROKERS")]
    brokers: Option<String>,

    #[arg(long, env = "KAFKA_GROUP_ID")]
    group_id: Option<String>,

    #[arg(long, env = "KAFKA_OFFSET", default_value = "latest")]
    offset: String,

    #[arg(long, env = "KAFKA_MAX_MESSAGES", default_value = "100000")]
    max_messages: usize,

    #[arg(long, env = "KAFKA_IDLE_TIMEOUT", default_value = "60.0")]
    idle_timeout: f64,

    #[arg(long, env = "KAFKA_WORKERS", default_value = "1")]
    workers: usize,

    #[arg(long, env = "KAFKA_TOP", default_value = "10")]
    top: usize,

    #[arg(long, env = "KAFKA_PARTITIONS", default_value = "32")]
    partitions: usize,

    #[arg(long, env = "KAFKA_OUTPUT_JSON")]
    output_json: bool,

    #[arg(long, env = "KAFKA_OUT")]
    out: Option<PathBuf>,

    #[arg(long, env = "KAFKA_CSV_DIR", default_value = ".")]
    csv_dir: PathBuf,

    #[arg(long, env = "KAFKA_LOG_LEVEL", default_value = "INFO")]
    log_level: String,

    #[arg(long, env = "KAFKA_PROGRESS_INTERVAL", default_value_t = PROGRESS_INTERVAL)]
    progress_interval: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Must outlive main — holds OTel providers alive and flushes on drop
    let _guard = telemetry::init(&cli.log_level)?;

    if cli.from_file.is_none() && cli.topic.is_none() {
        eprintln!("Error: supply --from-file FILE or --topic TOPIC (or set KAFKA_TOPIC)");
        std::process::exit(1);
    }
    if cli.topic.is_some() && cli.brokers.is_none() {
        eprintln!("Error: supply --brokers (or set KAFKA_BROKERS) for live Kafka input");
        std::process::exit(1);
    }

    let schema_counts = if let Some(path) = &cli.from_file {
        let mut counts = SchemaCounts::new();
        for line in reader::read_jsonl(path)? {
            counts.add(parse_resource_attrs(&line));
        }
        counts
    } else {
        let group_id = cli
            .group_id
            .clone()
            .unwrap_or_else(|| format!("kafka-key-guestimator-{}", Uuid::new_v4()));

        let config = consumer::ConsumeConfig {
            brokers: cli.brokers.clone().unwrap(),
            topic: cli.topic.clone().unwrap(),
            group_id,
            offset: cli.offset.clone(),
            max_messages: Some(cli.max_messages),
            idle_timeout: cli.idle_timeout,
            workers: cli.workers,
            progress_interval: cli.progress_interval,
        };

        let worker_counts = consumer::consume(config).await?;
        SchemaCounts::merge_all(worker_counts)
    };

    let completed = schema_counts.snapshot();
    if completed.is_empty() {
        eprintln!("Error: no messages parsed successfully");
        std::process::exit(2);
    }

    let report = build_report(completed, cli.top, cli.partitions);
    let output = if cli.output_json {
        format_json(&report)
    } else {
        format_table(&report)
    };

    if let Some(out_path) = &cli.out {
        std::fs::write(out_path, &output)?;
    } else {
        println!("{output}");
    }

    let csv_path = write_csv(&report, &cli.csv_dir, cli.brokers.as_deref())?;
    tracing::info!("CSV written to {}", csv_path.display());

    Ok(())
}
