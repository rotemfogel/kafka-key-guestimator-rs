#[cfg(unix)]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;
use opentelemetry::KeyValue;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;

use crate::analyzer::SchemaCounts;
use crate::otlp_parser::parse_resource_attrs;

const POLL_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct ConsumeConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub offset: String,
    /// `None` = unlimited
    pub max_messages: Option<usize>,
    pub idle_timeout: f64,
    pub workers: usize,
    pub progress_interval: usize,
}

/// Consume messages from Kafka with `config.workers` independent tasks.
///
/// Each worker owns its [`SchemaCounts`] with no shared mutable state.
/// The `max_messages` budget is split evenly across workers (remainder to worker 0).
/// Returns one [`SchemaCounts`] per worker; caller merges with [`SchemaCounts::merge_all`].
pub async fn consume(config: ConsumeConfig) -> Result<Vec<SchemaCounts>> {
    let stop = Arc::new(AtomicBool::new(false));

    install_signal_handler(stop.clone());

    tracing::info!(
        topic = %config.topic,
        workers = config.workers,
        max_messages = ?config.max_messages,
        idle_timeout = config.idle_timeout,
        "starting consumption"
    );

    #[cfg(unix)]
    bump_fd_limit(config.workers);

    let budgets = distribute_budget(config.max_messages, config.workers);

    let handles: Vec<_> = budgets
        .into_iter()
        .enumerate()
        .map(|(id, budget)| {
            let cfg = config.clone();
            let stop = stop.clone();
            tokio::spawn(async move { worker(id, cfg, budget, stop).await })
        })
        .collect();

    let mut results = Vec::with_capacity(config.workers);
    for handle in handles {
        results.push(handle.await??);
    }
    Ok(results)
}

/// Spread `total` messages as evenly as possible; worker 0 absorbs any remainder.
fn distribute_budget(max_messages: Option<usize>, workers: usize) -> Vec<Option<usize>> {
    match max_messages {
        None => vec![None; workers],
        Some(total) => {
            let per = total / workers;
            let rem = total % workers;
            (0..workers)
                .map(|i| Some(per + if i < rem { 1 } else { 0 }))
                .collect()
        }
    }
}

fn install_signal_handler(stop: Arc<AtomicBool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        tracing::info!("shutdown signal received");
        stop.store(true, Ordering::Release);
    });
}

#[tracing::instrument(skip(config, stop), fields(worker_id))]
async fn worker(
    worker_id: usize,
    config: ConsumeConfig,
    budget: Option<usize>,
    stop: Arc<AtomicBool>,
) -> Result<SchemaCounts> {
    if budget == Some(0) {
        tracing::debug!(worker_id, "budget is 0, not starting");
        return Ok(SchemaCounts::new());
    }

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", &config.offset)
        .set("enable.auto.commit", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()?;

    consumer.subscribe(&[config.topic.as_str()])?;

    // Metrics — no-op when OTLP is not configured
    let meter = opentelemetry::global::meter("kafka-key-guestimator");
    let processed_ctr = meter
        .u64_counter("kafka_messages_processed_total")
        .with_description("Messages with at least one resource extracted")
        .build();
    let skipped_ctr = meter
        .u64_counter("kafka_messages_skipped_total")
        .with_description("Messages with null payload or failed parse")
        .build();
    let labels = [KeyValue::new("worker_id", worker_id.to_string())];

    let mut analyzer = SchemaCounts::new();
    let mut count: usize = 0;
    let mut last_activity = Instant::now();
    let idle_timeout = Duration::from_secs_f64(config.idle_timeout);

    tracing::info!(worker_id, "started");

    let stream = consumer.stream();
    tokio::pin!(stream);

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }

        match tokio::time::timeout(POLL_TIMEOUT, stream.next()).await {
            Err(_) => {
                // no message within POLL_TIMEOUT — check idle
                if last_activity.elapsed() >= idle_timeout {
                    tracing::info!(worker_id, "idle timeout, stopping");
                    stop.store(true, Ordering::Release);
                    break;
                }
            }
            Ok(None) => break,
            Ok(Some(Err(e))) => {
                return Err(anyhow::anyhow!("worker {worker_id} kafka error: {e}"));
            }
            Ok(Some(Ok(msg))) => {
                last_activity = Instant::now();
                match msg.payload() {
                    None => {
                        tracing::debug!(worker_id, "skipping null payload");
                        skipped_ctr.add(1, &labels);
                    }
                    Some(payload) => {
                        let line = String::from_utf8_lossy(payload);
                        let resources = parse_resource_attrs(&line);
                        if resources.is_empty() {
                            skipped_ctr.add(1, &labels);
                        } else {
                            analyzer.add(resources);
                            processed_ctr.add(1, &labels);
                            count += 1;
                            if count.is_multiple_of(config.progress_interval) {
                                tracing::info!(worker_id, count, "progress");
                            }
                            if budget.is_some_and(|b| count >= b) {
                                tracing::info!(worker_id, count, "budget reached");
                                stop.store(true, Ordering::Release);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    tracing::info!(worker_id, count, "stopped");
    Ok(analyzer)
}

/// Raise the soft fd limit to accommodate `workers` rdkafka clients.
/// Each client opens ~32 fds (sockets, pipes, eventfds); silently no-ops if
/// already sufficient or if the hard limit is too low.
#[cfg(unix)]
fn bump_fd_limit(workers: usize) {
    let needed = ((workers * 32) as u64).max(1024);
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            return;
        }
        if rl.rlim_cur >= needed {
            return;
        }
        rl.rlim_cur = needed.min(rl.rlim_max);
        if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
            tracing::warn!(
                needed,
                current = rl.rlim_cur,
                "could not raise fd limit; if you see 'too many open files', run: ulimit -n {needed}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_none_gives_all_none() {
        assert_eq!(distribute_budget(None, 3), vec![None, None, None]);
    }

    #[test]
    fn budget_evenly_divisible() {
        assert_eq!(
            distribute_budget(Some(12), 4),
            vec![Some(3), Some(3), Some(3), Some(3)]
        );
    }

    #[test]
    fn budget_remainder_distributed_to_first_workers() {
        // 10 / 3 = 3 rem 1: worker 0 gets 4, workers 1–2 get 3
        assert_eq!(
            distribute_budget(Some(10), 3),
            vec![Some(4), Some(3), Some(3)]
        );
    }

    #[test]
    fn budget_total_always_preserved() {
        let total = 101;
        let workers = 7;
        let sum: usize = distribute_budget(Some(total), workers)
            .into_iter()
            .map(|b| b.unwrap())
            .sum();
        assert_eq!(sum, total);
    }

    #[test]
    fn budget_single_worker_gets_all() {
        assert_eq!(distribute_budget(Some(50), 1), vec![Some(50)]);
    }
}
