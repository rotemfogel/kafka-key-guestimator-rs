use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Holds OTel providers alive for the process lifetime; flushes on drop.
pub struct TelemetryGuard {
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(mp) = self.meter_provider.take() {
            mp.shutdown().ok();
        }
        if let Some(tp) = self.tracer_provider.take() {
            tp.shutdown().ok();
        }
    }
}

/// Initialise tracing + optional OTLP export.
///
/// OTLP is enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
/// `log_level` accepts Python-style names (DEBUG, INFO, WARNING, ERROR, CRITICAL).
pub fn init(log_level: &str) -> Result<TelemetryGuard> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(python_to_tracing_level(log_level)));

    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_thread_names(true);

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    if let Some(endpoint) = otlp_endpoint {
        let (tracer_provider, meter_provider) = setup_otlp(&endpoint)?;
        let tracer = {
            use opentelemetry::trace::TracerProvider as _;
            tracer_provider.tracer("kafka-key-guestimator")
        };
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        Ok(TelemetryGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
        })
    } else {
        tracing_subscriber::registry().with(filter).with(fmt).init();
        Ok(TelemetryGuard {
            tracer_provider: None,
            meter_provider: None,
        })
    }
}

fn setup_otlp(
    endpoint: &str,
) -> Result<(
    opentelemetry_sdk::trace::TracerProvider,
    opentelemetry_sdk::metrics::SdkMeterProvider,
)> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{metrics::PeriodicReader, runtime, Resource};

    let resource = Resource::new(vec![KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        "kafka-key-guestimator",
    )]);

    // ── Traces ──────────────────────────────────────────────────────────────
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(span_exporter, runtime::Tokio)
        .with_resource(resource.clone())
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // ── Metrics ─────────────────────────────────────────────────────────────
    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let reader = PeriodicReader::builder(metrics_exporter, runtime::Tokio).build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Ok((tracer_provider, meter_provider))
}

fn python_to_tracing_level(level: &str) -> &'static str {
    match level.to_ascii_uppercase().as_str() {
        "DEBUG" => "debug",
        "WARNING" | "WARN" => "warn",
        "ERROR" | "CRITICAL" => "error",
        _ => "info",
    }
}
