use std::cmp::Reverse;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::Local;

use crate::partitioning::simulate_counted_distribution;

pub struct Candidate {
    pub rank: usize,
    pub key_repr: String,
    pub message_count: usize,
    pub std_dev: f64,
    pub max_count: usize,
    pub min_count: usize,
    pub partition_counts: Vec<usize>,
}

pub struct Report {
    pub candidates: Vec<Candidate>,
    pub n_partitions: usize,
    pub total_messages: usize,
    pub recommended_index: usize,
}

pub fn build_report(
    schema_counts: HashMap<String, HashMap<Vec<u8>, usize>>,
    top_n: usize,
    n_partitions: usize,
) -> Report {
    let total_messages: usize = schema_counts.values().flat_map(|c| c.values()).sum();

    let mut ranked: Vec<(String, usize)> = schema_counts
        .iter()
        .map(|(s, c)| (s.clone(), c.values().sum()))
        .collect();
    ranked.sort_by_key(|b| Reverse(b.1));
    ranked.truncate(top_n);

    let candidates: Vec<Candidate> = ranked
        .into_iter()
        .enumerate()
        .map(|(i, (schema, msg_count))| {
            let dist = simulate_counted_distribution(&schema_counts[&schema], n_partitions);
            Candidate {
                rank: i + 1,
                key_repr: schema,
                message_count: msg_count,
                std_dev: dist.std_dev,
                max_count: dist.max_count,
                min_count: dist.min_count,
                partition_counts: dist.partition_counts,
            }
        })
        .collect();

    let recommended_index = candidates
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.std_dev
                .partial_cmp(&b.std_dev)
                .unwrap()
                .then(a.max_count.cmp(&b.max_count))
        })
        .map(|(i, _)| i)
        .unwrap_or(0);

    Report {
        candidates,
        n_partitions,
        total_messages,
        recommended_index,
    }
}

pub fn format_table(report: &Report) -> String {
    let rec = &report.candidates[report.recommended_index];
    let mut lines = vec![
        format!("Recommended key: [{}] {}", rec.rank, rec.key_repr),
        format!(
            "  messages={}  std_dev={:.2}  max={}  min={}",
            rec.message_count, rec.std_dev, rec.max_count, rec.min_count
        ),
        String::new(),
        format!(
            "{:>4}  {:<40}  {:>7}  {:>8}  {:>7}  {:>7}  Rec",
            "Rank", "Key", "Count", "StdDev", "Max", "Min"
        ),
        "-".repeat(84),
    ];

    for (i, c) in report.candidates.iter().enumerate() {
        let marker = if i == report.recommended_index {
            "*"
        } else {
            " "
        };
        let key_display: String = c.key_repr.chars().take(40).collect();
        lines.push(format!(
            "{:>4}  {:<40}  {:>7}  {:>8.2}  {:>7}  {:>7}  {}",
            c.rank, key_display, c.message_count, c.std_dev, c.max_count, c.min_count, marker
        ));
    }

    lines.join("\n")
}

pub fn format_json(report: &Report) -> String {
    use serde_json::{json, Map, Value};

    let rec = &report.candidates[report.recommended_index];
    let candidates: Vec<Value> = report
        .candidates
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let per_partition: Map<String, Value> = c
                .partition_counts
                .iter()
                .enumerate()
                .map(|(j, &cnt)| (j.to_string(), Value::from(cnt)))
                .collect();
            json!({
                "rank": c.rank,
                "key_repr": c.key_repr,
                "message_count": c.message_count,
                "std_dev": c.std_dev,
                "max_count": c.max_count,
                "min_count": c.min_count,
                "per_partition_counts": per_partition,
                "recommended": i == report.recommended_index,
            })
        })
        .collect();

    serde_json::to_string_pretty(&json!({
        "total_messages": report.total_messages,
        "n_partitions": report.n_partitions,
        "recommended_key": rec.key_repr,
        "recommended_rank": rec.rank,
        "candidates": candidates,
    }))
    .unwrap()
}

pub fn write_csv(report: &Report, csv_dir: &Path, broker: Option<&str>) -> Result<PathBuf> {
    let ts = Local::now().format("%Y-%m-%d-%H-%M-%S");
    let broker_tag = broker
        .and_then(|b| b.split(':').next())
        .map(|b| format!("-{b}"))
        .unwrap_or_default();
    let path = csv_dir.join(format!("kafka-key-guestimator{broker_tag}-{ts}.csv"));

    let mut header = vec![
        "rank",
        "key_repr",
        "message_count",
        "std_dev",
        "max_count",
        "min_count",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for i in 0..report.n_partitions {
        header.push(format!("partition_{i}"));
    }
    header.push("recommended".to_owned());

    let mut wtr = csv::Writer::from_path(&path)?;
    wtr.write_record(&header)?;

    for (i, c) in report.candidates.iter().enumerate() {
        let mut row = vec![
            c.rank.to_string(),
            c.key_repr.clone(),
            c.message_count.to_string(),
            format!("{:.6}", c.std_dev),
            c.max_count.to_string(),
            c.min_count.to_string(),
        ];
        for j in 0..report.n_partitions {
            row.push(c.partition_counts.get(j).copied().unwrap_or(0).to_string());
        }
        row.push((i == report.recommended_index).to_string());
        wtr.write_record(&row)?;
    }

    wtr.flush()?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Builds a minimal schema_counts: one schema "service.name" with one key ["gateway"] → 10 msgs.
    // encode_key_values sorts by key, serialises values as compact JSON array.
    fn one_schema() -> HashMap<String, HashMap<Vec<u8>, usize>> {
        let mut inner = HashMap::new();
        inner.insert(br#"["gateway"]"#.to_vec(), 10usize);
        let mut outer = HashMap::new();
        outer.insert("service.name".to_string(), inner);
        outer
    }

    #[test]
    fn build_report_single_candidate() {
        let report = build_report(one_schema(), 10, 4);
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.total_messages, 10);
        assert_eq!(report.candidates[0].message_count, 10);
        assert_eq!(report.recommended_index, 0);
    }

    #[test]
    fn build_report_truncates_to_top_n() {
        let mut counts: HashMap<String, HashMap<Vec<u8>, usize>> = HashMap::new();
        for i in 0..5usize {
            let mut inner = HashMap::new();
            inner.insert(format!(r#"["v{i}"]"#).into_bytes(), 10 + i);
            counts.insert(format!("schema{i}"), inner);
        }
        let report = build_report(counts, 3, 4);
        assert_eq!(report.candidates.len(), 3);
        // top 3 by count descending: 14, 13, 12
        assert_eq!(report.candidates[0].message_count, 14);
        assert_eq!(report.candidates[2].message_count, 12);
    }

    #[test]
    fn build_report_recommended_index_in_bounds() {
        let report = build_report(one_schema(), 10, 4);
        assert!(report.recommended_index < report.candidates.len());
    }

    #[test]
    fn build_report_total_messages_sums_all_schemas() {
        let mut counts = one_schema();
        let mut inner2 = HashMap::new();
        inner2.insert(br#"["other"]"#.to_vec(), 5usize);
        counts.insert("host.name".to_string(), inner2);
        let report = build_report(counts, 10, 4);
        assert_eq!(report.total_messages, 15);
    }

    #[test]
    fn format_table_contains_recommendation_marker() {
        let report = build_report(one_schema(), 10, 4);
        let output = format_table(&report);
        assert!(output.contains('*'));
        assert!(output.contains("service.name"));
    }

    #[test]
    fn format_table_starts_with_recommended_key_line() {
        let report = build_report(one_schema(), 10, 4);
        assert!(format_table(&report).starts_with("Recommended key:"));
    }

    #[test]
    fn format_json_is_valid_with_required_fields() {
        let report = build_report(one_schema(), 10, 4);
        let parsed: serde_json::Value = serde_json::from_str(&format_json(&report)).unwrap();
        assert_eq!(parsed["total_messages"], 10);
        assert!(parsed["candidates"].is_array());
        assert!(parsed["recommended_key"].is_string());
        assert!(parsed["n_partitions"].is_number());
    }

    #[test]
    fn format_json_candidate_has_recommended_flag() {
        let report = build_report(one_schema(), 10, 4);
        let parsed: serde_json::Value = serde_json::from_str(&format_json(&report)).unwrap();
        let candidates = parsed["candidates"].as_array().unwrap();
        let rec_count = candidates
            .iter()
            .filter(|c| c["recommended"] == true)
            .count();
        assert_eq!(rec_count, 1);
    }

    #[test]
    fn write_csv_creates_file_with_correct_headers() {
        let dir = std::env::temp_dir();
        let report = build_report(one_schema(), 10, 4);
        let path = write_csv(&report, &dir, None).unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("rank,key_repr,message_count,std_dev,max_count,min_count"));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn write_csv_broker_tag_appears_in_filename() {
        let dir = std::env::temp_dir();
        let report = build_report(one_schema(), 10, 4);
        let path = write_csv(&report, &dir, Some("mybroker:9092")).unwrap();
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("mybroker"));
        std::fs::remove_file(path).ok();
    }
}
