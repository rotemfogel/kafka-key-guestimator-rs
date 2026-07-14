use std::collections::HashMap;

pub fn encode_key_values(attrs: &HashMap<String, String>) -> Vec<u8> {
    let mut keys: Vec<&str> = attrs.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let values: Vec<&str> = keys.iter().map(|k| attrs[*k].as_str()).collect();
    // Compact JSON, no spaces — matches Python json.dumps(..., separators=(",",":"))
    serde_json::to_vec(&values).unwrap_or_default()
}

fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_B28C;
    const M: u32 = 0x5BD1_E995;

    let mut h = SEED ^ (data.len() as u32);
    let mut chunks = data.chunks_exact(4);

    for chunk in chunks.by_ref() {
        let mut k = u32::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let rem = chunks.remainder();
    // Mirrors Python's cascade of `if len >= N` (not elif)
    if rem.len() >= 3 {
        h ^= (rem[2] as u32) << 16;
    }
    if rem.len() >= 2 {
        h ^= (rem[1] as u32) << 8;
    }
    if !rem.is_empty() {
        h ^= rem[0] as u32;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

pub fn kafka_partition(key: &[u8], n_partitions: usize) -> usize {
    debug_assert!(n_partitions > 0);
    (murmur2(key) & 0x7FFF_FFFF) as usize % n_partitions
}

pub struct Distribution {
    pub partition_counts: Vec<usize>,
    pub std_dev: f64,
    pub max_count: usize,
    pub min_count: usize,
}

pub fn simulate_counted_distribution(
    key_counts: &HashMap<Vec<u8>, usize>,
    n_partitions: usize,
) -> Distribution {
    assert!(n_partitions > 0, "partitions must be positive");

    let mut partition_counts = vec![0usize; n_partitions];

    for (key, &count) in key_counts {
        if count == 0 {
            continue;
        }
        partition_counts[kafka_partition(key, n_partitions)] += count;
    }

    let n = n_partitions as f64;
    let mean = partition_counts.iter().sum::<usize>() as f64 / n;
    let variance = partition_counts
        .iter()
        .map(|&c| (c as f64 - mean).powi(2))
        .sum::<f64>()
        / n;
    let std_dev = variance.sqrt();
    let max_count = partition_counts.iter().copied().max().unwrap_or(0);
    let min_count = partition_counts.iter().copied().min().unwrap_or(0);

    Distribution {
        partition_counts,
        std_dev,
        max_count,
        min_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_empty() {
        // Regression: empty slice must not panic
        let _ = murmur2(&[]);
    }

    #[test]
    fn kafka_partition_stable() {
        let key = b"service.name=gateway|host.name=prod-1";
        let p = kafka_partition(key, 32);
        assert!(p < 32);
        assert_eq!(p, kafka_partition(key, 32)); // deterministic
    }

    #[test]
    fn distribute_evenly() {
        let attrs: HashMap<String, String> =
            [("b".into(), "2".into()), ("a".into(), "1".into())].into();
        let key = encode_key_values(&attrs);
        // sorted by key: a,b → values: ["1","2"]
        assert_eq!(key, br#"["1","2"]"#);
    }

    #[test]
    fn encode_empty_attrs() {
        let attrs: HashMap<String, String> = HashMap::new();
        assert_eq!(encode_key_values(&attrs), b"[]");
    }

    #[test]
    fn kafka_partition_range_check() {
        let inputs: &[&[u8]] = &[b"key1", b"key2", b"", b"a", b"\x00\xff"];
        for input in inputs {
            let p = kafka_partition(input, 16);
            assert!(p < 16, "partition {p} out of range for key {input:?}");
        }
    }

    #[test]
    fn simulate_total_count_preserved() {
        let mut key_counts = HashMap::new();
        key_counts.insert(b"key_a".to_vec(), 7usize);
        key_counts.insert(b"key_b".to_vec(), 3usize);
        let dist = simulate_counted_distribution(&key_counts, 4);
        assert_eq!(dist.partition_counts.iter().sum::<usize>(), 10);
    }

    #[test]
    fn simulate_zero_count_entries_skipped() {
        let mut key_counts = HashMap::new();
        key_counts.insert(b"active".to_vec(), 5usize);
        key_counts.insert(b"zero".to_vec(), 0usize);
        let dist = simulate_counted_distribution(&key_counts, 4);
        assert_eq!(dist.partition_counts.iter().sum::<usize>(), 5);
    }

    #[test]
    fn simulate_partition_counts_length() {
        let mut key_counts = HashMap::new();
        key_counts.insert(b"k".to_vec(), 1);
        let dist = simulate_counted_distribution(&key_counts, 8);
        assert_eq!(dist.partition_counts.len(), 8);
    }

    #[test]
    fn simulate_max_gte_min_and_nonneg_stddev() {
        let mut key_counts = HashMap::new();
        key_counts.insert(b"a".to_vec(), 100);
        key_counts.insert(b"b".to_vec(), 200);
        let dist = simulate_counted_distribution(&key_counts, 16);
        assert!(dist.max_count >= dist.min_count);
        assert!(dist.std_dev >= 0.0);
    }

    #[test]
    fn simulate_empty_counts() {
        let key_counts: HashMap<Vec<u8>, usize> = HashMap::new();
        let dist = simulate_counted_distribution(&key_counts, 4);
        assert_eq!(dist.std_dev, 0.0);
        assert_eq!(dist.max_count, 0);
        assert_eq!(dist.min_count, 0);
    }
}
