use std::collections::HashMap;

use crate::partitioning::encode_key_values;

/// One instance per consumer worker — never shared across threads.
/// Combine with [`SchemaCounts::merge_all`] after workers join.
pub struct SchemaCounts {
    counts: HashMap<String, HashMap<Vec<u8>, usize>>,
}

impl SchemaCounts {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    pub fn add(&mut self, resources: Vec<HashMap<String, String>>) {
        for attrs in resources {
            let mut keys: Vec<&str> = attrs.keys().map(String::as_str).collect();
            keys.sort_unstable();
            let schema = keys.join("|");
            let key_bytes = encode_key_values(&attrs);
            *self
                .counts
                .entry(schema)
                .or_default()
                .entry(key_bytes)
                .or_insert(0) += 1;
        }
    }

    pub fn merge_all(parts: Vec<SchemaCounts>) -> SchemaCounts {
        let mut merged = SchemaCounts::new();
        for part in parts {
            for (schema, key_counts) in part.counts {
                let entry = merged.counts.entry(schema).or_default();
                for (key, count) in key_counts {
                    *entry.entry(key).or_insert(0) += count;
                }
            }
        }
        merged
    }

    pub fn snapshot(self) -> HashMap<String, HashMap<Vec<u8>, usize>> {
        self.counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn new_is_empty() {
        assert!(SchemaCounts::new().snapshot().is_empty());
    }

    #[test]
    fn add_creates_schema_entry() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![attrs(&[("service.name", "gw")])]);
        let snap = sc.snapshot();
        assert!(snap.contains_key("service.name"));
        assert_eq!(snap["service.name"].values().sum::<usize>(), 1);
    }

    #[test]
    fn same_key_increments_count() {
        let mut sc = SchemaCounts::new();
        let a = attrs(&[("svc", "gw")]);
        sc.add(vec![a.clone()]);
        sc.add(vec![a]);
        assert_eq!(sc.snapshot()["svc"].values().sum::<usize>(), 2);
    }

    #[test]
    fn different_values_same_schema_are_distinct_keys() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![attrs(&[("svc", "a")])]);
        sc.add(vec![attrs(&[("svc", "b")])]);
        let snap = sc.snapshot();
        assert_eq!(snap["svc"].len(), 2);
        assert_eq!(snap["svc"].values().sum::<usize>(), 2);
    }

    #[test]
    fn different_attribute_sets_produce_different_schemas() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![attrs(&[("a", "1")])]);
        sc.add(vec![attrs(&[("b", "2")])]);
        assert_eq!(sc.snapshot().len(), 2);
    }

    #[test]
    fn schema_key_is_sorted_field_names() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![attrs(&[("z", "1"), ("a", "2"), ("m", "3")])]);
        assert!(sc.snapshot().contains_key("a|m|z"));
    }

    #[test]
    fn add_multiple_resources_per_call() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![attrs(&[("svc", "a")]), attrs(&[("svc", "b")])]);
        assert_eq!(sc.snapshot()["svc"].values().sum::<usize>(), 2);
    }

    #[test]
    fn add_empty_resources_vec_is_noop() {
        let mut sc = SchemaCounts::new();
        sc.add(vec![]);
        assert!(sc.snapshot().is_empty());
    }

    #[test]
    fn merge_all_empty_vec() {
        assert!(SchemaCounts::merge_all(vec![]).snapshot().is_empty());
    }

    #[test]
    fn merge_all_combines_counts_for_same_key() {
        let mut sc1 = SchemaCounts::new();
        sc1.add(vec![attrs(&[("svc", "gw")])]);
        let mut sc2 = SchemaCounts::new();
        sc2.add(vec![attrs(&[("svc", "gw")])]);
        let merged = SchemaCounts::merge_all(vec![sc1, sc2]);
        assert_eq!(merged.snapshot()["svc"].values().sum::<usize>(), 2);
    }

    #[test]
    fn merge_all_combines_different_schemas() {
        let mut sc1 = SchemaCounts::new();
        sc1.add(vec![attrs(&[("a", "1")])]);
        let mut sc2 = SchemaCounts::new();
        sc2.add(vec![attrs(&[("b", "2")])]);
        assert_eq!(SchemaCounts::merge_all(vec![sc1, sc2]).snapshot().len(), 2);
    }
}
