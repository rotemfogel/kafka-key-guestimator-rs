use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;

fn any_value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::StringValue(s) => Some(s.clone()),
        Value::BoolValue(b) => Some(b.to_string()),
        Value::IntValue(i) => Some(i.to_string()),
        Value::DoubleValue(d) => Some(d.to_string()),
        Value::StringValueStrindex(_)
        | Value::ArrayValue(_)
        | Value::KvlistValue(_)
        | Value::BytesValue(_) => None,
    }
}

/// Extract resource attribute maps from one raw OTLP JSON line.
/// Returns one map per ResourceMetrics block; returns `[]` on any parse error.
pub fn parse_resource_attrs(line: &str) -> Vec<HashMap<String, String>> {
    if line.is_empty() {
        tracing::debug!("skipping empty input");
        return vec![];
    }

    let req: ExportMetricsServiceRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(
                error = %e,
                excerpt = &line[..line.len().min(80)],
                "skipping malformed OTLP line"
            );
            return vec![];
        }
    };

    req.resource_metrics
        .into_iter()
        .map(|rm| {
            rm.resource
                .into_iter()
                .flat_map(|r| r.attributes)
                .filter_map(|kv| {
                    kv.value
                        .and_then(|av| av.value)
                        .and_then(|v| any_value_to_string(&v))
                        .map(|s| (kv.key, s))
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_line_returns_empty() {
        assert!(parse_resource_attrs("").is_empty());
    }

    #[test]
    fn malformed_json_returns_empty() {
        assert!(parse_resource_attrs("{not json}").is_empty());
    }

    #[test]
    fn empty_object_no_resource_metrics() {
        assert_eq!(parse_resource_attrs("{}").len(), 0);
    }

    #[test]
    fn parses_string_value() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"gateway"}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["service.name"], "gateway");
    }

    #[test]
    fn parses_bool_value() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"enabled","value":{"boolValue":true}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result[0]["enabled"], "true");
    }

    #[test]
    fn parses_int_value_as_quoted_string() {
        // protobuf JSON encodes int64 as a JSON string
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"port","value":{"intValue":"8080"}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result[0]["port"], "8080");
    }

    #[test]
    fn parses_int_value_as_number() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"port","value":{"intValue":8080}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result[0]["port"], "8080");
    }

    #[test]
    fn parses_double_value() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"ratio","value":{"doubleValue":1.5}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result[0]["ratio"], "1.5");
    }

    #[test]
    fn multiple_resource_metrics_yields_multiple_maps() {
        let json = r#"{"resourceMetrics":[
            {"resource":{"attributes":[{"key":"a","value":{"stringValue":"1"}}]}},
            {"resource":{"attributes":[{"key":"b","value":{"stringValue":"2"}}]}}
        ]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["a"], "1");
        assert_eq!(result[1]["b"], "2");
    }

    #[test]
    fn unknown_value_type_filtered_out_leaving_empty_map() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"arr","value":{"arrayValue":{"values":[]}}}]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn missing_resource_key_uses_default_empty() {
        let json = r#"{"resourceMetrics":[{}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn multiple_attributes_in_one_resource() {
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[
            {"key":"svc","value":{"stringValue":"api"}},
            {"key":"host","value":{"stringValue":"prod-1"}}
        ]}}]}"#;
        let result = parse_resource_attrs(json);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[0]["svc"], "api");
        assert_eq!(result[0]["host"], "prod-1");
    }
}
