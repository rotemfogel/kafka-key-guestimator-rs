use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportRequest {
    #[serde(default)]
    resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Deserialize)]
struct ResourceMetrics {
    #[serde(default)]
    resource: Resource,
}

#[derive(Deserialize, Default)]
struct Resource {
    #[serde(default)]
    attributes: Vec<KeyValue>,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: serde_json::Value,
}

/// OTLP JSON encodes int64 as a JSON string; everything else is a JSON primitive.
fn any_value_to_string(v: &serde_json::Value) -> Option<String> {
    let obj = v.as_object()?;
    if let Some(s) = obj.get("stringValue").and_then(|v| v.as_str()) {
        return Some(s.to_owned());
    }
    if let Some(b) = obj.get("boolValue").and_then(|v| v.as_bool()) {
        return Some(b.to_string());
    }
    if let Some(i) = obj.get("intValue") {
        // protobuf JSON: int64 serialised as a quoted string
        return Some(match i {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => return None,
        });
    }
    if let Some(d) = obj.get("doubleValue").and_then(|v| v.as_f64()) {
        return Some(d.to_string());
    }
    None
}

/// Extract resource attribute maps from one raw OTLP JSON line.
/// Returns one map per ResourceMetrics block; returns `[]` on any parse error.
pub fn parse_resource_attrs(line: &str) -> Vec<HashMap<String, String>> {
    if line.is_empty() {
        tracing::debug!("skipping empty input");
        return vec![];
    }

    let req: ExportRequest = match serde_json::from_str(line) {
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
                .attributes
                .into_iter()
                .filter_map(|kv| any_value_to_string(&kv.value).map(|v| (kv.key, v)))
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
        let json = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"arr","value":{"arrayValue":{}}}]}}]}"#;
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
