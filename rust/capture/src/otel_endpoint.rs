use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Json;
use axum_client_ip::InsecureClientIp;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use common_types::CapturedEvent;
use flate2::read::GzDecoder;
use metrics::counter;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;
use prost::Message;
use serde_json::{json, Value};
use std::io::Read;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::api::{CaptureError, CaptureResponse, CaptureResponseCode};
use crate::extractors::extract_body_with_timeout;
use crate::prometheus::report_dropped_events;
use crate::router::State as AppState;
use crate::token::validate_token;
use crate::v0_request::{DataType, ProcessedEvent, ProcessedEventMetadata};

/// Patch empty `{}` objects in OTEL JSON that should be `null` for proper deserialization.
/// See https://github.com/open-telemetry/opentelemetry-rust/issues/1253
fn patch_otel_json(v: &mut Value) {
    match v {
        Value::Object(map) => {
            if let Some(inner) = map.get_mut("value") {
                if inner.is_object() && inner.as_object().map(|obj| obj.is_empty()).unwrap_or(false)
                {
                    *inner = Value::Null;
                }
            }
            for (_, val) in map.iter_mut() {
                patch_otel_json(val);
            }
        }
        Value::Array(arr) => {
            for val in arr.iter_mut() {
                patch_otel_json(val);
            }
        }
        _ => {}
    }
}

/// Count spans in an ExportTraceServiceRequest
fn count_spans(request: &ExportTraceServiceRequest) -> usize {
    request
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum()
}

/// Convert OTel request to clean JSON using serde serialization + post-processing
fn request_to_clean_json(request: &ExportTraceServiceRequest) -> Value {
    let mut json = serde_json::to_value(request).expect("OTel types are serializable");
    clean_otel_json(&mut json);
    json
}

/// Recursively clean OTel JSON:
/// - Convert byte arrays to hex strings (per OTLP spec)
/// - Unwrap AnyValue typed wrappers
/// - Convert attributes array to flat object
fn clean_otel_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            // Handle AnyValue unwrapping: {"stringValue": "x"} → "x"
            if let Some(inner) = unwrap_any_value(map) {
                *value = inner;
                return;
            }

            // Handle attributes: [{"key": "k", "value": {...}}] → {"k": v}
            if let Some(attrs) = map.get("attributes") {
                if is_key_value_array(attrs) {
                    let flattened = flatten_attributes(attrs);
                    map.insert("attributes".to_string(), flattened);
                }
            }

            // Handle byte fields (traceId, spanId, parentSpanId) - hex encode per OTLP spec
            for key in ["traceId", "spanId", "parentSpanId"] {
                if let Some(arr) = map.get(key) {
                    if let Some(bytes) = as_byte_array(arr) {
                        map.insert(key.to_string(), Value::String(hex::encode(&bytes)));
                    }
                }
            }

            // Recurse into all values
            for (_, v) in map.iter_mut() {
                clean_otel_json(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                clean_otel_json(v);
            }
        }
        _ => {}
    }
}

/// Check if this is an AnyValue wrapper and extract the inner value
fn unwrap_any_value(map: &serde_json::Map<String, Value>) -> Option<Value> {
    if map.len() == 1 {
        if let Some(v) = map.get("stringValue") {
            return Some(v.clone());
        }
        if let Some(v) = map.get("boolValue") {
            return Some(v.clone());
        }
        if let Some(v) = map.get("intValue") {
            return Some(v.clone());
        }
        if let Some(v) = map.get("doubleValue") {
            return Some(v.clone());
        }
        if let Some(v) = map.get("bytesValue") {
            if let Some(bytes) = as_byte_array(v) {
                return Some(Value::String(hex::encode(&bytes)));
            }
        }
        // arrayValue needs recursive handling
        if let Some(v) = map.get("arrayValue") {
            if let Some(arr) = v.get("values") {
                let mut clean_arr = arr.clone();
                clean_otel_json(&mut clean_arr);
                return Some(clean_arr);
            }
        }
        // kvlistValue needs to be flattened
        if let Some(v) = map.get("kvlistValue") {
            if let Some(values) = v.get("values") {
                return Some(flatten_attributes(values));
            }
        }
    }
    None
}

/// Check if a value is a key-value array (OTel attributes format)
fn is_key_value_array(v: &Value) -> bool {
    matches!(v, Value::Array(arr) if arr.first()
        .and_then(|v| v.as_object())
        .is_some_and(|o| o.contains_key("key") && o.contains_key("value")))
}

/// Flatten OTel attributes array to a simple object
fn flatten_attributes(v: &Value) -> Value {
    let mut map = serde_json::Map::new();
    if let Value::Array(arr) = v {
        for item in arr {
            if let (Some(key), Some(value)) = (
                item.get("key").and_then(|k| k.as_str()),
                item.get("value"),
            ) {
                let mut clean_value = value.clone();
                clean_otel_json(&mut clean_value);
                map.insert(key.to_string(), clean_value);
            }
        }
    }
    Value::Object(map)
}

/// Try to interpret a JSON value as a byte array
fn as_byte_array(v: &Value) -> Option<Vec<u8>> {
    v.as_array().map(|arr| {
        arr.iter()
            .filter_map(|n| n.as_u64().map(|n| n as u8))
            .collect()
    })
}

fn decompress_gzip(compressed: &Bytes) -> Result<Bytes, CaptureError> {
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).map_err(|e| {
        warn!("Failed to decompress gzip body: {}", e);
        CaptureError::RequestDecodingError(format!("Failed to decompress gzip body: {e}"))
    })?;
    Ok(Bytes::from(decompressed))
}

/// Extract distinct_id from OTel resource attributes
fn extract_distinct_id(request: &ExportTraceServiceRequest) -> String {
    for rs in &request.resource_spans {
        if let Some(resource) = &rs.resource {
            // Try posthog.distinct_id first
            for attr in &resource.attributes {
                if attr.key == "posthog.distinct_id" {
                    if let Some(value) = &attr.value {
                        if let Some(any_value::Value::StringValue(s)) = &value.value {
                            if !s.is_empty() {
                                return s.clone();
                            }
                        }
                    }
                }
            }
            // Then try user.id
            for attr in &resource.attributes {
                if attr.key == "user.id" {
                    if let Some(value) = &attr.value {
                        if let Some(any_value::Value::StringValue(s)) = &value.value {
                            if !s.is_empty() {
                                return s.clone();
                            }
                        }
                    }
                }
            }
        }
    }
    // Fallback to random UUID
    Uuid::new_v4().to_string()
}

struct SpanEvent {
    event_name: String,
    distinct_id: String,
    properties: Value,
    timestamp: Option<DateTime<Utc>>,
}

fn get_event_name(operation_name: Option<&str>) -> &'static str {
    match operation_name {
        Some("chat") => "$ai_generation",
        Some("embeddings") => "$ai_embedding",
        _ => "$ai_span",
    }
}

fn nanos_to_datetime(nanos: &Value) -> Option<DateTime<Utc>> {
    let n: i64 = match nanos {
        Value::String(s) => s.parse().ok()?,
        Value::Number(n) => n.as_i64()?,
        _ => return None,
    };
    let secs = n / 1_000_000_000;
    let nsecs = (n % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single()
}

fn extract_span_events(clean_json: &Value, distinct_id: &str) -> Vec<SpanEvent> {
    let mut events = Vec::new();

    let resource_spans = match clean_json.get("resourceSpans").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return events,
    };

    for rs in resource_spans {
        let resource_attrs = rs
            .pointer("/resource/attributes")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        let scope_spans = match rs.get("scopeSpans").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => continue,
        };

        for ss in scope_spans {
            let spans = match ss.get("spans").and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };

            for span in spans {
                let attrs = span
                    .get("attributes")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();

                let operation_name = attrs
                    .get("gen_ai.operation.name")
                    .and_then(|v| v.as_str());
                let event_name = get_event_name(operation_name).to_string();

                let mut properties = serde_json::Map::new();

                // Structural trace properties
                if let Some(trace_id) = span.get("traceId") {
                    properties.insert("$ai_trace_id".to_string(), trace_id.clone());
                }
                if let Some(span_id) = span.get("spanId") {
                    properties.insert("$ai_span_id".to_string(), span_id.clone());
                }
                if let Some(parent_id) = span.get("parentSpanId") {
                    if !parent_id.is_null()
                        && parent_id.as_str().map_or(true, |s| !s.is_empty())
                    {
                        properties.insert("$ai_parent_id".to_string(), parent_id.clone());
                    }
                }
                properties.insert(
                    "$ai_ingestion_source".to_string(),
                    Value::String("otel".to_string()),
                );

                // Span attributes (flat keys from cleaned JSON)
                for (key, value) in &attrs {
                    if !properties.contains_key(key.as_str()) {
                        properties.insert(key.clone(), value.clone());
                    }
                }

                // Resource attributes (don't overwrite span attributes)
                for (key, value) in &resource_attrs {
                    if !properties.contains_key(key.as_str()) {
                        properties.insert(key.clone(), value.clone());
                    }
                }

                let timestamp = span
                    .get("startTimeUnixNano")
                    .and_then(|v| nanos_to_datetime(v));

                events.push(SpanEvent {
                    event_name,
                    distinct_id: distinct_id.to_string(),
                    properties: Value::Object(properties),
                    timestamp,
                });
            }
        }
    }

    events
}

pub async fn otel_handler(
    State(state): State<AppState>,
    ip: Option<InsecureClientIp>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<serde_json::Value>, CaptureError> {
    debug!("Received request to /i/v0/llma_otel endpoint");

    // Extract body with timed streaming
    let body_limit = 4 * 1024 * 1024; // 4MB
    let body = extract_body_with_timeout(
        body,
        body_limit,
        state.body_chunk_read_timeout,
        state.body_read_chunk_size_kb,
        "/i/v0/llma_otel",
    )
    .await?;

    if body.is_empty() {
        warn!("OTEL endpoint received empty body");
        return Err(CaptureError::EmptyPayload);
    }

    // Decompress gzip if needed
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let body = if content_encoding.eq_ignore_ascii_case("gzip") {
        decompress_gzip(&body)?
    } else {
        body
    };

    // Check content type - must be protobuf or JSON
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let is_protobuf = content_type.starts_with("application/x-protobuf");
    let is_json = content_type.starts_with("application/json");

    if !is_protobuf && !is_json {
        warn!(
            "OTEL endpoint received unsupported content type: {}",
            content_type
        );
        return Err(CaptureError::RequestDecodingError(
            "Content-Type must be application/x-protobuf or application/json".to_string(),
        ));
    }

    // Extract and validate bearer token
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !auth_header.starts_with("Bearer ") {
        warn!("OTEL endpoint missing or invalid Authorization header");
        return Err(CaptureError::NoTokenError);
    }

    let token = &auth_header[7..]; // Remove "Bearer " prefix
    validate_token(token)?;

    if state.token_dropper.should_drop(token, "") {
        report_dropped_events("token_dropper", 1);
        return Ok(Json(serde_json::json!({})));
    }

    // TODO: Add quota limiter check (needs HasEventName impl for OTel events)
    // TODO: Add event restriction checks

    // Parse the request based on content type
    let request: ExportTraceServiceRequest = if is_protobuf {
        // Decode protobuf directly
        ExportTraceServiceRequest::decode(&body[..]).map_err(|e| {
            warn!("Failed to decode OTEL protobuf: {}", e);
            CaptureError::RequestParsingError(format!("Invalid protobuf: {e}"))
        })?
    } else {
        // Parse JSON with patching for empty AnyValue bug
        let mut json_value: Value = serde_json::from_slice(&body).map_err(|e| {
            warn!("Failed to parse OTEL JSON: {}", e);
            CaptureError::RequestParsingError(format!("Invalid JSON: {e}"))
        })?;

        patch_otel_json(&mut json_value);

        serde_json::from_value(json_value).map_err(|e| {
            warn!("Failed to parse OTEL trace request: {}", e);
            CaptureError::RequestParsingError(format!("Invalid OTLP trace format: {e}"))
        })?
    };

    let span_count = count_spans(&request);
    counter!("capture_otel_llma_spans_received").increment(span_count as u64);

    let received_at = Utc::now();

    // Extract distinct_id from OTel attributes
    let distinct_id = extract_distinct_id(&request);

    let client_ip = ip
        .map(|InsecureClientIp(addr)| addr.to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let clean_json = request_to_clean_json(&request);
    let span_events = extract_span_events(&clean_json, &distinct_id);

    let processed_events: Vec<ProcessedEvent> = span_events
        .into_iter()
        .map(|span_event| {
            let event_data = json!({
                "event": &span_event.event_name,
                "distinct_id": &span_event.distinct_id,
                "properties": span_event.properties,
            });

            let data = serde_json::to_string(&event_data).expect("SpanEvent is serializable");

            let event_uuid = Uuid::now_v7();
            let captured_event = CapturedEvent {
                uuid: event_uuid,
                distinct_id: span_event.distinct_id,
                session_id: None,
                ip: client_ip.clone(),
                data,
                now: received_at.to_rfc3339(),
                sent_at: None,
                token: token.to_string(),
                event: span_event.event_name.clone(),
                timestamp: span_event.timestamp.unwrap_or(received_at),
                is_cookieless_mode: false,
                historical_migration: false,
            };

            let metadata = ProcessedEventMetadata {
                data_type: DataType::AnalyticsMain,
                session_id: None,
                computed_timestamp: Some(span_event.timestamp.unwrap_or(received_at)),
                event_name: span_event.event_name,
                force_overflow: false,
                skip_person_processing: false,
                redirect_to_dlq: false,
            };

            ProcessedEvent {
                event: captured_event,
                metadata,
            }
        })
        .collect();

    state
        .sink
        .send_batch(processed_events)
        .await
        .map_err(|e| {
            warn!("Failed to send OTel events to Kafka: {:?}", e);
            e
        })?;

    counter!("capture_otel_llma_requests_success").increment(1);

    debug!(
        "OTEL endpoint request processed successfully: {} spans",
        span_count
    );

    // Return empty JSON object per OTLP spec
    Ok(Json(serde_json::json!({})))
}

pub async fn options() -> Result<CaptureResponse, CaptureError> {
    Ok(CaptureResponse {
        status: CaptureResponseCode::Ok,
        quota_limited: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use serde_json::json;

    #[test]
    fn test_patch_otel_json_empty_value() {
        let mut v = json!({
            "value": {}
        });
        patch_otel_json(&mut v);
        assert_eq!(v["value"], Value::Null);
    }

    #[test]
    fn test_patch_otel_json_nested() {
        let mut v = json!({
            "attributes": [
                {"key": "test", "value": {}},
                {"key": "other", "value": {"stringValue": "hello"}}
            ]
        });
        patch_otel_json(&mut v);
        assert_eq!(v["attributes"][0]["value"], Value::Null);
        assert_eq!(v["attributes"][1]["value"]["stringValue"], "hello");
    }

    #[test]
    fn test_patch_otel_json_deeply_nested() {
        let mut v = json!({
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{
                        "attributes": [
                            {"key": "empty", "value": {}},
                            {"key": "string", "value": {"stringValue": "test"}}
                        ]
                    }]
                }]
            }]
        });
        patch_otel_json(&mut v);
        assert_eq!(
            v["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"],
            Value::Null
        );
    }

    #[test]
    fn test_count_spans() {
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let request = ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: None,
                    scope_spans: vec![
                        ScopeSpans {
                            scope: None,
                            spans: vec![Span::default(), Span::default()],
                            schema_url: String::new(),
                        },
                        ScopeSpans {
                            scope: None,
                            spans: vec![Span::default()],
                            schema_url: String::new(),
                        },
                    ],
                    schema_url: String::new(),
                },
                ResourceSpans {
                    resource: None,
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span::default(), Span::default()],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
            ],
        };

        assert_eq!(count_spans(&request), 5);
    }

    #[test]
    fn test_clean_otel_json_unwraps_any_values() {
        let mut v = json!({
            "stringValue": "hello"
        });
        clean_otel_json(&mut v);
        assert_eq!(v, json!("hello"));

        let mut v = json!({
            "intValue": 42
        });
        clean_otel_json(&mut v);
        assert_eq!(v, json!(42));

        let mut v = json!({
            "boolValue": true
        });
        clean_otel_json(&mut v);
        assert_eq!(v, json!(true));
    }

    #[test]
    fn test_clean_otel_json_flattens_attributes() {
        let mut v = json!({
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "my-service"}},
                {"key": "count", "value": {"intValue": 5}}
            ]
        });
        clean_otel_json(&mut v);
        assert_eq!(v["attributes"]["service.name"], json!("my-service"));
        assert_eq!(v["attributes"]["count"], json!(5));
    }

    #[test]
    fn test_clean_otel_json_hex_encodes_trace_ids() {
        let mut v = json!({
            "traceId": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "spanId": [1, 2, 3, 4, 5, 6, 7, 8]
        });
        clean_otel_json(&mut v);
        assert_eq!(v["traceId"], json!("0102030405060708090a0b0c0d0e0f10"));
        assert_eq!(v["spanId"], json!("0102030405060708"));
    }

    #[test]
    fn test_extract_distinct_id_posthog_attribute() {
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "posthog.distinct_id".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "user-123".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };

        assert_eq!(extract_distinct_id(&request), "user-123");
    }

    #[test]
    fn test_extract_distinct_id_user_id_fallback() {
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "user.id".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "user-456".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };

        assert_eq!(extract_distinct_id(&request), "user-456");
    }

    #[test]
    fn test_extract_distinct_id_uuid_fallback() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };

        let distinct_id = extract_distinct_id(&request);
        // Should be a valid UUID
        assert!(Uuid::parse_str(&distinct_id).is_ok());
    }
}
