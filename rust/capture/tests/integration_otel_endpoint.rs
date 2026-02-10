// TODO: Add comprehensive test suite
#[path = "common/integration_utils.rs"]
mod integration_utils;

use async_trait::async_trait;
use axum_test_helper::TestClient;
use capture::ai_s3::MockBlobStorage;
use capture::api::CaptureError;
use capture::config::CaptureMode;
use capture::quota_limiters::CaptureQuotaLimiter;
use capture::router::router;
use capture::sinks::Event;
use capture::time::TimeSource;
use capture::v0_request::{DataType, ProcessedEvent};
use chrono::{DateTime, Utc};
use common_redis::MockRedisClient;
use health::HealthRegistry;
use integration_utils::DEFAULT_TEST_TIME;
use limiters::token_dropper::TokenDropper;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;

#[path = "common/utils.rs"]
mod test_utils;
use test_utils::DEFAULT_CONFIG;

struct FixedTime {
    pub time: DateTime<Utc>,
}

impl TimeSource for FixedTime {
    fn current_time(&self) -> DateTime<Utc> {
        self.time
    }
}

#[derive(Clone)]
struct CapturingSink {
    events: Arc<tokio::sync::Mutex<Vec<ProcessedEvent>>>,
}

impl CapturingSink {
    fn new() -> Self {
        Self {
            events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    async fn get_events(&self) -> Vec<ProcessedEvent> {
        self.events.lock().await.clone()
    }
}

#[async_trait]
impl Event for CapturingSink {
    async fn send(&self, event: ProcessedEvent) -> Result<(), CaptureError> {
        self.events.lock().await.push(event);
        Ok(())
    }

    async fn send_batch(&self, events: Vec<ProcessedEvent>) -> Result<(), CaptureError> {
        self.events.lock().await.extend(events);
        Ok(())
    }
}

fn build_test_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "posthog.distinct_id".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "test-user-123".to_string(),
                        )),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    name: "test-span".to_string(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn test_otel_endpoint_happy_path_protobuf() {
    let sink = CapturingSink::new();
    let liveness = HealthRegistry::new("otel_test");
    let timesource = FixedTime {
        time: DateTime::parse_from_rfc3339(DEFAULT_TEST_TIME)
            .expect("Invalid fixed time format")
            .with_timezone(&Utc),
    };
    let redis = Arc::new(MockRedisClient::new());

    let mut cfg = DEFAULT_CONFIG.clone();
    cfg.capture_mode = CaptureMode::Events;

    let quota_limiter =
        CaptureQuotaLimiter::new(&cfg, redis.clone(), Duration::from_secs(60 * 60 * 24 * 7));

    let app = router(
        timesource,
        liveness,
        sink.clone(),
        redis,
        None,
        quota_limiter,
        TokenDropper::default(),
        None,
        false,
        CaptureMode::Events,
        String::from("capture-otel-test"),
        None,
        25 * 1024 * 1024,
        false,
        1_i64,
        false,
        0.0_f32,
        26_214_400,
        Some(Arc::new(MockBlobStorage::new(
            "test-bucket".to_string(),
            "llma/".to_string(),
        ))),
        Some(10),
        None,
        256,
    );

    let client = TestClient::new(app);

    let request = build_test_request();
    let body = request.encode_to_vec();

    let resp = client
        .post("/i/v0/llma_otel")
        .header("Content-Type", "application/x-protobuf")
        .header(
            "Authorization",
            "Bearer phc_VXRzc3poSG9GZm1JenRianJ6TTJFZGh4OWY2QXzx9f3",
        )
        .body(body)
        .send()
        .await;

    assert_eq!(resp.status(), 200);
    let response_body: serde_json::Value = resp.json().await;
    assert_eq!(response_body, serde_json::json!({}));

    let events = sink.get_events().await;
    assert_eq!(events.len(), 1);

    let event = &events[0];
    assert_eq!(
        event.event.token,
        "phc_VXRzc3poSG9GZm1JenRianJ6TTJFZGh4OWY2QXzx9f3"
    );
    assert_eq!(event.event.event, "$ai_raw_data");
    assert_eq!(event.event.distinct_id, "test-user-123");
    assert_eq!(event.metadata.data_type, DataType::AnalyticsMain);
    assert_eq!(event.metadata.event_name, "$ai_raw_data");
}
