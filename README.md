# Karasu

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.5-brightgreen.svg)](https://spring.io/projects/spring-boot)

**Karasu** is a cloud-native, multi-cluster Apache Kafka consumer lag monitoring service written in **Java 17** and **Spring Boot 3**, inspired by [LinkedIn Burrow](https://github.com/linkedin/Burrow).

It preserves Burrow's core strength—consuming Kafka's internal `__consumer_offsets` topic to capture real commit history and evaluate 7-level sliding-window consumer health (`OK / WARN / ERR / STOP / STALL / REWIND`)—while rebuilding the engine from scratch for modern **Kafka 3.9–4.2 (KRaft)** and cloud-platform multi-cluster operations.

## Key Features

- **Multi-Cluster Fault Isolation**: Monitor multiple independent Kafka clusters (across one or more Kubernetes clusters) inside a single Karasu process. Unreachable brokers, ACL denials, or a not-yet-created `__consumer_offsets` topic never crash or degrade other clusters.
- **Hot-Pluggable Reconfiguration**: Add, hot-patch, Blue-Green swap (including credential/TLS rotation), or remove Kafka clusters without restarting the JVM.
- **Complete Kafka 3.9–4.2 Protocol Decoding**:
  - `OffsetCommitValue` v0–v4 (including flexible v4 unsigned-varint compact strings and tagged `topicId`).
  - Classic `GroupMetadataValue` v0–v4 and **KIP-848** modern consumer groups (keys `3`, `5`, `8` with `TopicId` UUID resolution for active `owner` and `client_id`).
  - Proper handling of **Offset & Group Tombstones** to eliminate stale metrics immediately.
  - Default `isolation.level=read_committed` to filter aborted/uncommitted EOS transactional commits.
- **Quality Gates & Lock-Free Prometheus Exposition**:
  - Per-cluster health and replay catch-up gates (`karasu_cluster_up`, `karasu_cluster_state`, `karasu_cluster_offsets_caught_up`) suppress stale business lag on outages.
  - Primitive-array ring buffers (`long[]`) minimize heap footprint and GC pause overhead.
  - Background immutable snapshots served via a native Prometheus `MultiCollector` (`GET /metrics`) with zero scrape-time locking and zero stale Gauge leaks.
- **Extensible Multi-Dimensional Labels**:
  - Mandatory `k8s_cluster` and `kafka_cluster` labels (with composite target key `(k8s_cluster, kafka_cluster)` preventing cross-K8s name collisions).
  - Three-layer label extensibility: global `common-labels`, per-cluster `labels`, and programmatic `MetricLabelEnricher` SPI with unified OpenMetrics schema normalization.
- **Clean Apache-2.0 Licensing**: Full dependency tree strictly kept to Apache-2.0 / MIT / BSD (Log4j2 instead of Logback, no MPL/GPL dependencies) with built-in CycloneDX SBOM generation.

## Architecture

```text
karasu/
├── karasu-protocol/   # Pure domain models, StatusCode, Kafka __consumer_offsets binary decoder
├── karasu-core/       # ClusterManager, ClusterRuntime, BrokerOffsetCollector, OffsetsTopicIngestor, ClusterStore, LagEvaluator
├── karasu-exporter/   # 3-layer LabelNormalizer, lock-free Prometheus MultiCollector, read-only GET /metrics
└── karasu-app/        # Spring Boot 3 application, application.yml binding, watched config/credential reloader
```

See [docs/design.md](./docs/design.md) for the full architecture and detailed design document.

## Prometheus Metrics

All metrics automatically carry `k8s_cluster`, `kafka_cluster`, plus configured `common-labels`, per-cluster `labels`, and `MetricLabelEnricher` SPI labels.

### Consumer Lag & Evaluation Metrics (gated by `karasu_cluster_up`)

| Metric | Type | Core Labels | Description |
|---|---|---|---|
| `karasu_kafka_consumer_lag_sum` | Gauge | `k8s_cluster`, `kafka_cluster`, `consumer_group` | Total lag across all topics and partitions of the group |
| `karasu_kafka_consumer_status` | Gauge | `k8s_cluster`, `kafka_cluster`, `consumer_group` | Group status (`0=NOTFOUND, 1=OK, 2=WARN, 3=ERR`) |
| `karasu_kafka_consumer_partition_lag` | Gauge | `k8s_cluster`, `kafka_cluster`, `consumer_group`, `topic`, `partition` | Approximate lag per partition |
| `karasu_kafka_consumer_current_offset` | Gauge | `k8s_cluster`, `kafka_cluster`, `consumer_group`, `topic`, `partition` | Latest committed offset per partition |
| `karasu_kafka_topic_partition_status` | Gauge | `k8s_cluster`, `kafka_cluster`, `consumer_group`, `topic`, `partition` | Partition status (`1=OK, 2=WARN, 4=STOP, 5=STALL, 6=REWIND`) |
| `karasu_kafka_topic_partition_offset` | Gauge | `k8s_cluster`, `kafka_cluster`, `topic`, `partition` | Latest sampled broker log-end offset |

### Per-Cluster Health & Capacity Metrics (always exported)

- `karasu_cluster_up`
- `karasu_cluster_state{state="INITIALIZING|WAITING_OFFSETS_TOPIC|REPLAYING|READY|DEGRADED"}`
- `karasu_cluster_collection_success`
- `karasu_cluster_collection_last_success_timestamp_seconds`
- `karasu_cluster_offsets_caught_up`
- `karasu_cluster_offsets_lag`
- `karasu_cluster_tracked_groups` / `karasu_cluster_tracked_partitions`
- `karasu_cluster_cardinality_limit_reached`
- `karasu_cluster_decode_errors_total` / `karasu_cluster_reconcile_errors_total`
- `karasu_snapshot_build_duration_seconds`

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.9+
- Apache Kafka 3.9+ (KRaft)

### Build & Test

```bash
mvn clean test
mvn package -DskipTests
```

Generate CycloneDX SBOM:

```bash
mvn org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom
```

### Run

Create an `application.yml` (see [karasu-app/src/main/resources/application.yml](./karasu-app/src/main/resources/application.yml) for all options):

```yaml
server:
  port: 8000

karasu:
  metrics:
    common-labels:
      env: prod
      region: cn-east-1
  clusters:
    - k8s-cluster: k8s-prod-01
      kafka-cluster: kafka-orders
      bootstrap-servers:
        - localhost:9092
      labels:
        namespace: daas
        tenant: order-team
      auth:
        security-protocol: SASL_PLAINTEXT
        sasl-mechanism: SCRAM-SHA-512
        username: monitor
        password: ${KAFKA_MONITOR_PASSWORD}
```

Start Karasu:

```bash
java -XX:MaxRAMPercentage=65.0 -XX:+UseG1GC \
  -jar karasu-app/target/karasu-app-0.1.0-SNAPSHOT.jar \
  --spring.config.additional-location=file:./application.yml
```

Endpoints (Phase 1 read-only surface):
- `GET http://localhost:8000/metrics` (supports optional `?k8s_cluster=...&kafka_cluster=...`)
- `GET http://localhost:8000/actuator/health/liveness`
- `GET http://localhost:8000/actuator/health/readiness`

## Roadmap / Backlog

- **BL-01**: Read-only diagnostic & Burrow-compatible `/v3/kafka/*` REST APIs on an isolated admin port.
- **BL-02**: Dual-architecture (`linux/amd64`, `linux/arm64`) `jlink` container image & Kafka Helm Chart integration.
- **BL-03**: Kubernetes CRD (`KafkaLagTarget`) dynamic discovery & Secret auto-rotation (`karasu-k8s`).
- **BL-04**: StatefulSet consistent-hash sharding and Kubernetes Lease failover.
- **BL-05**: Share Group (KIP-932) Admin API collector and time-based lag estimation (`lag_seconds`).

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
