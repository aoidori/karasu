/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.exporter.metrics;

import io.karasu.core.collector.ClusterManager;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DecodeErrorKey;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DurationHistogramSnapshot;
import io.karasu.exporter.labels.LabelNormalizer;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.GroupEvaluation;
import io.karasu.protocol.model.PartitionEvaluation;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBuckets;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot.HistogramDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Lock-free Prometheus {@link MultiCollector} streaming Karasu's immutable per-cluster snapshots.
 *
 * <p>Hardening guarantees:
 * <ul>
 *   <li>Per-cluster label resolution errors are isolated so one cluster never breaks the scrape (#12).</li>
 *   <li>Snapshot staleness is verified against wall clock on every scrape (#15).</li>
 *   <li>Incomplete groups (cardinality limit or unresolved topics) omit partial {@code lag_sum} (#03),
 *       and uncommitted partitions never emit synthetic zero lag (#09).</li>
 *   <li>{@code karasu_snapshot_build_duration_seconds} is exposed as a standard Prometheus Histogram (#18).</li>
 * </ul>
 */
public final class KarasuPrometheusCollector implements MultiCollector {

    private static final String[] EMPTY = new String[0];
    private static final String[] STATE_NAME = new String[]{"state"};
    private static final String[] GROUP_NAME = new String[]{"consumer_group"};
    private static final String[] PARTITION_NAMES = new String[]{"consumer_group", "topic", "partition"};
    private static final String[] TOPIC_PART_NAMES = new String[]{"topic", "partition"};
    private static final String[] DECODE_ERR_NAMES = new String[]{"record_type", "reason"};
    private static final String[] RECONCILE_ERR_NAMES = new String[]{"phase"};
    private static final ClusterPhase[] EXPOSED_PHASES = new ClusterPhase[]{
            ClusterPhase.INITIALIZING,
            ClusterPhase.WAITING_OFFSETS_TOPIC,
            ClusterPhase.REPLAYING,
            ClusterPhase.READY,
            ClusterPhase.DEGRADED
    };

    private final ClusterManager clusterManager;
    private final LabelNormalizer labelNormalizer;
    private final ExporterOptions options;
    private final Clock clock;

    public record ExporterOptions(
            String metricPrefix,
            boolean publishStaleOnDegraded,
            boolean requireCompleteWindowForOffset
    ) {
        public ExporterOptions {
            metricPrefix = (metricPrefix == null || metricPrefix.isBlank()) ? "karasu_kafka" : metricPrefix.trim();
        }
    }

    public KarasuPrometheusCollector(ClusterManager clusterManager,
                                     LabelNormalizer labelNormalizer,
                                     ExporterOptions options) {
        this(clusterManager, labelNormalizer, options, Clock.systemUTC());
    }

    public KarasuPrometheusCollector(ClusterManager clusterManager,
                                     LabelNormalizer labelNormalizer,
                                     ExporterOptions options,
                                     Clock clock) {
        this.clusterManager = clusterManager;
        this.labelNormalizer = labelNormalizer;
        this.options = options;
        this.clock = clock;
    }

    @Override
    public MetricSnapshots collect() {
        return collectSnapshots(clusterManager.currentSnapshots());
    }

    public MetricSnapshots collectFiltered(String k8sClusterFilter, String kafkaClusterFilter) {
        List<ClusterMetricsSnapshot> all = clusterManager.currentSnapshots();
        List<ClusterMetricsSnapshot> filtered = new ArrayList<>(all.size());
        for (ClusterMetricsSnapshot snap : all) {
            ClusterTargetKey k = snap.targetKey();
            if (k8sClusterFilter != null && !k8sClusterFilter.isEmpty() && !k8sClusterFilter.equals(k.k8sCluster())) {
                continue;
            }
            if (kafkaClusterFilter != null && !kafkaClusterFilter.isEmpty() && !kafkaClusterFilter.equals(k.kafkaCluster())) {
                continue;
            }
            filtered.add(snap);
        }
        return collectSnapshots(filtered);
    }

    MetricSnapshots collectSnapshots(List<ClusterMetricsSnapshot> snapshots) {
        long nowMs = clock.millis();

        // Compute effective health (enforcing snapshot age freshness at scrape time, #15)
        // and scrape-safe extra labels (#12)
        boolean[] effectiveDataReady = new boolean[snapshots.size()];
        ClusterPhase[] effectivePhases = new ClusterPhase[snapshots.size()];
        List<Map<String, String>> clusterExtras = new ArrayList<>(snapshots.size());
        List<Map<String, String>> allGroupExtrasObserved = new ArrayList<>();

        for (int i = 0; i < snapshots.size(); i++) {
            ClusterMetricsSnapshot snap = snapshots.get(i);
            boolean snapshotStale = isSnapshotStale(snap, nowMs);
            effectiveDataReady[i] = !snapshotStale && snap.health().dataReady();
            effectivePhases[i] = snapshotStale ? ClusterPhase.DEGRADED : snap.health().phase();

            Map<String, String> cExtra = labelNormalizer.resolveClusterExtraLabelsSafe(snap.spec());
            clusterExtras.add(cExtra);
            allGroupExtrasObserved.add(cExtra);

            if (shouldPublishBusinessMetrics(effectiveDataReady[i], effectivePhases[i])) {
                for (GroupEvaluation g : snap.groups()) {
                    Map<String, String> gExtra = labelNormalizer.resolveGroupExtraLabelsSafe(snap.spec(), cExtra, g.group());
                    if (gExtra != cExtra) {
                        allGroupExtrasObserved.add(gExtra);
                    }
                }
            }
        }

        List<String> unifiedClusterExtraKeys = labelNormalizer.computeUnifiedExtraKeys(clusterExtras);
        List<String> unifiedGroupExtraKeys = labelNormalizer.computeUnifiedExtraKeys(allGroupExtrasObserved);

        String p = options.metricPrefix();

        // Business metric builders
        GaugeSnapshot.Builder consumerLagSum = GaugeSnapshot.builder()
                .name(p + "_consumer_lag_sum")
                .help("Sum of all partition current lag values for the consumer group (emitted only when complete).");
        GaugeSnapshot.Builder consumerStatus = GaugeSnapshot.builder()
                .name(p + "_consumer_status")
                .help("Consumer group evaluation status (0=NOTFOUND, 1=OK, 2=WARN, 3=ERR).");
        GaugeSnapshot.Builder partitionLag = GaugeSnapshot.builder()
                .name(p + "_consumer_partition_lag")
                .help("Approximate message lag for a committed consumer group topic-partition.");
        GaugeSnapshot.Builder partitionCurrentOffset = GaugeSnapshot.builder()
                .name(p + "_consumer_current_offset")
                .help("Latest committed offset stored for this consumer group topic-partition.");
        GaugeSnapshot.Builder partitionStatus = GaugeSnapshot.builder()
                .name(p + "_topic_partition_status")
                .help("Partition evaluation status (1=OK, 2=WARN, 4=STOP, 5=STALL, 6=REWIND).");
        GaugeSnapshot.Builder topicPartitionOffset = GaugeSnapshot.builder()
                .name(p + "_topic_partition_offset")
                .help("Latest sampled broker log-end offset for this topic-partition.");

        // Cluster health & capacity metric builders
        GaugeSnapshot.Builder clusterUp = GaugeSnapshot.builder()
                .name("karasu_cluster_up")
                .help("Per-cluster collection quality gate (1=fresh, caught up, reachable, within limits; 0=unhealthy/replaying).");
        GaugeSnapshot.Builder clusterState = GaugeSnapshot.builder()
                .name("karasu_cluster_state")
                .help("One-hot indicator of current per-cluster lifecycle phase.");
        GaugeSnapshot.Builder collectionSuccess = GaugeSnapshot.builder()
                .name("karasu_cluster_collection_success")
                .help("Latest broker head-offset cycle succeeded for all known online partitions (1/0).");
        GaugeSnapshot.Builder collectionLastSuccess = GaugeSnapshot.builder()
                .name("karasu_cluster_collection_last_success_timestamp_seconds")
                .help("Unix timestamp in seconds of the last complete broker head-offset cycle.");
        GaugeSnapshot.Builder offsetsCaughtUp = GaugeSnapshot.builder()
                .name("karasu_cluster_offsets_caught_up")
                .help("Internal __consumer_offsets reader completed replay catch-up without runtime stall (1/0).");
        GaugeSnapshot.Builder offsetsLag = GaugeSnapshot.builder()
                .name("karasu_cluster_offsets_lag")
                .help("Remaining unread records across assigned __consumer_offsets partitions.");
        GaugeSnapshot.Builder trackedGroups = GaugeSnapshot.builder()
                .name("karasu_cluster_tracked_groups")
                .help("Number of consumer groups currently tracked in memory for this cluster.");
        GaugeSnapshot.Builder trackedPartitions = GaugeSnapshot.builder()
                .name("karasu_cluster_tracked_partitions")
                .help("Number of (group, topic, partition) windows currently tracked in memory.");
        GaugeSnapshot.Builder cardinalityLimit = GaugeSnapshot.builder()
                .name("karasu_cluster_cardinality_limit_reached")
                .help("Whether this cluster reached its configured memory guardrail limit (1/0).");
        HistogramSnapshot.Builder snapshotBuildDuration = HistogramSnapshot.builder()
                .name("karasu_snapshot_build_duration_seconds")
                .help("Histogram of background metrics snapshot build durations in seconds.");
        CounterSnapshot.Builder decodeErrors = CounterSnapshot.builder()
                .name("karasu_cluster_decode_errors")
                .help("Total __consumer_offsets record decode failures or unknown key versions.");
        CounterSnapshot.Builder reconcileErrors = CounterSnapshot.builder()
                .name("karasu_cluster_reconcile_errors")
                .help("Total cluster hot-reload validation or swap failures.");

        boolean hasGroupLagSum = false;
        boolean hasGroupStatus = false;
        boolean hasPartitionLag = false;
        boolean hasPartitionOffset = false;
        boolean hasTopicOffset = false;
        boolean hasDecodeErrors = false;
        boolean hasReconcileErrors = false;

        for (int i = 0; i < snapshots.size(); i++) {
            ClusterMetricsSnapshot snap = snapshots.get(i);
            ClusterTargetKey target = snap.targetKey();
            Map<String, String> cExtra = clusterExtras.get(i);
            ClusterHealthSnapshot h = snap.health();
            boolean dataReady = effectiveDataReady[i];
            ClusterPhase phase = effectivePhases[i];

            Labels baseLabels = labelNormalizer.buildLabels(target, EMPTY, EMPTY, unifiedClusterExtraKeys, cExtra);
            clusterUp.dataPoint(gaugePoint(baseLabels, dataReady ? 1.0 : 0.0));
            collectionSuccess.dataPoint(gaugePoint(baseLabels, h.collectionSuccess() ? 1.0 : 0.0));
            collectionLastSuccess.dataPoint(gaugePoint(baseLabels, (double) h.lastCollectionSuccessEpochSec()));
            offsetsCaughtUp.dataPoint(gaugePoint(baseLabels, h.offsetsCaughtUp() ? 1.0 : 0.0));
            offsetsLag.dataPoint(gaugePoint(baseLabels, (double) h.offsetsLagTotal()));
            trackedGroups.dataPoint(gaugePoint(baseLabels, (double) h.trackedGroups()));
            trackedPartitions.dataPoint(gaugePoint(baseLabels, (double) h.trackedPartitions()));
            cardinalityLimit.dataPoint(gaugePoint(baseLabels, h.cardinalityLimitReached() ? 1.0 : 0.0));

            DurationHistogramSnapshot hist = snap.snapshotBuildDurationHistogram() != null
                    ? snap.snapshotBuildDurationHistogram()
                    : DurationHistogramSnapshot.empty();
            snapshotBuildDuration.dataPoint(HistogramDataPointSnapshot.builder()
                    .labels(baseLabels)
                    .classicHistogramBuckets(ClassicHistogramBuckets.of(hist.upperBounds(), hist.bucketCounts()))
                    .sum(hist.sumSeconds())
                    .build());

            for (ClusterPhase pItem : EXPOSED_PHASES) {
                Labels stateLabels = labelNormalizer.buildLabels(
                        target, STATE_NAME, new String[]{pItem.name()}, unifiedClusterExtraKeys, cExtra);
                clusterState.dataPoint(gaugePoint(stateLabels, phase == pItem ? 1.0 : 0.0));
            }

            for (Map.Entry<DecodeErrorKey, Long> err : snap.decodeErrorCounts().entrySet()) {
                Labels errLabels = labelNormalizer.buildLabels(
                        target, DECODE_ERR_NAMES,
                        new String[]{err.getKey().recordType(), err.getKey().reason()},
                        unifiedClusterExtraKeys, cExtra);
                decodeErrors.dataPoint(CounterDataPointSnapshot.builder()
                        .labels(errLabels).value(err.getValue().doubleValue()).build());
                hasDecodeErrors = true;
            }

            for (Map.Entry<String, Long> err : snap.reconcileErrorCounts().entrySet()) {
                Labels errLabels = labelNormalizer.buildLabels(
                        target, RECONCILE_ERR_NAMES,
                        new String[]{err.getKey()},
                        unifiedClusterExtraKeys, cExtra);
                reconcileErrors.dataPoint(CounterDataPointSnapshot.builder()
                        .labels(errLabels).value(err.getValue().doubleValue()).build());
                hasReconcileErrors = true;
            }

            // Quality gate: suppress stale/incomplete business lag metrics when cluster is unhealthy (#03, #15)
            if (!shouldPublishBusinessMetrics(dataReady, phase)) {
                continue;
            }

            for (GroupEvaluation group : snap.groups()) {
                Map<String, String> gExtra = labelNormalizer.resolveGroupExtraLabelsSafe(snap.spec(), cExtra, group.group());
                Labels groupLabels = labelNormalizer.buildLabels(
                        target, GROUP_NAME, new String[]{group.group()}, unifiedGroupExtraKeys, gExtra);

                // Omit partial lag_sum when group is incomplete or has no committed partitions (#03, #09)
                if (group.lagTotalComplete()) {
                    consumerLagSum.dataPoint(gaugePoint(groupLabels, (double) group.totalLag()));
                    hasGroupLagSum = true;
                    consumerStatus.dataPoint(gaugePoint(groupLabels, (double) group.status().code()));
                    hasGroupStatus = true;
                }

                for (PartitionEvaluation part : group.partitions()) {
                    // Never emit synthetic lag=0 for uncommitted partitions (#09)
                    if (!part.hasKnownLag()) {
                        continue;
                    }
                    Labels partLabels = labelNormalizer.buildLabels(
                            target,
                            PARTITION_NAMES,
                            new String[]{group.group(), part.topic(), Integer.toString(part.partition())},
                            unifiedGroupExtraKeys,
                            gExtra);
                    partitionLag.dataPoint(gaugePoint(partLabels, (double) part.currentLag()));
                    hasPartitionLag = true;

                    boolean windowReady = !options.requireCompleteWindowForOffset() || part.complete() == 1.0f;
                    if (windowReady && part.end() != null) {
                        partitionCurrentOffset.dataPoint(gaugePoint(partLabels, (double) part.end().offset()));
                        partitionStatus.dataPoint(gaugePoint(partLabels, (double) part.status().code()));
                        hasPartitionOffset = true;
                    }
                }
            }

            for (Map.Entry<String, long[]> topicEntry : snap.topicPartitionOffsets().entrySet()) {
                String topic = topicEntry.getKey();
                long[] offsets = topicEntry.getValue();
                for (int partIdx = 0; partIdx < offsets.length; partIdx++) {
                    if (offsets[partIdx] < 0L) {
                        continue;
                    }
                    Labels tpLabels = labelNormalizer.buildLabels(
                            target,
                            TOPIC_PART_NAMES,
                            new String[]{topic, Integer.toString(partIdx)},
                            unifiedClusterExtraKeys,
                            cExtra);
                    topicPartitionOffset.dataPoint(gaugePoint(tpLabels, (double) offsets[partIdx]));
                    hasTopicOffset = true;
                }
            }
        }

        MetricSnapshots.Builder out = MetricSnapshots.builder();
        if (!snapshots.isEmpty()) {
            out.metricSnapshot(clusterUp.build());
            out.metricSnapshot(clusterState.build());
            out.metricSnapshot(collectionSuccess.build());
            out.metricSnapshot(collectionLastSuccess.build());
            out.metricSnapshot(offsetsCaughtUp.build());
            out.metricSnapshot(offsetsLag.build());
            out.metricSnapshot(trackedGroups.build());
            out.metricSnapshot(trackedPartitions.build());
            out.metricSnapshot(cardinalityLimit.build());
            out.metricSnapshot(snapshotBuildDuration.build());
        }
        if (hasDecodeErrors) {
            out.metricSnapshot(decodeErrors.build());
        }
        if (hasReconcileErrors) {
            out.metricSnapshot(reconcileErrors.build());
        }
        if (hasGroupLagSum) {
            out.metricSnapshot(consumerLagSum.build());
        }
        if (hasGroupStatus) {
            out.metricSnapshot(consumerStatus.build());
        }
        if (hasPartitionLag) {
            out.metricSnapshot(partitionLag.build());
        }
        if (hasPartitionOffset) {
            out.metricSnapshot(partitionCurrentOffset.build());
            out.metricSnapshot(partitionStatus.build());
        }
        if (hasTopicOffset) {
            out.metricSnapshot(topicPartitionOffset.build());
        }
        return out.build();
    }

    private boolean isSnapshotStale(ClusterMetricsSnapshot snap, long nowMs) {
        if (snap.builtAtEpochMs() <= 0L) {
            return true;
        }
        long maxAgeMs = Math.max(snap.spec().collection().snapshotRefreshSeconds() * 3_000L, 60_000L);
        return (nowMs - snap.builtAtEpochMs()) > maxAgeMs;
    }

    private boolean shouldPublishBusinessMetrics(boolean dataReady, ClusterPhase phase) {
        if (dataReady) {
            return true;
        }
        return options.publishStaleOnDegraded() && phase == ClusterPhase.DEGRADED;
    }

    private static GaugeDataPointSnapshot gaugePoint(Labels labels, double value) {
        return GaugeDataPointSnapshot.builder().labels(labels).value(value).build();
    }
}
