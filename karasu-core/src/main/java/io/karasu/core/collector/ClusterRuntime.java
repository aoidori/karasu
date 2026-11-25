/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.evaluator.LagEvaluator;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DurationHistogramSnapshot;
import io.karasu.core.storage.ClusterStore;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.GroupEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Self-contained, fault-isolated runtime managing collection, storage, evaluation, health gating,
 * histogram tracking, and immutable snapshot publishing for a single {@link ClusterTargetKey}.
 */
public class ClusterRuntime {

    private static final Logger log = LoggerFactory.getLogger(ClusterRuntime.class);

    private final ClusterTargetKey targetKey;
    private final long generation;
    private volatile ClusterSpec spec;
    private final String connectionFingerprint;
    private final ClusterStore store;
    private final LagEvaluator evaluator;
    private final BrokerOffsetCollector brokerCollector;
    private final OffsetsTopicIngestor offsetsIngestor;
    private final ScheduledExecutorService collectionScheduler;
    private final ScheduledExecutorService snapshotScheduler;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicReference<ClusterMetricsSnapshot> latestSnapshot = new AtomicReference<>();
    private final Map<String, AtomicLong> reconcileErrors = new HashMap<>();

    // Cumulative histogram state for karasu_snapshot_build_duration_seconds (#18)
    private final long[] durationBucketCounts = new long[DurationHistogramSnapshot.DEFAULT_BUCKETS.length];
    private double durationSumSeconds = 0.0;

    private volatile ScheduledFuture<?> offsetTask;
    private volatile ScheduledFuture<?> snapshotTask;
    private volatile ScheduledFuture<?> expiryTask;

    public ClusterRuntime(ClusterSpec spec,
                          long generation,
                          String connectionFingerprint,
                          ScheduledExecutorService collectionScheduler,
                          ScheduledExecutorService snapshotScheduler) {
        this.targetKey = spec.targetKey();
        this.generation = generation;
        this.spec = spec;
        this.connectionFingerprint = connectionFingerprint;
        this.collectionScheduler = collectionScheduler;
        this.snapshotScheduler = snapshotScheduler;

        AtomicReference<BrokerOffsetCollector> brokerRef = new AtomicReference<>();
        this.store = new ClusterStore(spec, () -> {
            BrokerOffsetCollector bc = brokerRef.get();
            if (bc != null) {
                bc.requestImmediateMetadataRefresh();
            }
        });
        this.evaluator = new LagEvaluator(spec, store);
        this.brokerCollector = new BrokerOffsetCollector(spec, generation, store, evaluator::invalidateAll);
        this.brokerCollector.attachScheduler(collectionScheduler);
        brokerRef.set(this.brokerCollector);

        this.offsetsIngestor = new OffsetsTopicIngestor(spec, generation, store, brokerCollector, evaluator::invalidateGroup);
        this.latestSnapshot.set(buildSnapshot());
    }

    public ClusterTargetKey targetKey() {
        return targetKey;
    }

    public long generation() {
        return generation;
    }

    public ClusterSpec spec() {
        return spec;
    }

    public String connectionFingerprint() {
        return connectionFingerprint;
    }

    public boolean isCardinalityLimitReached() {
        return store.isCardinalityLimitReached();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        int offsetRefresh = spec.collection().offsetRefreshSeconds();
        this.offsetTask = collectionScheduler.scheduleWithFixedDelay(
                brokerCollector::collectOnce, 0L, offsetRefresh, TimeUnit.SECONDS);

        offsetsIngestor.start();

        int snapRefresh = Math.max(1, spec.collection().snapshotRefreshSeconds());
        this.snapshotTask = snapshotScheduler.scheduleWithFixedDelay(
                this::refreshSnapshotSafe, 1L, snapRefresh, TimeUnit.SECONDS);

        this.expiryTask = snapshotScheduler.scheduleWithFixedDelay(
                this::purgeExpiredSafe, 300L, 300L, TimeUnit.SECONDS);

        log.info("Started ClusterRuntime target={} gen={}", targetKey.canonicalId(), generation);
    }

    /**
     * Applies non-structural policy changes in place without reconnecting or replaying.
     */
    public synchronized void hotPatch(ClusterSpec updatedSpec) {
        if (stopped.get()) {
            return;
        }
        int oldOffsetSec = this.spec.collection().offsetRefreshSeconds();
        int oldSnapSec = this.spec.collection().snapshotRefreshSeconds();
        this.spec = updatedSpec;
        List<String> purgedGroups = this.store.applyPolicy(updatedSpec);
        this.evaluator.applyPolicy(updatedSpec);
        for (String purged : purgedGroups) {
            this.evaluator.invalidateGroup(purged);
        }
        this.brokerCollector.applyPolicy(updatedSpec);

        if (started.get() && oldOffsetSec != updatedSpec.collection().offsetRefreshSeconds() && offsetTask != null) {
            offsetTask.cancel(false);
            offsetTask = collectionScheduler.scheduleWithFixedDelay(
                    brokerCollector::collectOnce,
                    updatedSpec.collection().offsetRefreshSeconds(),
                    updatedSpec.collection().offsetRefreshSeconds(),
                    TimeUnit.SECONDS);
        }
        if (started.get() && oldSnapSec != updatedSpec.collection().snapshotRefreshSeconds() && snapshotTask != null) {
            snapshotTask.cancel(false);
            int snapSec = Math.max(1, updatedSpec.collection().snapshotRefreshSeconds());
            snapshotTask = snapshotScheduler.scheduleWithFixedDelay(
                    this::refreshSnapshotSafe, snapSec, snapSec, TimeUnit.SECONDS);
        }
        refreshSnapshotSafe();
        log.info("Hot-patched ClusterRuntime target={} gen={} (purgedFilteredGroups={})",
                targetKey.canonicalId(), generation, purgedGroups.size());
    }

    public boolean isDataReady() {
        ClusterMetricsSnapshot snap = latestSnapshot.get();
        return snap != null && snap.health().dataReady();
    }

    public ClusterMetricsSnapshot currentSnapshot() {
        return latestSnapshot.get();
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        cancelQuietly(offsetTask);
        cancelQuietly(snapshotTask);
        cancelQuietly(expiryTask);
        offsetsIngestor.stop(Duration.ofSeconds(5));
        brokerCollector.close();
        evaluator.invalidateAll();
        log.info("Stopped ClusterRuntime target={} gen={}", targetKey.canonicalId(), generation);
    }

    void refreshSnapshotSafe() {
        if (stopped.get()) {
            return;
        }
        try {
            latestSnapshot.set(buildSnapshot());
        } catch (Throwable t) {
            log.warn("Failed to build metrics snapshot for {}: {}", targetKey.canonicalId(), t.getMessage());
            // Actively publish a degraded snapshot so scrapes never see stale dataReady=true (#15)
            latestSnapshot.set(buildDegradedFallbackSnapshot("snapshot_build_failed: " + t.getClass().getSimpleName()));
        }
    }

    private void purgeExpiredSafe() {
        if (stopped.get()) {
            return;
        }
        try {
            List<String> purged = store.purgeExpiredGroups(System.currentTimeMillis());
            for (String g : purged) {
                evaluator.invalidateGroup(g);
            }
        } catch (Throwable t) {
            log.warn("Expired group purge error for {}: {}", targetKey.canonicalId(), t.getMessage());
        }
    }

    protected ClusterMetricsSnapshot buildSnapshot() {
        long startNs = System.nanoTime();
        ClusterSpec currentSpec = this.spec;
        long nowMs = System.currentTimeMillis();
        long nowSec = nowMs / 1000L;

        boolean collectionSuccess = brokerCollector.isLastCollectionSuccess();
        long lastSuccessSec = brokerCollector.lastCollectionSuccessEpochSec();
        long maxAgeSec = Math.max(currentSpec.collection().offsetRefreshSeconds() * 3L, 60L);
        boolean headFresh = collectionSuccess && lastSuccessSec > 0L && (nowSec - lastSuccessSec) <= maxAgeSec;
        boolean offsetsCaughtUp = offsetsIngestor.isCaughtUp();
        boolean reachable = brokerCollector.isReachable() && offsetsIngestor.isHealthy();
        boolean cardinalityReached = store.isCardinalityLimitReached();
        // Cardinality limit exhaustion invalidates completeness -> cluster_up=0 / DEGRADED (#03)
        boolean dataReady = headFresh && offsetsCaughtUp && reachable && !cardinalityReached;

        ClusterPhase phase = determinePhase(dataReady, offsetsCaughtUp, reachable, cardinalityReached);
        String errMsg = !offsetsIngestor.lastError().isEmpty()
                ? offsetsIngestor.lastError()
                : (!brokerCollector.lastError().isEmpty()
                ? brokerCollector.lastError()
                : (cardinalityReached ? "cardinality_limit_reached" : ""));

        List<GroupEvaluation> groups = dataReady || phase == ClusterPhase.DEGRADED
                ? evaluator.evaluateAllGroups()
                : List.of();
        Map<String, long[]> topicOffsets = currentSpec.filters().exportTopicPartitionOffset()
                ? store.snapshotAllBrokerLatestOffsets()
                : Map.of();

        ClusterHealthSnapshot health = new ClusterHealthSnapshot(
                phase,
                dataReady,
                headFresh,
                offsetsCaughtUp,
                reachable,
                collectionSuccess,
                lastSuccessSec,
                offsetsIngestor.computeInternalOffsetsLag(),
                store.trackedGroupCount(),
                store.trackedPartitionCount(),
                cardinalityReached,
                generation,
                errMsg
        );

        double buildSec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        DurationHistogramSnapshot hist = observeBuildDuration(buildSec);
        return new ClusterMetricsSnapshot(
                targetKey,
                currentSpec,
                health,
                groups,
                topicOffsets,
                offsetsIngestor.snapshotDecodeErrors(),
                snapshotReconcileErrors(),
                hist,
                nowMs
        );
    }

    private ClusterMetricsSnapshot buildDegradedFallbackSnapshot(String errorMessage) {
        ClusterMetricsSnapshot prev = latestSnapshot.get();
        long nowMs = System.currentTimeMillis();
        ClusterHealthSnapshot degraded = new ClusterHealthSnapshot(
                ClusterPhase.DEGRADED,
                false,
                false,
                offsetsIngestor.isCaughtUp(),
                false,
                false,
                brokerCollector.lastCollectionSuccessEpochSec(),
                offsetsIngestor.computeInternalOffsetsLag(),
                store.trackedGroupCount(),
                store.trackedPartitionCount(),
                store.isCardinalityLimitReached(),
                generation,
                errorMessage
        );
        DurationHistogramSnapshot hist = prev != null
                ? prev.snapshotBuildDurationHistogram()
                : DurationHistogramSnapshot.empty();
        return new ClusterMetricsSnapshot(
                targetKey,
                this.spec,
                degraded,
                List.of(),
                Map.of(),
                offsetsIngestor.snapshotDecodeErrors(),
                snapshotReconcileErrors(),
                hist,
                nowMs
        );
    }

    private synchronized DurationHistogramSnapshot observeBuildDuration(double seconds) {
        double[] bounds = DurationHistogramSnapshot.DEFAULT_BUCKETS;
        for (int i = 0; i < bounds.length; i++) {
            if (seconds <= bounds[i]) {
                durationBucketCounts[i]++;
                break;
            }
        }
        durationSumSeconds += seconds;
        return new DurationHistogramSnapshot(bounds, Arrays.copyOf(durationBucketCounts, durationBucketCounts.length), durationSumSeconds);
    }

    private ClusterPhase determinePhase(boolean dataReady, boolean offsetsCaughtUp, boolean reachable, boolean cardinalityReached) {
        if (stopped.get()) {
            return ClusterPhase.STOPPED;
        }
        if (offsetsIngestor.isWaitingForTopic()) {
            return ClusterPhase.WAITING_OFFSETS_TOPIC;
        }
        if (cardinalityReached) {
            return ClusterPhase.DEGRADED;
        }
        if (!offsetsCaughtUp) {
            return brokerCollector.isInitialMetadataLoaded() ? ClusterPhase.REPLAYING : ClusterPhase.INITIALIZING;
        }
        if (dataReady) {
            return ClusterPhase.READY;
        }
        return reachable ? ClusterPhase.INITIALIZING : ClusterPhase.DEGRADED;
    }

    private Map<String, Long> snapshotReconcileErrors() {
        Map<String, Long> out = new HashMap<>();
        synchronized (reconcileErrors) {
            for (Map.Entry<String, AtomicLong> e : reconcileErrors.entrySet()) {
                out.put(e.getKey(), e.getValue().get());
            }
        }
        return out;
    }

    private static void cancelQuietly(ScheduledFuture<?> f) {
        if (f != null) {
            f.cancel(false);
        }
    }
}
