/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.evaluator.LagEvaluator;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.core.storage.ClusterStore;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.GroupEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
 * and immutable snapshot publishing for a single {@link ClusterTargetKey}.
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
        this.store = new ClusterStore(spec);
        this.evaluator = new LagEvaluator(spec, store);
        this.brokerCollector = new BrokerOffsetCollector(spec, generation, store, evaluator::invalidateAll);
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

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // Kick off initial broker offset cycle immediately on shared scheduler, then periodically
        int offsetRefresh = spec.collection().offsetRefreshSeconds();
        this.offsetTask = collectionScheduler.scheduleWithFixedDelay(
                brokerCollector::collectOnce, 0L, offsetRefresh, TimeUnit.SECONDS);

        // Start dedicated __consumer_offsets reader thread(s)
        offsetsIngestor.start();

        // Periodic background immutable snapshot build
        int snapRefresh = Math.max(1, spec.collection().snapshotRefreshSeconds());
        this.snapshotTask = snapshotScheduler.scheduleWithFixedDelay(
                this::refreshSnapshotSafe, 1L, snapRefresh, TimeUnit.SECONDS);

        // Proactive expired-group purge every 5 minutes
        this.expiryTask = snapshotScheduler.scheduleWithFixedDelay(
                this::purgeExpiredSafe, 300L, 300L, TimeUnit.SECONDS);

        log.info("Started ClusterRuntime target={} gen={}", targetKey.canonicalId(), generation);
    }

    /**
     * Applies non-connection policy changes in place without reconnecting or replaying {@code __consumer_offsets}.
     */
    public synchronized void hotPatch(ClusterSpec updatedSpec) {
        if (stopped.get()) {
            return;
        }
        int oldOffsetSec = this.spec.collection().offsetRefreshSeconds();
        int oldSnapSec = this.spec.collection().snapshotRefreshSeconds();
        this.spec = updatedSpec;
        this.store.applyPolicy(updatedSpec);
        this.evaluator.applyPolicy(updatedSpec);
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
        log.info("Hot-patched ClusterRuntime target={} gen={}", targetKey.canonicalId(), generation);
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

    private void refreshSnapshotSafe() {
        if (stopped.get()) {
            return;
        }
        try {
            latestSnapshot.set(buildSnapshot());
        } catch (Throwable t) {
            log.warn("Failed to build metrics snapshot for {}: {}", targetKey.canonicalId(), t.getMessage());
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

    private ClusterMetricsSnapshot buildSnapshot() {
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
        boolean dataReady = headFresh && offsetsCaughtUp && reachable;

        ClusterPhase phase = determinePhase(dataReady, offsetsCaughtUp, reachable);
        String errMsg = !offsetsIngestor.lastError().isEmpty()
                ? offsetsIngestor.lastError()
                : brokerCollector.lastError();

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
                store.isCardinalityLimitReached(),
                generation,
                errMsg
        );

        double buildSec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        return new ClusterMetricsSnapshot(
                targetKey,
                currentSpec,
                health,
                groups,
                topicOffsets,
                offsetsIngestor.snapshotDecodeErrors(),
                snapshotReconcileErrors(),
                buildSec,
                nowMs
        );
    }

    private ClusterPhase determinePhase(boolean dataReady, boolean offsetsCaughtUp, boolean reachable) {
        if (stopped.get()) {
            return ClusterPhase.STOPPED;
        }
        if (offsetsIngestor.isWaitingForTopic()) {
            return ClusterPhase.WAITING_OFFSETS_TOPIC;
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
