/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.ClusterSpec.SwapPolicy;
import io.karasu.core.config.ClusterSpecValidator;
import io.karasu.core.config.KafkaClientConfigBuilder;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.protocol.model.ClusterTargetKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Orchestrates multiple isolated {@link ClusterRuntime} instances keyed by {@link ClusterTargetKey}.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Per-cluster serialized reconciliation: Add / Hot-Patch / Blue-Green Swap / Delete on one cluster
 *       never blocks or restarts other clusters in the same JVM.</li>
 *   <li>Monotonic generation barrier per target key prevents stale callbacks or old runtimes from publishing.</li>
 *   <li>Deleting a cluster immediately removes its snapshot from Prometheus exposition before tearing down I/O.</li>
 * </ul>
 */
public final class ClusterManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    @FunctionalInterface
    public interface RuntimeFactory {
        ClusterRuntime create(ClusterSpec spec,
                              long generation,
                              String connectionFingerprint,
                              ScheduledExecutorService collectionScheduler,
                              ScheduledExecutorService snapshotScheduler);
    }

    private final int maxExtraLabels;
    private final Duration blueGreenTimeout;
    private final RuntimeFactory runtimeFactory;
    private final Function<ClusterSpec, String> materialDigestFn;

    private final ScheduledExecutorService collectionScheduler;
    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService swapExecutor;

    private final ConcurrentHashMap<ClusterTargetKey, ClusterRuntime> activeRuntimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClusterTargetKey, Object> targetLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClusterTargetKey, AtomicLong> generations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClusterTargetKey, ConcurrentHashMap<String, AtomicLong>> reconcileErrorCounters = new ConcurrentHashMap<>();

    public ClusterManager(int maxExtraLabels) {
        this(
                maxExtraLabels,
                Duration.ofSeconds(120),
                ClusterRuntime::new,
                KafkaClientConfigBuilder::computeMaterialDigest
        );
    }

    public ClusterManager(int maxExtraLabels,
                          Duration blueGreenTimeout,
                          RuntimeFactory runtimeFactory,
                          Function<ClusterSpec, String> materialDigestFn) {
        this.maxExtraLabels = maxExtraLabels;
        this.blueGreenTimeout = blueGreenTimeout;
        this.runtimeFactory = runtimeFactory;
        this.materialDigestFn = materialDigestFn;
        this.collectionScheduler = Executors.newScheduledThreadPool(8, namedThreadFactory("karasu-collect-"));
        this.snapshotScheduler = Executors.newScheduledThreadPool(4, namedThreadFactory("karasu-snap-"));
        this.swapExecutor = Executors.newCachedThreadPool(namedThreadFactory("karasu-swap-"));
    }

    /**
     * Reconciles the running cluster set to match {@code desiredSpecs}.
     * Invalid specs are rejected individually without impacting valid or existing clusters.
     */
    public void reconcile(List<ClusterSpec> desiredSpecs) {
        Map<ClusterTargetKey, ClusterSpec> desiredByKey = new LinkedHashMap<>();
        for (ClusterSpec spec : desiredSpecs) {
            if (spec == null || spec.targetKey() == null) {
                continue;
            }
            try {
                ClusterSpecValidator.validate(spec, maxExtraLabels);
            } catch (IllegalArgumentException ex) {
                log.error("Rejecting invalid config for {}: {}", spec.targetKey().canonicalId(), ex.getMessage());
                recordReconcileError(spec.targetKey(), "validation");
                continue;
            }
            if (desiredByKey.putIfAbsent(spec.targetKey(), spec) != null) {
                log.error("Duplicate cluster target key {} ignored", spec.targetKey().canonicalId());
                recordReconcileError(spec.targetKey(), "duplicate_key");
            }
        }

        // 1. Delete clusters removed from desired set
        Set<ClusterTargetKey> toRemove = new HashSet<>(activeRuntimes.keySet());
        toRemove.removeAll(desiredByKey.keySet());
        for (ClusterTargetKey key : toRemove) {
            removeCluster(key);
        }

        // 2. Add or update clusters in desired set
        for (ClusterSpec desired : desiredByKey.values()) {
            upsertCluster(desired);
        }
    }

    public void upsertCluster(ClusterSpec spec) {
        ClusterTargetKey key = spec.targetKey();
        Object lock = targetLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            String materialDigest = materialDigestFn.apply(spec);
            String newFingerprint = spec.connectionFingerprint(materialDigest);
            ClusterRuntime existing = activeRuntimes.get(key);

            if (existing == null) {
                long gen = nextGeneration(key);
                ClusterRuntime created = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
                activeRuntimes.put(key, created);
                created.start();
                log.info("Added cluster target={} gen={}", key.canonicalId(), gen);
                return;
            }

            if (existing.connectionFingerprint().equals(newFingerprint)) {
                if (!existing.spec().equals(spec)) {
                    existing.hotPatch(spec);
                }
                return;
            }

            // Connection / auth / TLS changed
            SwapPolicy policy = spec.collection().swapPolicy();
            if (policy == SwapPolicy.RECREATE || !existing.isDataReady()) {
                long gen = nextGeneration(key);
                ClusterRuntime replacement = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
                activeRuntimes.put(key, replacement);
                replacement.start();
                stopAsync(existing);
                log.info("Recreated cluster target={} oldGen={} newGen={}", key.canonicalId(), existing.generation(), gen);
            } else {
                long gen = nextGeneration(key);
                ClusterRuntime candidate = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
                candidate.start();
                log.info("Starting Blue-Green swap for target={} oldGen={} candidateGen={}",
                        key.canonicalId(), existing.generation(), gen);
                swapExecutor.submit(() -> completeBlueGreenSwap(key, existing, candidate, gen));
            }
        }
    }

    public void removeCluster(ClusterTargetKey key) {
        Object lock = targetLocks.computeIfAbsent(key, k -> new Object());
        ClusterRuntime removed;
        synchronized (lock) {
            // Bump generation so any in-flight blue-green candidate aborts cleanly
            nextGeneration(key);
            removed = activeRuntimes.remove(key);
        }
        if (removed != null) {
            log.info("Removed cluster target={} gen={} (metrics unpublished immediately)", key.canonicalId(), removed.generation());
            removed.stop();
        }
    }

    /**
     * Lock-free read of all active clusters' latest immutable metrics snapshots, sorted deterministically.
     */
    public List<ClusterMetricsSnapshot> currentSnapshots() {
        List<ClusterMetricsSnapshot> list = new ArrayList<>(activeRuntimes.size());
        for (ClusterRuntime runtime : activeRuntimes.values()) {
            ClusterMetricsSnapshot snap = runtime.currentSnapshot();
            if (snap != null) {
                Map<String, Long> mergedErrors = mergeReconcileErrors(snap.targetKey(), snap.reconcileErrorCounts());
                if (mergedErrors != snap.reconcileErrorCounts()) {
                    snap = new ClusterMetricsSnapshot(
                            snap.targetKey(), snap.spec(), snap.health(), snap.groups(),
                            snap.topicPartitionOffsets(), snap.decodeErrorCounts(),
                            mergedErrors, snap.lastSnapshotBuildDurationSeconds(), snap.builtAtEpochMs());
                }
                list.add(snap);
            }
        }
        list.sort(Comparator.comparing(ClusterMetricsSnapshot::targetKey));
        return list;
    }

    public Set<ClusterTargetKey> activeTargets() {
        return Set.copyOf(activeRuntimes.keySet());
    }

    public long currentGeneration(ClusterTargetKey key) {
        AtomicLong gen = generations.get(key);
        return gen == null ? 0L : gen.get();
    }

    @Override
    public void close() {
        List<ClusterRuntime> toStop = new ArrayList<>(activeRuntimes.values());
        activeRuntimes.clear();
        for (ClusterRuntime runtime : toStop) {
            try {
                runtime.stop();
            } catch (Exception e) {
                log.warn("Error stopping runtime {}: {}", runtime.targetKey().canonicalId(), e.getMessage());
            }
        }
        shutdownExecutor(swapExecutor);
        shutdownExecutor(collectionScheduler);
        shutdownExecutor(snapshotScheduler);
    }

    private void completeBlueGreenSwap(ClusterTargetKey key,
                                       ClusterRuntime existing,
                                       ClusterRuntime candidate,
                                       long candidateGen) {
        long deadlineMs = System.currentTimeMillis() + blueGreenTimeout.toMillis();
        boolean ready = false;
        while (System.currentTimeMillis() < deadlineMs) {
            if (currentGeneration(key) != candidateGen) {
                // Superseded by a newer update or deletion
                candidate.stop();
                return;
            }
            if (candidate.isDataReady()) {
                ready = true;
                break;
            }
            try {
                Thread.sleep(250L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                candidate.stop();
                return;
            }
        }

        Object lock = targetLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            if (currentGeneration(key) != candidateGen) {
                candidate.stop();
                return;
            }
            if (ready) {
                activeRuntimes.put(key, candidate);
                stopAsync(existing);
                log.info("Blue-Green swap succeeded for target={} switched to gen={}", key.canonicalId(), candidateGen);
            } else {
                // Keep old healthy runtime active and report reconcile failure
                recordReconcileError(key, "blue_green_timeout");
                log.error("Blue-Green swap timed out for target={} candidateGen={}; retaining active gen={}",
                        key.canonicalId(), candidateGen, existing.generation());
                stopAsync(candidate);
            }
        }
    }

    private void stopAsync(ClusterRuntime runtime) {
        swapExecutor.submit(() -> {
            try {
                runtime.stop();
            } catch (Exception e) {
                log.warn("Error stopping old runtime {} gen={}: {}",
                        runtime.targetKey().canonicalId(), runtime.generation(), e.getMessage());
            }
        });
    }

    private long nextGeneration(ClusterTargetKey key) {
        return generations.computeIfAbsent(key, k -> new AtomicLong(0L)).incrementAndGet();
    }

    private void recordReconcileError(ClusterTargetKey key, String phase) {
        reconcileErrorCounters
                .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(phase, p -> new AtomicLong(0L))
                .incrementAndGet();
    }

    private Map<String, Long> mergeReconcileErrors(ClusterTargetKey key, Map<String, Long> existing) {
        ConcurrentHashMap<String, AtomicLong> mgrErrors = reconcileErrorCounters.get(key);
        if (mgrErrors == null || mgrErrors.isEmpty()) {
            return existing;
        }
        Map<String, Long> merged = new HashMap<>(existing);
        for (Map.Entry<String, AtomicLong> e : mgrErrors.entrySet()) {
            merged.merge(e.getKey(), e.getValue().get(), Long::sum);
        }
        return merged;
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger(1);
        return r -> {
            Thread t = new Thread(r, prefix + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
