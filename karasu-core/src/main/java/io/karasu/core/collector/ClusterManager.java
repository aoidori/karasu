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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Orchestrates multiple isolated {@link ClusterRuntime} instances keyed by {@link ClusterTargetKey}.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Invalid target specs never delete an already-running valid runtime for that key (#06).</li>
 *   <li>Multi-provider reconciliation merges last-known-good sets across all registered providers (#13).</li>
 *   <li>Per-target desired-fingerprint & generation barriers prevent stale Blue-Green candidates from
 *       activating after a rollback (A -> B -> A) (#07).</li>
 *   <li>Timed-out Blue-Green swaps automatically schedule exponential-backoff retries until resolved (#14).</li>
 *   <li>Raising limits after cardinality saturation or loosening group filters triggers Blue-Green recovery (#03, #10).</li>
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
    private final Duration blueGreenRetryBaseDelay;
    private final RuntimeFactory runtimeFactory;
    private final Function<ClusterSpec, String> materialDigestFn;
    private volatile Consumer<ClusterSpec> extraSpecValidator;

    private final ScheduledExecutorService collectionScheduler;
    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService swapExecutor;

    private final Object providerReconcileLock = new Object();
    private final Map<String, List<ClusterSpec>> specsByProvider = new LinkedHashMap<>();

    private final ConcurrentHashMap<ClusterTargetKey, TargetSlot> slots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClusterTargetKey, AtomicLong> generations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClusterTargetKey, ConcurrentHashMap<String, AtomicLong>> reconcileErrorCounters = new ConcurrentHashMap<>();

    public ClusterManager(int maxExtraLabels) {
        this(
                maxExtraLabels,
                Duration.ofSeconds(120),
                Duration.ofSeconds(15),
                ClusterRuntime::new,
                KafkaClientConfigBuilder::computeMaterialDigest
        );
    }

    public ClusterManager(int maxExtraLabels,
                          Duration blueGreenTimeout,
                          RuntimeFactory runtimeFactory,
                          Function<ClusterSpec, String> materialDigestFn) {
        this(maxExtraLabels, blueGreenTimeout, Duration.ofSeconds(15), runtimeFactory, materialDigestFn);
    }

    public ClusterManager(int maxExtraLabels,
                          Duration blueGreenTimeout,
                          Duration blueGreenRetryBaseDelay,
                          RuntimeFactory runtimeFactory,
                          Function<ClusterSpec, String> materialDigestFn) {
        this.maxExtraLabels = maxExtraLabels;
        this.blueGreenTimeout = blueGreenTimeout;
        this.blueGreenRetryBaseDelay = blueGreenRetryBaseDelay;
        this.runtimeFactory = runtimeFactory;
        this.materialDigestFn = materialDigestFn;
        this.collectionScheduler = Executors.newScheduledThreadPool(8, namedThreadFactory("karasu-collect-"));
        this.snapshotScheduler = Executors.newScheduledThreadPool(4, namedThreadFactory("karasu-snap-"));
        this.swapExecutor = Executors.newCachedThreadPool(namedThreadFactory("karasu-swap-"));
    }

    /**
     * Attaches an additional validator (e.g. checking merged L1+L2+L3 Prometheus labels before activation, #12).
     */
    public void setExtraSpecValidator(Consumer<ClusterSpec> extraSpecValidator) {
        this.extraSpecValidator = extraSpecValidator;
    }

    /**
     * Records a provider's desired target set and reconciles the union across all registered providers (#13).
     */
    public void reconcileFromProvider(String providerId, List<ClusterSpec> providerSpecs) {
        synchronized (providerReconcileLock) {
            specsByProvider.put(providerId, providerSpecs == null ? List.of() : List.copyOf(providerSpecs));
            List<ClusterSpec> union = new ArrayList<>();
            for (List<ClusterSpec> list : specsByProvider.values()) {
                union.addAll(list);
            }
            reconcile(union);
        }
    }

    /**
     * Reconciles the running cluster set to match {@code desiredSpecs}.
     * Invalid specs are rejected individually while preserving any existing active runtime for that key (#06).
     */
    public void reconcile(List<ClusterSpec> desiredSpecs) {
        Map<ClusterTargetKey, ClusterSpec> desiredByKey = new LinkedHashMap<>();
        Set<ClusterTargetKey> invalidKeys = new HashSet<>();

        for (ClusterSpec spec : desiredSpecs) {
            if (spec == null || spec.targetKey() == null) {
                continue;
            }
            try {
                ClusterSpecValidator.validate(spec, maxExtraLabels);
                Consumer<ClusterSpec> extra = this.extraSpecValidator;
                if (extra != null) {
                    extra.accept(spec);
                }
            } catch (IllegalArgumentException ex) {
                log.error("Rejecting invalid config for {} (retaining previous valid runtime if any): {}",
                        spec.targetKey().canonicalId(), ex.getMessage());
                recordReconcileError(spec.targetKey(), "validation");
                invalidKeys.add(spec.targetKey());
                continue;
            }
            if (desiredByKey.putIfAbsent(spec.targetKey(), spec) != null) {
                log.error("Duplicate cluster target key {} ignored", spec.targetKey().canonicalId());
                recordReconcileError(spec.targetKey(), "duplicate_key");
            }
        }

        // 1. Delete ONLY targets explicitly absent from both desiredByKey AND invalidKeys (#06)
        Set<ClusterTargetKey> toRemove = new HashSet<>(slots.keySet());
        toRemove.removeAll(desiredByKey.keySet());
        toRemove.removeAll(invalidKeys);
        for (ClusterTargetKey key : toRemove) {
            removeCluster(key);
        }

        // 2. Add or update valid targets
        for (ClusterSpec desired : desiredByKey.values()) {
            upsertCluster(desired);
        }
    }

    public void upsertCluster(ClusterSpec spec) {
        ClusterTargetKey key = spec.targetKey();
        TargetSlot slot = slots.computeIfAbsent(key, k -> new TargetSlot());
        synchronized (slot) {
            String materialDigest = materialDigestFn.apply(spec);
            String newFingerprint = spec.connectionFingerprint(materialDigest);
            ClusterRuntime existing = slot.active;

            if (existing == null) {
                long gen = nextGeneration(key);
                ClusterRuntime created = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
                slot.active = created;
                slot.desiredSpec = spec;
                slot.desiredFingerprint = newFingerprint;
                slot.desiredRequiresRebuild = false;
                created.start();
                log.info("Added cluster target={} gen={}", key.canonicalId(), gen);
                return;
            }

            boolean needsRebuild = !existing.connectionFingerprint().equals(newFingerprint)
                    || requiresRecoveryRebuild(existing, spec);

            if (!needsRebuild) {
                // Desired configuration matches active runtime -> abort any in-flight Blue-Green swap or retry (#07)
                slot.desiredSpec = spec;
                slot.desiredFingerprint = newFingerprint;
                slot.desiredRequiresRebuild = false;
                cancelInFlightSwapLocked(key, slot, "reverted_to_active_spec");
                if (!existing.spec().equals(spec)) {
                    existing.hotPatch(spec);
                }
                return;
            }

            // If an in-flight candidate is already building for this exact spec & fingerprint, keep waiting
            if (slot.inFlightCandidate != null
                    && newFingerprint.equals(slot.inFlightFingerprint)
                    && spec.equals(slot.desiredSpec)) {
                return;
            }

            // Start replacement (either RECREATE or BLUE_GREEN)
            cancelInFlightSwapLocked(key, slot, "superseded_by_newer_spec");
            slot.desiredSpec = spec;
            slot.desiredFingerprint = newFingerprint;
            slot.desiredRequiresRebuild = true;
            launchReplacementLocked(key, slot, spec, newFingerprint, 0);
        }
    }

    public void removeCluster(ClusterTargetKey key) {
        TargetSlot slot = slots.remove(key);
        if (slot == null) {
            return;
        }
        ClusterRuntime removedActive;
        ClusterRuntime removedCandidate;
        synchronized (slot) {
            nextGeneration(key);
            cancelRetryLocked(slot);
            removedActive = slot.active;
            removedCandidate = slot.inFlightCandidate;
            slot.active = null;
            slot.inFlightCandidate = null;
            slot.desiredSpec = null;
            slot.desiredFingerprint = null;
        }
        if (removedCandidate != null) {
            removedCandidate.stop();
        }
        if (removedActive != null) {
            log.info("Removed cluster target={} gen={} (metrics unpublished immediately)", key.canonicalId(), removedActive.generation());
            removedActive.stop();
        }
    }

    public List<ClusterMetricsSnapshot> currentSnapshots() {
        List<ClusterMetricsSnapshot> list = new ArrayList<>(slots.size());
        for (Map.Entry<ClusterTargetKey, TargetSlot> entry : slots.entrySet()) {
            ClusterRuntime runtime = entry.getValue().active;
            if (runtime == null) {
                continue;
            }
            ClusterMetricsSnapshot snap = runtime.currentSnapshot();
            if (snap != null) {
                Map<String, Long> mergedErrors = mergeReconcileErrors(snap.targetKey(), snap.reconcileErrorCounts());
                if (mergedErrors != snap.reconcileErrorCounts()) {
                    snap = new ClusterMetricsSnapshot(
                            snap.targetKey(), snap.spec(), snap.health(), snap.groups(),
                            snap.topicPartitionOffsets(), snap.decodeErrorCounts(),
                            mergedErrors, snap.snapshotBuildDurationHistogram(), snap.builtAtEpochMs());
                }
                list.add(snap);
            }
        }
        list.sort(Comparator.comparing(ClusterMetricsSnapshot::targetKey));
        return list;
    }

    public Set<ClusterTargetKey> activeTargets() {
        Set<ClusterTargetKey> keys = new HashSet<>();
        for (Map.Entry<ClusterTargetKey, TargetSlot> entry : slots.entrySet()) {
            if (entry.getValue().active != null) {
                keys.add(entry.getKey());
            }
        }
        return Set.copyOf(keys);
    }

    public long currentGeneration(ClusterTargetKey key) {
        AtomicLong gen = generations.get(key);
        return gen == null ? 0L : gen.get();
    }

    @Override
    public void close() {
        List<ClusterTargetKey> keys = new ArrayList<>(slots.keySet());
        for (ClusterTargetKey key : keys) {
            try {
                removeCluster(key);
            } catch (Exception e) {
                log.warn("Error stopping runtime {}: {}", key.canonicalId(), e.getMessage());
            }
        }
        shutdownExecutor(swapExecutor);
        shutdownExecutor(collectionScheduler);
        shutdownExecutor(snapshotScheduler);
    }

    /**
     * Determines whether an in-place hotPatch is insufficient because previously discarded state must be replayed:
     * <ul>
     *   <li>Cardinality limit was reached and limits were expanded (#03).</li>
     *   <li>Group allowlist/denylist was loosened or modified beyond pure tightening (#10).</li>
     * </ul>
     */
    private static boolean requiresRecoveryRebuild(ClusterRuntime existing, ClusterSpec updatedSpec) {
        ClusterSpec oldSpec = existing.spec();
        if (existing.isCardinalityLimitReached()
                && (updatedSpec.limits().maxGroups() > oldSpec.limits().maxGroups()
                || updatedSpec.limits().maxGroupPartitions() > oldSpec.limits().maxGroupPartitions())) {
            return true;
        }
        String oldAllow = oldSpec.filters().groupAllowlist();
        String newAllow = updatedSpec.filters().groupAllowlist();
        String oldDeny = oldSpec.filters().groupDenylist();
        String newDeny = updatedSpec.filters().groupDenylist();
        if (!oldAllow.equals(newAllow) || !oldDeny.equals(newDeny)) {
            boolean pureTightenAllow = oldAllow.isEmpty() && !newAllow.isEmpty() && oldDeny.equals(newDeny);
            boolean pureTightenDeny = oldDeny.isEmpty() && !newDeny.isEmpty() && oldAllow.equals(newAllow);
            return !(pureTightenAllow || pureTightenDeny);
        }
        return false;
    }

    private void launchReplacementLocked(ClusterTargetKey key,
                                         TargetSlot slot,
                                         ClusterSpec spec,
                                         String newFingerprint,
                                         int attempt) {
        ClusterRuntime existing = slot.active;
        if (existing == null) {
            return;
        }
        SwapPolicy policy = spec.collection().swapPolicy();
        if (policy == SwapPolicy.RECREATE || !existing.isDataReady()) {
            long gen = nextGeneration(key);
            ClusterRuntime replacement = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
            slot.active = replacement;
            slot.desiredRequiresRebuild = false;
            replacement.start();
            stopAsync(existing);
            log.info("Recreated cluster target={} oldGen={} newGen={}", key.canonicalId(), existing.generation(), gen);
        } else {
            long gen = nextGeneration(key);
            ClusterRuntime candidate = runtimeFactory.create(spec, gen, newFingerprint, collectionScheduler, snapshotScheduler);
            slot.inFlightCandidate = candidate;
            slot.inFlightCandidateGen = gen;
            slot.inFlightFingerprint = newFingerprint;
            candidate.start();
            log.info("Starting Blue-Green swap for target={} oldGen={} candidateGen={} attempt={}",
                    key.canonicalId(), existing.generation(), gen, attempt);
            swapExecutor.submit(() -> completeBlueGreenSwap(key, slot, existing, candidate, gen, newFingerprint, attempt));
        }
    }

    private void completeBlueGreenSwap(ClusterTargetKey key,
                                       TargetSlot slot,
                                       ClusterRuntime existing,
                                       ClusterRuntime candidate,
                                       long candidateGen,
                                       String candidateFingerprint,
                                       int attempt) {
        long deadlineMs = System.currentTimeMillis() + blueGreenTimeout.toMillis();
        boolean ready = false;
        while (System.currentTimeMillis() < deadlineMs) {
            if (currentGeneration(key) != candidateGen || slots.get(key) != slot) {
                candidate.stop();
                return;
            }
            if (candidate.isDataReady()) {
                ready = true;
                break;
            }
            try {
                Thread.sleep(Math.min(250L, Math.max(10L, blueGreenTimeout.toMillis() / 10L)));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                candidate.stop();
                return;
            }
        }

        synchronized (slot) {
            // Re-verify generation, candidate identity, AND desired fingerprint under the same lock (#07)
            boolean stillDesired = slots.get(key) == slot
                    && slot.inFlightCandidate == candidate
                    && slot.inFlightCandidateGen == candidateGen
                    && currentGeneration(key) == candidateGen
                    && candidateFingerprint.equals(slot.desiredFingerprint);
            if (!stillDesired) {
                stopAsync(candidate);
                return;
            }
            slot.inFlightCandidate = null;
            slot.inFlightCandidateGen = 0L;
            slot.inFlightFingerprint = null;

            if (ready) {
                slot.active = candidate;
                slot.desiredRequiresRebuild = false;
                cancelRetryLocked(slot);
                stopAsync(existing);
                log.info("Blue-Green swap succeeded for target={} switched to gen={}", key.canonicalId(), candidateGen);
            } else {
                recordReconcileError(key, "blue_green_timeout");
                stopAsync(candidate);
                // Schedule exponential-backoff retry so recovery happens automatically once target recovers (#14)
                long delayMs = Math.min(120_000L, blueGreenRetryBaseDelay.toMillis() * (1L << Math.min(attempt, 3)));
                log.error("Blue-Green swap timed out for target={} candidateGen={} attempt={}; retaining active gen={} and retrying in {}ms",
                        key.canonicalId(), candidateGen, attempt, existing.generation(), delayMs);
                ClusterSpec pendingSpec = slot.desiredSpec;
                String pendingFp = slot.desiredFingerprint;
                slot.pendingRetryTask = snapshotScheduler.schedule(
                        () -> retryBlueGreenSwap(key, slot, pendingSpec, pendingFp, attempt + 1),
                        delayMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }
    }

    private void retryBlueGreenSwap(ClusterTargetKey key,
                                    TargetSlot slot,
                                    ClusterSpec expectedSpec,
                                    String expectedFp,
                                    int nextAttempt) {
        synchronized (slot) {
            if (slots.get(key) != slot || slot.active == null || !slot.desiredRequiresRebuild) {
                return;
            }
            if (!expectedFp.equals(slot.desiredFingerprint) || slot.inFlightCandidate != null) {
                return;
            }
            slot.pendingRetryTask = null;
            launchReplacementLocked(key, slot, expectedSpec, expectedFp, nextAttempt);
        }
    }

    private void cancelInFlightSwapLocked(ClusterTargetKey key, TargetSlot slot, String reason) {
        cancelRetryLocked(slot);
        if (slot.inFlightCandidate != null) {
            ClusterRuntime obsolete = slot.inFlightCandidate;
            long obsoleteGen = slot.inFlightCandidateGen;
            slot.inFlightCandidate = null;
            slot.inFlightCandidateGen = 0L;
            slot.inFlightFingerprint = null;
            // Advance generation so any waiting swap thread immediately exits (#07)
            nextGeneration(key);
            stopAsync(obsolete);
            log.info("Cancelled in-flight Blue-Green candidate target={} gen={} reason={}",
                    key.canonicalId(), obsoleteGen, reason);
        }
    }

    private static void cancelRetryLocked(TargetSlot slot) {
        if (slot.pendingRetryTask != null) {
            slot.pendingRetryTask.cancel(false);
            slot.pendingRetryTask = null;
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

    private static final class TargetSlot {
        volatile ClusterRuntime active;
        ClusterRuntime inFlightCandidate;
        long inFlightCandidateGen;
        String inFlightFingerprint;
        ClusterSpec desiredSpec;
        String desiredFingerprint;
        boolean desiredRequiresRebuild;
        ScheduledFuture<?> pendingRetryTask;
    }
}
