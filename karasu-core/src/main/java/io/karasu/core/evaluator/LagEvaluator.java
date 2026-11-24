/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.evaluator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.karasu.core.config.ClusterSpec;
import io.karasu.core.storage.ClusterStore;
import io.karasu.core.storage.ClusterStore.PartitionSnapshot;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.GroupEvaluation;
import io.karasu.protocol.model.PartitionEvaluation;
import io.karasu.protocol.model.StatusCode;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-cluster evaluator with bounded Caffeine cache, supporting active invalidation and hot-patched thresholds.
 */
public final class LagEvaluator {

    private final ClusterTargetKey targetKey;
    private final ClusterStore store;
    private final Clock clock;
    private volatile float minimumComplete;
    private volatile long allowedLag;
    private volatile Cache<String, GroupEvaluation> cache;

    public LagEvaluator(ClusterSpec spec, ClusterStore store) {
        this(spec, store, Clock.systemUTC());
    }

    public LagEvaluator(ClusterSpec spec, ClusterStore store, Clock clock) {
        this.targetKey = spec.targetKey();
        this.store = store;
        this.clock = clock;
        applyPolicy(spec);
    }

    public void applyPolicy(ClusterSpec spec) {
        this.minimumComplete = spec.evaluation().minimumComplete();
        this.allowedLag = spec.evaluation().allowedLag();
        int ttl = Math.max(1, spec.evaluation().cacheExpireSeconds());
        this.cache = Caffeine.newBuilder()
                .maximumSize(Math.max(1_000, spec.limits().maxGroups()))
                .expireAfterWrite(Duration.ofSeconds(ttl))
                .build();
    }

    public void invalidateGroup(String group) {
        cache.invalidate(group);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public GroupEvaluation evaluateGroup(String group) {
        GroupEvaluation cached = cache.getIfPresent(group);
        if (cached != null) {
            return cached;
        }
        GroupEvaluation computed = computeGroupStatus(group);
        if (computed != null) {
            cache.put(group, computed);
        }
        return computed;
    }

    public List<GroupEvaluation> evaluateAllGroups() {
        List<GroupEvaluation> results = new ArrayList<>();
        for (String group : store.consumerGroupNames()) {
            GroupEvaluation eval = evaluateGroup(group);
            if (eval != null && eval.status() != StatusCode.NOTFOUND) {
                results.add(eval);
            }
        }
        return results;
    }

    private GroupEvaluation computeGroupStatus(String group) {
        long nowMs = clock.millis();
        Map<String, List<PartitionSnapshot>> topics = store.fetchConsumer(group, nowMs);
        if (topics == null) {
            return null;
        }

        long nowSec = nowMs / 1000L;
        StatusCode groupStatus = StatusCode.OK;
        long totalLag = 0L;
        int totalPartitions = 0;
        int completePartitions = 0;
        PartitionEvaluation maxLag = null;
        List<PartitionEvaluation> allPartitions = new ArrayList<>();

        for (Map.Entry<String, List<PartitionSnapshot>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            for (PartitionSnapshot ps : entry.getValue()) {
                PartitionEvaluation pe = evaluatePartition(topic, ps, nowSec);
                allPartitions.add(pe);
                totalPartitions++;
                totalLag += pe.currentLag();

                if (pe.status().code() > groupStatus.code()) {
                    groupStatus = pe.status().code() > StatusCode.ERR.code() ? StatusCode.ERR : pe.status();
                }
                if (maxLag == null || pe.currentLag() > maxLag.currentLag()) {
                    maxLag = pe;
                }
                if (pe.complete() == 1.0f) {
                    completePartitions++;
                }
            }
        }

        float groupComplete = totalPartitions > 0 ? ((float) completePartitions / (float) totalPartitions) : 0.0f;
        return new GroupEvaluation(
                targetKey,
                group,
                groupStatus,
                groupComplete,
                List.copyOf(allPartitions),
                totalPartitions,
                maxLag,
                totalLag
        );
    }

    private PartitionEvaluation evaluatePartition(String topic, PartitionSnapshot ps, long nowSec) {
        CommitSample[] commits = ps.commits();
        if (commits.length == 0) {
            return new PartitionEvaluation(
                    topic, ps.partition(), ps.owner(), ps.clientId(),
                    StatusCode.OK, null, null, ps.currentLag(), 0.0f
            );
        }

        float complete = commits.length >= ps.windowCapacity()
                ? 1.0f
                : ((float) commits.length / (float) ps.windowCapacity());

        StatusCode status = StatusCode.OK;
        if (complete >= minimumComplete) {
            status = PartitionStatusCalculator.calculatePartitionStatus(
                    commits, ps.brokerOffsets(), ps.currentLag(), nowSec, allowedLag);
        }

        return new PartitionEvaluation(
                topic,
                ps.partition(),
                ps.owner(),
                ps.clientId(),
                status,
                commits[0],
                commits[commits.length - 1],
                ps.currentLag(),
                complete
        );
    }
}
