/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.storage;

import io.karasu.core.config.ClusterSpec;
import io.karasu.protocol.decode.DecodedRecord.TopicIdPartitions;
import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.PartitionEvaluation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Isolated per-cluster in-memory store holding broker head-offset rings, consumer commit rings,
 * and KIP-848 member/UUID assignment state.
 *
 * <p>Key invariants:
 * <ul>
 *   <li>Locking discipline: never hold a broker topic lock and a consumer group lock simultaneously.</li>
 *   <li>Commits for newly created topics are retained even before broker metadata arrives (with unknown commit-time lag)
 *       and compute accurate lag once broker head offsets are sampled.</li>
 *   <li>Member metadata (Key 2/3/5/8) and committed offsets (Key 0/1) have independent lifecycles so
 *       Classic -> KIP-848 online protocol migration preserves valid committed offsets.</li>
 *   <li>Topic recreation (TopicId UUID change) resets both broker and consumer state and fences stale v4/v3 replays.</li>
 *   <li>Cardinality limit exhaustion marks affected groups and the cluster as incomplete until rebuilt.</li>
 * </ul>
 */
public final class ClusterStore {

    private final int intervals;
    private final Runnable onUnknownTopicCommit;
    private final ConcurrentHashMap<String, TopicBrokerState> brokerTopics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, String> topicIdToName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Set<String>> pendingKip848GroupsByTopicId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GroupState> consumerGroups = new ConcurrentHashMap<>();
    private final AtomicInteger trackedPartitionsCounter = new AtomicInteger(0);
    private final AtomicBoolean cardinalityLimitReached = new AtomicBoolean(false);

    // Hot-patchable runtime policy parameters
    private volatile long minDistanceMs;
    private volatile long expireGroupMs;
    private volatile int maxGroups;
    private volatile int maxGroupPartitions;
    private volatile Pattern groupAllowlist;
    private volatile Pattern groupDenylist;

    public ClusterStore(ClusterSpec spec) {
        this(spec, null);
    }

    public ClusterStore(ClusterSpec spec, Runnable onUnknownTopicCommit) {
        this.intervals = spec.evaluation().intervals();
        this.onUnknownTopicCommit = onUnknownTopicCommit;
        applyPolicy(spec);
    }

    /**
     * Applies updated policy parameters and immediately purges any already-tracked groups
     * that no longer satisfy the updated allowlist/denylist.
     */
    public List<String> applyPolicy(ClusterSpec spec) {
        this.minDistanceMs = spec.evaluation().minDistanceSeconds() * 1000L;
        this.expireGroupMs = spec.evaluation().expireGroupSeconds() * 1000L;
        this.maxGroups = spec.limits().maxGroups();
        this.maxGroupPartitions = spec.limits().maxGroupPartitions();
        this.groupAllowlist = compileOrNull(spec.filters().groupAllowlist());
        this.groupDenylist = compileOrNull(spec.filters().groupDenylist());

        List<String> purged = new ArrayList<>();
        for (String group : new ArrayList<>(consumerGroups.keySet())) {
            if (!acceptsGroup(group)) {
                deleteConsumerGroup(group);
                purged.add(group);
            }
        }
        return purged;
    }

    public int intervals() {
        return intervals;
    }

    public boolean acceptsGroup(String group) {
        Pattern allow = this.groupAllowlist;
        if (allow != null && !allow.matcher(group).find()) {
            return false;
        }
        Pattern deny = this.groupDenylist;
        return deny == null || !deny.matcher(group).find();
    }

    // --- Broker Topic & Head Offset operations ---

    /**
     * Updates topic metadata and detects same-name topic recreation via TopicId UUID change.
     *
     * @return {@code true} if the topic was recreated (UUID changed) and consumer state for it was cleared
     */
    public boolean updateTopicMetadata(String topic, UUID topicId, int partitionCount) {
        boolean[] recreatedFlag = new boolean[]{false};
        UUID[] replacedOldId = new UUID[]{null};

        brokerTopics.compute(topic, (k, existing) -> {
            if (existing == null) {
                return new TopicBrokerState(topicId, partitionCount, intervals);
            }
            UUID prevId = existing.topicId();
            boolean recreated = existing.reconcileMetadata(topicId, partitionCount, intervals);
            if (recreated) {
                recreatedFlag[0] = true;
                replacedOldId[0] = prevId;
            }
            return existing;
        });

        if (recreatedFlag[0]) {
            if (replacedOldId[0] != null) {
                topicIdToName.remove(replacedOldId[0], topic);
                pendingKip848GroupsByTopicId.remove(replacedOldId[0]);
            }
            clearConsumerTopicState(topic, topicId);
        } else if (topicId != null) {
            // Also evict any pre-discovery commits tagged with a mismatched topicId
            purgeMismatchedTopicIdCommits(topic, topicId);
        }

        if (topicId != null) {
            String previousName = topicIdToName.put(topicId, topic);
            if (previousName == null || !previousName.equals(topic)) {
                reconcilePendingKip848Assignments(topicId, topic);
            }
        }
        return recreatedFlag[0];
    }

    public void recordBrokerOffset(String topic, int partition, int partitionCount, long offset, long timestampMs) {
        if (partition < 0) {
            return;
        }
        TopicBrokerState state = brokerTopics.computeIfAbsent(
                topic, k -> new TopicBrokerState(null, Math.max(partitionCount, partition + 1), intervals));
        state.recordOffset(partition, partitionCount, offset, timestampMs, intervals);
    }

    public void deleteTopic(String topic) {
        TopicBrokerState removed = brokerTopics.remove(topic);
        if (removed != null && removed.topicId() != null) {
            topicIdToName.remove(removed.topicId(), topic);
            pendingKip848GroupsByTopicId.remove(removed.topicId());
        }
        clearConsumerTopicState(topic, null);
    }

    private void clearConsumerTopicState(String topic, UUID retainedTopicId) {
        for (GroupState groupState : consumerGroups.values()) {
            groupState.lock.writeLock().lock();
            try {
                Map<Integer, PartitionCommitRing> partMap = groupState.topics.get(topic);
                if (partMap == null) {
                    continue;
                }
                if (retainedTopicId == null) {
                    groupState.topics.remove(topic);
                    trackedPartitionsCounter.addAndGet(-partMap.size());
                } else {
                    List<Integer> toRemove = new ArrayList<>();
                    for (Map.Entry<Integer, PartitionCommitRing> pe : partMap.entrySet()) {
                        UUID ringTopicId = pe.getValue().topicId();
                        if (ringTopicId == null || !retainedTopicId.equals(ringTopicId)) {
                            toRemove.add(pe.getKey());
                        }
                    }
                    for (Integer p : toRemove) {
                        partMap.remove(p);
                        trackedPartitionsCounter.decrementAndGet();
                    }
                    if (partMap.isEmpty()) {
                        groupState.topics.remove(topic);
                    }
                }
            } finally {
                groupState.lock.writeLock().unlock();
            }
        }
    }

    private void purgeMismatchedTopicIdCommits(String topic, UUID currentTopicId) {
        for (GroupState groupState : consumerGroups.values()) {
            groupState.lock.writeLock().lock();
            try {
                Map<Integer, PartitionCommitRing> partMap = groupState.topics.get(topic);
                if (partMap == null) {
                    continue;
                }
                List<Integer> mismatched = new ArrayList<>();
                for (Map.Entry<Integer, PartitionCommitRing> pe : partMap.entrySet()) {
                    UUID ringTopicId = pe.getValue().topicId();
                    if (ringTopicId != null && !currentTopicId.equals(ringTopicId)) {
                        mismatched.add(pe.getKey());
                    }
                }
                for (Integer p : mismatched) {
                    partMap.remove(p);
                    trackedPartitionsCounter.decrementAndGet();
                }
                if (partMap.isEmpty()) {
                    groupState.topics.remove(topic);
                }
            } finally {
                groupState.lock.writeLock().unlock();
            }
        }
    }

    public Set<String> brokerTopicNames() {
        return Set.copyOf(brokerTopics.keySet());
    }

    public long getLatestBrokerOffset(String topic, int partition) {
        TopicBrokerState state = brokerTopics.get(topic);
        return state == null ? -1L : state.latestOffset(partition);
    }

    public Map<String, long[]> snapshotAllBrokerLatestOffsets() {
        Map<String, long[]> result = new LinkedHashMap<>();
        for (Map.Entry<String, TopicBrokerState> entry : brokerTopics.entrySet()) {
            long[] latest = entry.getValue().snapshotLatestOffsets();
            if (latest.length > 0) {
                result.put(entry.getKey(), latest);
            }
        }
        return result;
    }

    // --- Consumer Offset & Metadata operations ---

    public void recordConsumerOffset(String group,
                                     String topic,
                                     int partition,
                                     long offset,
                                     long order,
                                     long commitTimestampMs,
                                     long observedTimestampMs) {
        recordConsumerOffset(group, topic, partition, offset, order, commitTimestampMs, observedTimestampMs, null);
    }

    public void recordConsumerOffset(String group,
                                     String topic,
                                     int partition,
                                     long offset,
                                     long order,
                                     long commitTimestampMs,
                                     long observedTimestampMs,
                                     UUID commitTopicId) {
        if (partition < 0 || !acceptsGroup(group)) {
            return;
        }
        long cutoff = observedTimestampMs - expireGroupMs;
        if (commitTimestampMs > 0L && commitTimestampMs < cutoff) {
            return;
        }

        // Read broker state first without holding any consumer group lock.
        // If topic metadata or first head offset is not yet available, retain the commit with NO_LAG
        // and request an asynchronous metadata refresh instead of dropping the commit (#01).
        TopicBrokerState brokerState = brokerTopics.get(topic);
        long brokerOffset = -1L;
        if (brokerState != null) {
            UUID currentTopicId = brokerState.topicId();
            if (commitTopicId != null && currentTopicId != null && !commitTopicId.equals(currentTopicId)) {
                // Fence stale v4 commit from an older topic incarnation (#05)
                return;
            }
            long recreatedAt = brokerState.recreatedAtMs();
            if (commitTopicId == null && recreatedAt > 0L && commitTimestampMs > 0L && commitTimestampMs < recreatedAt) {
                // Fence pre-recreation legacy v3 commit (#05)
                return;
            }
            brokerOffset = brokerState.latestOffset(partition);
        } else if (onUnknownTopicCommit != null) {
            onUnknownTopicCommit.run();
        }

        GroupState groupState = getOrCreateGroup(group);
        if (groupState == null) {
            return;
        }

        groupState.lock.writeLock().lock();
        try {
            if (groupState.deleted) {
                return;
            }
            PartitionCommitRing ring = getOrCreatePartitionRing(groupState, topic, partition);
            if (ring == null) {
                groupState.truncatedByCardinalityLimit = true;
                return;
            }
            if (commitTopicId != null) {
                UUID prevRingId = ring.topicId();
                if (prevRingId != null && !prevRingId.equals(commitTopicId)) {
                    // Topic recreated before metadata refresh: replace ring cleanly
                    ring = new PartitionCommitRing(intervals);
                    groupState.topics.get(topic).put(partition, ring);
                }
                ring.setTopicId(commitTopicId);
            }
            boolean appended = ring.addCommit(offset, order, commitTimestampMs, observedTimestampMs, brokerOffset, minDistanceMs);
            if (appended) {
                groupState.lastCommitMs = Math.max(groupState.lastCommitMs, commitTimestampMs > 0 ? commitTimestampMs : observedTimestampMs);
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    public void deleteConsumerPartition(String group, String topic, int partition) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        boolean removeGroup = false;
        groupState.lock.writeLock().lock();
        try {
            Map<Integer, PartitionCommitRing> partMap = groupState.topics.get(topic);
            if (partMap != null && partMap.remove(partition) != null) {
                trackedPartitionsCounter.decrementAndGet();
                if (partMap.isEmpty()) {
                    groupState.topics.remove(topic);
                }
            }
            if (groupState.topics.isEmpty() && groupState.kip848Members.isEmpty() && !groupState.truncatedByCardinalityLimit) {
                groupState.deleted = true;
                removeGroup = true;
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
        if (removeGroup) {
            consumerGroups.remove(group, groupState);
        }
    }

    /**
     * Handles Key-2 (Classic group metadata) tombstone without erasing committed partition offsets.
     * During Kafka 4.x Classic -> KIP-848 online upgrade, Kafka writes a Key-2 tombstone while keeping
     * all Key-1 offset commits intact (#02).
     */
    public void clearClassicGroupMetadata(String group) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        boolean removeEmptyGroup = false;
        groupState.lock.writeLock().lock();
        try {
            clearClassicOwnersLocked(groupState);
            if (!hasAnyCommittedOffsetsLocked(groupState) && groupState.kip848Members.isEmpty() && !groupState.truncatedByCardinalityLimit) {
                int removedCount = removeAllEmptyRingsLocked(groupState);
                trackedPartitionsCounter.addAndGet(-removedCount);
                groupState.deleted = true;
                removeEmptyGroup = true;
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
        if (removeEmptyGroup) {
            consumerGroups.remove(group, groupState);
        }
    }

    /**
     * Handles Key-3 (KIP-848 group metadata) tombstone without erasing committed partition offsets (#02).
     */
    public void clearKip848GroupMetadata(String group) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        boolean removeEmptyGroup = false;
        groupState.lock.writeLock().lock();
        try {
            for (Kip848MemberState member : groupState.kip848Members.values()) {
                clearKip848MemberOwnersLocked(groupState, member);
            }
            groupState.kip848Members.clear();
            if (!hasAnyCommittedOffsetsLocked(groupState) && !groupState.truncatedByCardinalityLimit) {
                int removedCount = removeAllEmptyRingsLocked(groupState);
                trackedPartitionsCounter.addAndGet(-removedCount);
                groupState.deleted = true;
                removeEmptyGroup = true;
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
        if (removeEmptyGroup) {
            consumerGroups.remove(group, groupState);
        }
    }

    /**
     * Authoritatively removes a consumer group and all of its committed offset windows
     * (used by expiration, filter tightening, and optional Groups Reaper).
     */
    public void deleteConsumerGroup(String group) {
        GroupState removed = consumerGroups.remove(group);
        if (removed == null) {
            return;
        }
        removed.lock.writeLock().lock();
        try {
            removed.deleted = true;
            int count = 0;
            for (Map<Integer, PartitionCommitRing> pm : removed.topics.values()) {
                count += pm.size();
            }
            removed.topics.clear();
            removed.kip848Members.clear();
            trackedPartitionsCounter.addAndGet(-count);
        } finally {
            removed.lock.writeLock().unlock();
        }
    }

    public void setConsumerOwner(String group, String topic, int partition, String owner, String clientId) {
        if (partition < 0 || !acceptsGroup(group)) {
            return;
        }
        if (!brokerTopics.containsKey(topic)) {
            return;
        }
        GroupState groupState = getOrCreateGroup(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            if (groupState.deleted) {
                return;
            }
            PartitionCommitRing ring = getOrCreatePartitionRing(groupState, topic, partition);
            if (ring != null) {
                ring.setOwner(owner, clientId);
            } else {
                groupState.truncatedByCardinalityLimit = true;
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    public void clearConsumerOwners(String group) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            clearClassicOwnersLocked(groupState);
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    // --- KIP-848 Member & Assignment correlation ---

    public void upsertKip848Member(String group, String memberId, String clientId, String clientHost) {
        if (!acceptsGroup(group)) {
            return;
        }
        GroupState groupState = getOrCreateGroup(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            if (groupState.deleted) {
                return;
            }
            Kip848MemberState member = groupState.kip848Members.computeIfAbsent(memberId, k -> new Kip848MemberState());
            member.clientId = clientId == null ? "" : clientId;
            member.clientHost = clientHost == null ? "" : clientHost;
            applyKip848MemberAssignmentsLocked(group, groupState, member);
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    public void removeKip848Member(String group, String memberId) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            Kip848MemberState removed = groupState.kip848Members.remove(memberId);
            if (removed != null) {
                clearKip848MemberOwnersLocked(groupState, removed);
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    public void updateKip848Assignment(String group, String memberId, List<TopicIdPartitions> assignedPartitions) {
        if (!acceptsGroup(group)) {
            return;
        }
        GroupState groupState = getOrCreateGroup(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            if (groupState.deleted) {
                return;
            }
            Kip848MemberState member = groupState.kip848Members.computeIfAbsent(memberId, k -> new Kip848MemberState());
            clearKip848MemberOwnersLocked(groupState, member);
            member.assigned = assignedPartitions == null ? List.of() : assignedPartitions;
            applyKip848MemberAssignmentsLocked(group, groupState, member);
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    public void clearKip848Assignment(String group, String memberId) {
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return;
        }
        groupState.lock.writeLock().lock();
        try {
            Kip848MemberState member = groupState.kip848Members.get(memberId);
            if (member != null) {
                clearKip848MemberOwnersLocked(groupState, member);
                member.assigned = List.of();
            }
        } finally {
            groupState.lock.writeLock().unlock();
        }
    }

    /**
     * Indexed O(waitingGroups) reconciliation invoked only when a TopicId -> TopicName mapping appears or changes (#16).
     */
    private void reconcilePendingKip848Assignments(UUID topicId, String topicName) {
        Set<String> waitingGroups = pendingKip848GroupsByTopicId.remove(topicId);
        if (waitingGroups == null || waitingGroups.isEmpty()) {
            return;
        }
        for (String group : waitingGroups) {
            GroupState groupState = consumerGroups.get(group);
            if (groupState == null) {
                continue;
            }
            groupState.lock.writeLock().lock();
            try {
                if (groupState.deleted) {
                    continue;
                }
                for (Kip848MemberState member : groupState.kip848Members.values()) {
                    for (TopicIdPartitions tip : member.assigned) {
                        if (topicId.equals(tip.topicId())) {
                            applyPartitionsOwnerLocked(groupState, topicName, tip.partitions(), member.clientHost, member.clientId);
                        }
                    }
                }
            } finally {
                groupState.lock.writeLock().unlock();
            }
        }
    }

    private void applyKip848MemberAssignmentsLocked(String group, GroupState groupState, Kip848MemberState member) {
        if (member.assigned.isEmpty()) {
            return;
        }
        for (TopicIdPartitions tip : member.assigned) {
            String topicName = topicIdToName.get(tip.topicId());
            if (topicName == null) {
                pendingKip848GroupsByTopicId
                        .computeIfAbsent(tip.topicId(), k -> ConcurrentHashMap.newKeySet())
                        .add(group);
            } else if (!member.clientHost.isEmpty() || !member.clientId.isEmpty()) {
                applyPartitionsOwnerLocked(groupState, topicName, tip.partitions(), member.clientHost, member.clientId);
            }
        }
    }

    private void applyPartitionsOwnerLocked(GroupState groupState, String topicName, int[] partitions, String owner, String clientId) {
        Map<Integer, PartitionCommitRing> partMap = groupState.topics.get(topicName);
        if (partMap == null) {
            return;
        }
        for (int p : partitions) {
            PartitionCommitRing ring = partMap.get(p);
            if (ring != null) {
                ring.setOwner(owner, clientId);
            }
        }
    }

    private void clearClassicOwnersLocked(GroupState groupState) {
        for (Map<Integer, PartitionCommitRing> pm : groupState.topics.values()) {
            for (PartitionCommitRing ring : pm.values()) {
                ring.clearOwner();
            }
        }
    }

    private boolean hasAnyCommittedOffsetsLocked(GroupState groupState) {
        for (Map<Integer, PartitionCommitRing> pm : groupState.topics.values()) {
            for (PartitionCommitRing ring : pm.values()) {
                if (ring.count() > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private int removeAllEmptyRingsLocked(GroupState groupState) {
        int removed = 0;
        for (Map<Integer, PartitionCommitRing> pm : groupState.topics.values()) {
            removed += pm.size();
        }
        groupState.topics.clear();
        return removed;
    }

    private void clearKip848MemberOwnersLocked(GroupState groupState, Kip848MemberState member) {
        for (TopicIdPartitions tip : member.assigned) {
            String topicName = topicIdToName.get(tip.topicId());
            if (topicName == null) {
                continue;
            }
            Map<Integer, PartitionCommitRing> partMap = groupState.topics.get(topicName);
            if (partMap == null) {
                continue;
            }
            for (int p : tip.partitions()) {
                PartitionCommitRing ring = partMap.get(p);
                if (ring != null && ring.clientId().equals(member.clientId) && ring.owner().equals(member.clientHost)) {
                    ring.clearOwner();
                }
            }
        }
    }

    // --- Queries & Expiration ---

    public Set<String> consumerGroupNames() {
        return Set.copyOf(consumerGroups.keySet());
    }

    public int trackedGroupCount() {
        return consumerGroups.size();
    }

    public int trackedPartitionCount() {
        return Math.max(0, trackedPartitionsCounter.get());
    }

    public boolean isCardinalityLimitReached() {
        return cardinalityLimitReached.get();
    }

    public List<String> purgeExpiredGroups(long nowMs) {
        long cutoff = nowMs - expireGroupMs;
        List<String> purged = new ArrayList<>();
        for (Map.Entry<String, GroupState> entry : consumerGroups.entrySet()) {
            GroupState gs = entry.getValue();
            boolean expired;
            gs.lock.readLock().lock();
            try {
                expired = gs.lastCommitMs > 0L && gs.lastCommitMs < cutoff;
            } finally {
                gs.lock.readLock().unlock();
            }
            if (expired) {
                deleteConsumerGroup(entry.getKey());
                purged.add(entry.getKey());
            }
        }
        return purged;
    }

    /**
     * Fetches a consistent snapshot of one consumer group's partitions combined with broker head offsets.
     * Returns {@code null} if the group does not exist, does not match active filters, or is expired.
     */
    public GroupSnapshot fetchConsumerSnapshot(String group, long nowMs) {
        if (!acceptsGroup(group)) {
            return null;
        }
        GroupState groupState = consumerGroups.get(group);
        if (groupState == null) {
            return null;
        }

        // Phase 1: copy group commit windows under group read lock
        Map<String, List<RawPartitionCopy>> rawTopics = new LinkedHashMap<>();
        boolean expired = false;
        boolean truncatedByLimit = false;
        groupState.lock.readLock().lock();
        try {
            if (groupState.deleted) {
                return null;
            }
            if (groupState.lastCommitMs > 0L && (nowMs - expireGroupMs) > groupState.lastCommitMs) {
                expired = true;
            } else {
                truncatedByLimit = groupState.truncatedByCardinalityLimit;
                for (Map.Entry<String, Map<Integer, PartitionCommitRing>> tEntry : groupState.topics.entrySet()) {
                    List<RawPartitionCopy> copies = new ArrayList<>(tEntry.getValue().size());
                    for (Map.Entry<Integer, PartitionCommitRing> pEntry : tEntry.getValue().entrySet()) {
                        PartitionCommitRing ring = pEntry.getValue();
                        copies.add(new RawPartitionCopy(
                                pEntry.getKey(),
                                ring.owner(),
                                ring.clientId(),
                                ring.capacity(),
                                ring.snapshotCommits()
                        ));
                    }
                    rawTopics.put(tEntry.getKey(), copies);
                }
            }
        } finally {
            groupState.lock.readLock().unlock();
        }

        if (expired) {
            deleteConsumerGroup(group);
            return null;
        }

        // Phase 2: enrich with broker offsets (never holding consumer lock simultaneously)
        Map<String, List<PartitionSnapshot>> result = new LinkedHashMap<>(rawTopics.size());
        boolean hasIncompleteTopic = false;
        for (Map.Entry<String, List<RawPartitionCopy>> entry : rawTopics.entrySet()) {
            String topic = entry.getKey();
            TopicBrokerState brokerState = brokerTopics.get(topic);
            if (brokerState == null) {
                boolean topicHasCommits = entry.getValue().stream().anyMatch(r -> r.commits.length > 0);
                if (topicHasCommits) {
                    hasIncompleteTopic = true;
                }
                continue;
            }
            List<PartitionSnapshot> partitions = new ArrayList<>(entry.getValue().size());
            boolean topicIncomplete = false;
            for (RawPartitionCopy raw : entry.getValue()) {
                long[] brokerHistory = brokerState.snapshotHistory(raw.partition);
                if (brokerHistory == null || brokerHistory.length == 0) {
                    if (raw.commits.length > 0) {
                        topicIncomplete = true;
                        break;
                    }
                    continue;
                }
                long latestBroker = brokerHistory[brokerHistory.length - 1];
                boolean hasCommit = raw.commits.length > 0;
                long currentLag = PartitionEvaluation.UNKNOWN_LAG;
                if (hasCommit) {
                    long lastCommitted = raw.commits[raw.commits.length - 1].offset();
                    currentLag = latestBroker > lastCommitted ? (latestBroker - lastCommitted) : 0L;
                }
                partitions.add(new PartitionSnapshot(
                        raw.partition,
                        raw.owner,
                        raw.clientId,
                        raw.capacity,
                        raw.commits,
                        brokerHistory,
                        currentLag,
                        hasCommit
                ));
            }
            if (topicIncomplete) {
                hasIncompleteTopic = true;
            } else if (!partitions.isEmpty()) {
                result.put(topic, Collections.unmodifiableList(partitions));
            }
        }
        return new GroupSnapshot(Collections.unmodifiableMap(result), truncatedByLimit || hasIncompleteTopic);
    }

    /**
     * Convenience accessor returning topic partitions map (or {@code null} if absent/expired).
     */
    public Map<String, List<PartitionSnapshot>> fetchConsumer(String group, long nowMs) {
        GroupSnapshot snap = fetchConsumerSnapshot(group, nowMs);
        return snap == null ? null : snap.topics();
    }

    public List<String> retainOnlyGroups(Set<String> existingKafkaGroups) {
        List<String> removed = new ArrayList<>();
        for (String group : new HashSet<>(consumerGroups.keySet())) {
            if (!existingKafkaGroups.contains(group)) {
                deleteConsumerGroup(group);
                removed.add(group);
            }
        }
        return removed;
    }

    private GroupState getOrCreateGroup(String group) {
        GroupState existing = consumerGroups.get(group);
        if (existing != null) {
            return existing;
        }
        if (consumerGroups.size() >= maxGroups) {
            cardinalityLimitReached.set(true);
            return null;
        }
        return consumerGroups.computeIfAbsent(group, k -> new GroupState());
    }

    private PartitionCommitRing getOrCreatePartitionRing(GroupState groupState, String topic, int partition) {
        Map<Integer, PartitionCommitRing> partMap = groupState.topics.computeIfAbsent(topic, k -> new HashMap<>());
        PartitionCommitRing ring = partMap.get(partition);
        if (ring != null) {
            return ring;
        }
        if (trackedPartitionsCounter.get() >= maxGroupPartitions) {
            cardinalityLimitReached.set(true);
            return null;
        }
        ring = new PartitionCommitRing(intervals);
        partMap.put(partition, ring);
        trackedPartitionsCounter.incrementAndGet();
        return ring;
    }

    private static Pattern compileOrNull(String regex) {
        return (regex == null || regex.isEmpty()) ? null : Pattern.compile(regex);
    }

    public record GroupSnapshot(
            Map<String, List<PartitionSnapshot>> topics,
            boolean incomplete
    ) {
    }

    public record PartitionSnapshot(
            int partition,
            String owner,
            String clientId,
            int windowCapacity,
            CommitSample[] commits,
            long[] brokerOffsets,
            long currentLag,
            boolean hasCommit
    ) {
    }

    private record RawPartitionCopy(
            int partition,
            String owner,
            String clientId,
            int capacity,
            CommitSample[] commits
    ) {
    }

    private static final class GroupState {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final Map<String, Map<Integer, PartitionCommitRing>> topics = new HashMap<>();
        final Map<String, Kip848MemberState> kip848Members = new HashMap<>();
        long lastCommitMs = 0L;
        boolean deleted = false;
        boolean truncatedByCardinalityLimit = false;
    }

    private static final class Kip848MemberState {
        String clientId = "";
        String clientHost = "";
        List<TopicIdPartitions> assigned = List.of();
    }

    private static final class TopicBrokerState {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private UUID topicId;
        private long recreatedAtMs = 0L;
        private BrokerOffsetRing[] partitions;

        TopicBrokerState(UUID topicId, int partitionCount, int intervals) {
            this.topicId = topicId;
            this.partitions = new BrokerOffsetRing[Math.max(0, partitionCount)];
            for (int i = 0; i < this.partitions.length; i++) {
                this.partitions[i] = new BrokerOffsetRing(intervals);
            }
        }

        UUID topicId() {
            lock.readLock().lock();
            try {
                return topicId;
            } finally {
                lock.readLock().unlock();
            }
        }

        long recreatedAtMs() {
            lock.readLock().lock();
            try {
                return recreatedAtMs;
            } finally {
                lock.readLock().unlock();
            }
        }

        boolean reconcileMetadata(UUID newTopicId, int newPartitionCount, int intervals) {
            lock.writeLock().lock();
            try {
                boolean recreated = newTopicId != null && this.topicId != null && !newTopicId.equals(this.topicId);
                if (newTopicId != null) {
                    this.topicId = newTopicId;
                }
                if (recreated) {
                    this.recreatedAtMs = System.currentTimeMillis();
                }
                if (recreated || newPartitionCount < this.partitions.length) {
                    this.partitions = new BrokerOffsetRing[Math.max(0, newPartitionCount)];
                    for (int i = 0; i < this.partitions.length; i++) {
                        this.partitions[i] = new BrokerOffsetRing(intervals);
                    }
                } else if (newPartitionCount > this.partitions.length) {
                    BrokerOffsetRing[] expanded = new BrokerOffsetRing[newPartitionCount];
                    System.arraycopy(this.partitions, 0, expanded, 0, this.partitions.length);
                    for (int i = this.partitions.length; i < newPartitionCount; i++) {
                        expanded[i] = new BrokerOffsetRing(intervals);
                    }
                    this.partitions = expanded;
                }
                return recreated;
            } finally {
                lock.writeLock().unlock();
            }
        }

        void recordOffset(int partition, int partitionCount, long offset, long timestampMs, int intervals) {
            lock.writeLock().lock();
            try {
                int targetLen = Math.max(partitionCount, partition + 1);
                if (targetLen > this.partitions.length) {
                    BrokerOffsetRing[] expanded = new BrokerOffsetRing[targetLen];
                    System.arraycopy(this.partitions, 0, expanded, 0, this.partitions.length);
                    for (int i = this.partitions.length; i < targetLen; i++) {
                        expanded[i] = new BrokerOffsetRing(intervals);
                    }
                    this.partitions = expanded;
                }
                this.partitions[partition].add(offset, timestampMs);
            } finally {
                lock.writeLock().unlock();
            }
        }

        long latestOffset(int partition) {
            lock.readLock().lock();
            try {
                if (partition < 0 || partition >= partitions.length) {
                    return -1L;
                }
                return partitions[partition].latestOffset();
            } finally {
                lock.readLock().unlock();
            }
        }

        long[] snapshotHistory(int partition) {
            lock.readLock().lock();
            try {
                if (partition < 0 || partition >= partitions.length) {
                    return null;
                }
                if (partitions[partition].isEmpty()) {
                    return null;
                }
                return partitions[partition].snapshotOffsets();
            } finally {
                lock.readLock().unlock();
            }
        }

        long[] snapshotLatestOffsets() {
            lock.readLock().lock();
            try {
                long[] out = new long[partitions.length];
                int populated = 0;
                for (int i = 0; i < partitions.length; i++) {
                    long latest = partitions[i].latestOffset();
                    out[i] = latest;
                    if (latest >= 0L) {
                        populated++;
                    }
                }
                return populated == 0 ? new long[0] : out;
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}
