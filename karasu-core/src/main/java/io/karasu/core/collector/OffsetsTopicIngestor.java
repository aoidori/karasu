/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.KafkaClientConfigBuilder;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DecodeErrorKey;
import io.karasu.core.storage.ClusterStore;
import io.karasu.protocol.decode.DecodedRecord;
import io.karasu.protocol.decode.DecodedRecord.ClassicGroupMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.ClassicGroupTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.DecodeFailureRecord;
import io.karasu.protocol.decode.DecodedRecord.IgnoredInternalRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848AssignmentTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848CurrentAssignmentRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848GroupMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848GroupTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848MemberMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848MemberTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.MemberAssignment;
import io.karasu.protocol.decode.DecodedRecord.OffsetCommitRecord;
import io.karasu.protocol.decode.DecodedRecord.OffsetTombstoneRecord;
import io.karasu.protocol.decode.OffsetsRecordDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Dedicated per-cluster {@code __consumer_offsets} reader running on dedicated daemon thread(s).
 *
 * <p>Key design guarantees:
 * <ul>
 *   <li>Strictly single-threaded ownership of each {@link KafkaConsumer}; external stop uses {@code wakeup()}.</li>
 *   <li>Per-worker independent health, catch-up, poll-freshness, and error state; reconnecting a worker
 *       always resets its catch-up flag before replaying (#04).</li>
 *   <li>Runtime internal-backlog stall detector degrades {@code offsetsCaughtUp} when any partition lags behind
 *       a watermark beyond {@link #MAX_RUNTIME_BACKLOG_STALL_MS} (#04).</li>
 *   <li>Classic/KIP-848 group metadata tombstones clear member metadata without erasing committed offsets (#02).</li>
 * </ul>
 */
public final class OffsetsTopicIngestor {

    private static final Logger log = LoggerFactory.getLogger(OffsetsTopicIngestor.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final long INITIAL_BACKOFF_MS = 2_000L;
    private static final long MAX_BACKOFF_MS = 30_000L;
    private static final long MAX_RUNTIME_BACKLOG_STALL_MS = 60_000L;

    private final ClusterSpec spec;
    private final long generation;
    private final ClusterStore store;
    private final BrokerOffsetCollector brokerCollector;
    private final OffsetsRecordDecoder decoder;
    private final Consumer<String> onGroupChanged;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final ConcurrentHashMap<Integer, Long> partitionPositions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, BacklogWatermark> partitionWatermarks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<DecodeErrorKey, AtomicLong> decodeErrors = new ConcurrentHashMap<>();
    private final Set<Short> loggedUnknownTypes = ConcurrentHashMap.newKeySet();

    private final List<Thread> workerThreads = new ArrayList<>();
    private final List<AtomicReference<KafkaConsumer<byte[], byte[]>>> consumerRefs = new ArrayList<>();
    private final WorkerState[] workerStates;

    public OffsetsTopicIngestor(ClusterSpec spec,
                                long generation,
                                ClusterStore store,
                                BrokerOffsetCollector brokerCollector,
                                Consumer<String> onGroupChanged) {
        this.spec = spec;
        this.generation = generation;
        this.store = store;
        this.brokerCollector = brokerCollector;
        this.decoder = new OffsetsRecordDecoder();
        this.onGroupChanged = onGroupChanged;
        int workers = spec.collection().offsetsConsumerThreads();
        this.workerStates = new WorkerState[workers];
        for (int i = 0; i < workers; i++) {
            this.workerStates[i] = new WorkerState();
            this.consumerRefs.add(new AtomicReference<>());
        }
    }

    public void start() {
        int workers = spec.collection().offsetsConsumerThreads();
        for (int idx = 0; idx < workers; idx++) {
            final int workerIdx = idx;
            Thread t = new Thread(
                    () -> runWorker(workerIdx, workers),
                    "karasu-offsets-" + spec.targetKey().k8sCluster() + "-" + spec.targetKey().kafkaCluster() + "-g" + generation + "-w" + workerIdx
            );
            t.setDaemon(true);
            workerThreads.add(t);
            t.start();
        }
    }

    public void stop(Duration joinTimeout) {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        for (AtomicReference<KafkaConsumer<byte[], byte[]>> ref : consumerRefs) {
            KafkaConsumer<byte[], byte[]> c = ref.get();
            if (c != null) {
                try {
                    c.wakeup();
                } catch (Exception ignored) {
                }
            }
        }
        for (Thread t : workerThreads) {
            t.interrupt();
        }
        long deadlineMs = System.currentTimeMillis() + joinTimeout.toMillis();
        for (Thread t : workerThreads) {
            long rem = deadlineMs - System.currentTimeMillis();
            if (rem <= 0L) {
                break;
            }
            try {
                t.join(rem);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public boolean isWaitingForTopic() {
        for (WorkerState ws : workerStates) {
            if (ws.waitingForTopic.get()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true iff EVERY worker has completed initial/reconnect replay catch-up AND
     * no assigned {@code __consumer_offsets} partition has remained stalled behind a watermark
     * longer than {@link #MAX_RUNTIME_BACKLOG_STALL_MS}.
     */
    public boolean isCaughtUp() {
        return isCaughtUp(System.currentTimeMillis());
    }

    boolean isCaughtUp(long nowMs) {
        for (WorkerState ws : workerStates) {
            if (!ws.caughtUp.get()) {
                return false;
            }
        }
        return !hasRuntimeBacklogStall(nowMs);
    }

    /**
     * Returns true iff EVERY worker is currently healthy and has polled within freshness bounds.
     */
    public boolean isHealthy() {
        return isHealthy(System.currentTimeMillis());
    }

    boolean isHealthy(long nowMs) {
        long maxPollSilenceMs = Math.max(spec.collection().requestTimeoutMs() * 3L, 30_000L);
        for (WorkerState ws : workerStates) {
            if (!ws.healthy.get()) {
                return false;
            }
            long lastPoll = ws.lastPollSuccessMs.get();
            if (lastPoll > 0L && (nowMs - lastPoll) > maxPollSilenceMs) {
                return false;
            }
        }
        return true;
    }

    public String lastError() {
        for (WorkerState ws : workerStates) {
            String err = ws.lastError.get();
            if (err != null && !err.isEmpty()) {
                return err;
            }
        }
        return "";
    }

    public long computeInternalOffsetsLag() {
        String topic = spec.collection().offsetsTopic();
        long totalLag = 0L;
        for (Map.Entry<Integer, Long> entry : partitionPositions.entrySet()) {
            long head = store.getLatestBrokerOffset(topic, entry.getKey());
            if (head > entry.getValue()) {
                totalLag += (head - entry.getValue());
            }
        }
        return totalLag;
    }

    public Map<DecodeErrorKey, Long> snapshotDecodeErrors() {
        Map<DecodeErrorKey, Long> out = new HashMap<>();
        for (Map.Entry<DecodeErrorKey, AtomicLong> e : decodeErrors.entrySet()) {
            out.put(e.getKey(), e.getValue().get());
        }
        return out;
    }

    // Package-private test hooks for worker state and runtime backlog verification (#04)
    void markWorkerStateForTest(int workerIdx, boolean caughtUp, boolean healthy, long lastPollMs, String error) {
        WorkerState ws = workerStates[workerIdx];
        ws.caughtUp.set(caughtUp);
        ws.healthy.set(healthy);
        ws.lastPollSuccessMs.set(lastPollMs);
        ws.lastError.set(error == null ? "" : error);
    }

    void updatePartitionPositionForTest(int partition, long position) {
        partitionPositions.put(partition, position);
    }

    private boolean hasRuntimeBacklogStall(long nowMs) {
        String topic = spec.collection().offsetsTopic();
        boolean stalled = false;
        for (Map.Entry<Integer, Long> entry : partitionPositions.entrySet()) {
            int part = entry.getKey();
            long pos = entry.getValue();
            long head = store.getLatestBrokerOffset(topic, part);
            if (head <= pos) {
                partitionWatermarks.remove(part);
                continue;
            }
            BacklogWatermark wm = partitionWatermarks.get(part);
            if (wm == null || pos >= wm.targetHead) {
                // Caught up to previous watermark; record new watermark at current head
                partitionWatermarks.put(part, new BacklogWatermark(head, nowMs));
            } else if ((nowMs - wm.observedAtMs) > MAX_RUNTIME_BACKLOG_STALL_MS) {
                stalled = true;
            }
        }
        return stalled;
    }

    private void runWorker(int workerIdx, int totalWorkers) {
        WorkerState ws = workerStates[workerIdx];
        try (MDC.MDCCloseable c1 = MDC.putCloseable("k8s_cluster", spec.targetKey().k8sCluster());
             MDC.MDCCloseable c2 = MDC.putCloseable("kafka_cluster", spec.targetKey().kafkaCluster());
             MDC.MDCCloseable c3 = MDC.putCloseable("gen", Long.toString(generation))) {

            long backoffMs = INITIAL_BACKOFF_MS;
            while (!stopping.get()) {
                // Always reset caughtUp when establishing or re-establishing a consumer session (#04)
                ws.caughtUp.set(false);
                KafkaConsumer<byte[], byte[]> consumer = null;
                try {
                    waitForInitialBrokerMetadata();
                    if (stopping.get()) {
                        return;
                    }

                    Properties props = KafkaClientConfigBuilder.buildOffsetsConsumerProperties(spec, generation, workerIdx);
                    consumer = new KafkaConsumer<>(props);
                    consumerRefs.get(workerIdx).set(consumer);

                    String offsetsTopic = spec.collection().offsetsTopic();
                    Duration apiTimeout = Duration.ofMillis(spec.collection().requestTimeoutMs());
                    List<PartitionInfo> infos = consumer.partitionsFor(offsetsTopic, apiTimeout);
                    if (infos == null || infos.isEmpty()) {
                        ws.waitingForTopic.set(true);
                        ws.healthy.set(false);
                        ws.lastError.set("offsets_topic_unavailable: " + offsetsTopic);
                        log.warn("Internal topic '{}' not yet available; retrying in {}ms", offsetsTopic, backoffMs);
                        closeQuietly(consumer, workerIdx);
                        consumer = null;
                        sleepCancellable(backoffMs);
                        backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2);
                        continue;
                    }

                    ws.waitingForTopic.set(false);
                    backoffMs = INITIAL_BACKOFF_MS;

                    List<TopicPartition> myPartitions = slicePartitions(infos, offsetsTopic, workerIdx, totalWorkers);
                    if (myPartitions.isEmpty()) {
                        ws.caughtUp.set(true);
                        ws.healthy.set(true);
                        ws.lastPollSuccessMs.set(System.currentTimeMillis());
                        sleepCancellable(5_000L);
                        continue;
                    }

                    consumer.assign(myPartitions);
                    Map<TopicPartition, Long> replayTargets = new HashMap<>();
                    if (spec.collection().startMode() == ClusterSpec.StartMode.LATEST_ONLY) {
                        consumer.seekToEnd(myPartitions);
                        Map<TopicPartition, Long> ends = consumer.endOffsets(myPartitions, apiTimeout);
                        for (TopicPartition tp : myPartitions) {
                            partitionPositions.put(tp.partition(), ends.getOrDefault(tp, 0L));
                        }
                        ws.caughtUp.set(true);
                    } else {
                        Map<TopicPartition, Long> begins = consumer.beginningOffsets(myPartitions, apiTimeout);
                        Map<TopicPartition, Long> ends = consumer.endOffsets(myPartitions, apiTimeout);
                        consumer.seekToBeginning(myPartitions);
                        for (TopicPartition tp : myPartitions) {
                            long begin = begins.getOrDefault(tp, 0L);
                            long end = ends.getOrDefault(tp, 0L);
                            partitionPositions.put(tp.partition(), begin);
                            partitionWatermarks.remove(tp.partition());
                            if (end > begin) {
                                replayTargets.put(tp, end);
                            }
                        }
                        if (replayTargets.isEmpty()) {
                            ws.caughtUp.set(true);
                        }
                    }

                    ws.healthy.set(true);
                    ws.lastError.set("");
                    ws.lastPollSuccessMs.set(System.currentTimeMillis());
                    log.info("Started __consumer_offsets worker {} on {} partitions (replayPending={})",
                            workerIdx, myPartitions.size(), replayTargets.size());

                    while (!stopping.get()) {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                        long nowMs = System.currentTimeMillis();
                        for (ConsumerRecord<byte[], byte[]> rec : records) {
                            handleRecord(rec, nowMs);
                            partitionPositions.put(rec.partition(), rec.offset() + 1L);
                        }
                        if (!replayTargets.isEmpty()) {
                            checkReplayProgress(consumer, replayTargets, ws, workerIdx);
                        } else if (!ws.caughtUp.get()) {
                            ws.caughtUp.set(true);
                        }
                        for (TopicPartition tp : myPartitions) {
                            long pos = consumer.position(tp, Duration.ofSeconds(1));
                            partitionPositions.put(tp.partition(), pos);
                        }
                        ws.healthy.set(true);
                        ws.lastError.set("");
                        ws.lastPollSuccessMs.set(nowMs);
                    }
                } catch (WakeupException we) {
                    if (stopping.get()) {
                        log.debug("Offsets consumer worker {} woken up for shutdown", workerIdx);
                        return;
                    }
                } catch (UnknownTopicOrPartitionException utpe) {
                    ws.caughtUp.set(false);
                    ws.waitingForTopic.set(true);
                    ws.healthy.set(false);
                    ws.lastError.set("offsets_topic_missing: " + safeMsg(utpe));
                    log.warn("Internal offsets topic missing; backing off {}ms", backoffMs);
                    sleepCancellable(backoffMs);
                    backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2);
                } catch (AuthenticationException | AuthorizationException authEx) {
                    ws.caughtUp.set(false);
                    ws.healthy.set(false);
                    ws.lastError.set("auth_or_acl_error: " + authEx.getClass().getSimpleName() + ": " + safeMsg(authEx));
                    log.error("Offsets consumer authentication/ACL denied: {}", ws.lastError.get());
                    sleepCancellable(backoffMs);
                    backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2);
                } catch (Exception e) {
                    if (stopping.get()) {
                        return;
                    }
                    ws.caughtUp.set(false);
                    ws.healthy.set(false);
                    ws.lastError.set("consume_error: " + e.getClass().getSimpleName() + ": " + safeMsg(e));
                    log.warn("Offsets consumer worker {} error ({}); retrying in {}ms", workerIdx, ws.lastError.get(), backoffMs);
                    sleepCancellable(backoffMs);
                    backoffMs = Math.min(MAX_BACKOFF_MS, backoffMs * 2);
                } finally {
                    closeQuietly(consumer, workerIdx);
                }
            }
        }
    }

    private void checkReplayProgress(KafkaConsumer<byte[], byte[]> consumer,
                                     Map<TopicPartition, Long> replayTargets,
                                     WorkerState ws,
                                     int workerIdx) {
        List<TopicPartition> done = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : replayTargets.entrySet()) {
            long pos = consumer.position(entry.getKey(), Duration.ofSeconds(1));
            partitionPositions.put(entry.getKey().partition(), pos);
            if (pos >= entry.getValue()) {
                done.add(entry.getKey());
            }
        }
        for (TopicPartition tp : done) {
            replayTargets.remove(tp);
        }
        if (replayTargets.isEmpty()) {
            ws.caughtUp.set(true);
            log.info("Offsets worker {} completed replay catch-up", workerIdx);
        }
    }

    void handleRecord(ConsumerRecord<byte[], byte[]> rec, long observedMs) {
        DecodedRecord decoded = decoder.decode(rec.key(), rec.value());
        if (decoded instanceof OffsetCommitRecord commit) {
            store.recordConsumerOffset(
                    commit.group(),
                    commit.topic(),
                    commit.partition(),
                    commit.offset(),
                    rec.offset(),
                    commit.commitTimestampMs(),
                    observedMs,
                    commit.topicId()
            );
        } else if (decoded instanceof OffsetTombstoneRecord tombstone) {
            store.deleteConsumerPartition(tombstone.group(), tombstone.topic(), tombstone.partition());
            notifyGroupChanged(tombstone.group());
        } else if (decoded instanceof ClassicGroupMetadataRecord meta) {
            if ("consumer".equals(meta.protocolType())) {
                if (meta.members().isEmpty()) {
                    store.clearConsumerOwners(meta.group());
                } else {
                    for (MemberAssignment member : meta.members()) {
                        for (Map.Entry<String, int[]> entry : member.assignment().entrySet()) {
                            for (int partition : entry.getValue()) {
                                store.setConsumerOwner(meta.group(), entry.getKey(), partition, member.clientHost(), member.clientId());
                            }
                        }
                    }
                }
            }
        } else if (decoded instanceof ClassicGroupTombstoneRecord classicTombstone) {
            // Preserve committed offsets across Classic -> KIP-848 online protocol migration (#02)
            store.clearClassicGroupMetadata(classicTombstone.group());
            notifyGroupChanged(classicTombstone.group());
        } else if (decoded instanceof Kip848GroupTombstoneRecord kip848Tombstone) {
            store.clearKip848GroupMetadata(kip848Tombstone.group());
            notifyGroupChanged(kip848Tombstone.group());
        } else if (decoded instanceof Kip848GroupMetadataRecord) {
            // Group tracked via member/offset records
        } else if (decoded instanceof Kip848MemberMetadataRecord member) {
            store.upsertKip848Member(member.group(), member.memberId(), member.clientId(), member.clientHost());
        } else if (decoded instanceof Kip848MemberTombstoneRecord memberTombstone) {
            store.removeKip848Member(memberTombstone.group(), memberTombstone.memberId());
        } else if (decoded instanceof Kip848CurrentAssignmentRecord assign) {
            store.updateKip848Assignment(assign.group(), assign.memberId(), assign.assignedPartitions());
        } else if (decoded instanceof Kip848AssignmentTombstoneRecord assignTombstone) {
            store.clearKip848Assignment(assignTombstone.group(), assignTombstone.memberId());
        } else if (decoded instanceof IgnoredInternalRecord) {
            // Silently recognized auxiliary record (KIP-848 aux / Share / Streams)
        } else if (decoded instanceof DecodeFailureRecord fail) {
            DecodeErrorKey key = new DecodeErrorKey(Short.toString(fail.recordType()), fail.reason());
            decodeErrors.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
            if ("unsupported_key_version".equals(fail.reason())) {
                if (loggedUnknownTypes.add(fail.recordType())) {
                    log.warn("Encountered unsupported __consumer_offsets key version {} (logging once per cluster runtime)", fail.recordType());
                }
            } else {
                log.debug("Failed to decode __consumer_offsets record type={}: {}", fail.recordType(), fail.reason());
            }
        }
    }

    private void notifyGroupChanged(String group) {
        if (onGroupChanged != null) {
            onGroupChanged.accept(group);
        }
    }

    private void waitForInitialBrokerMetadata() {
        long deadline = System.currentTimeMillis() + Math.min(spec.collection().requestTimeoutMs(), 10_000L);
        while (!stopping.get() && !brokerCollector.isInitialMetadataLoaded() && System.currentTimeMillis() < deadline) {
            sleepCancellable(200L);
        }
    }

    private List<TopicPartition> slicePartitions(List<PartitionInfo> infos, String topic, int workerIdx, int totalWorkers) {
        List<Integer> sorted = infos.stream()
                .map(PartitionInfo::partition)
                .sorted(Comparator.naturalOrder())
                .toList();
        List<TopicPartition> slice = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++) {
            if (i % totalWorkers == workerIdx) {
                slice.add(new TopicPartition(topic, sorted.get(i)));
            }
        }
        return slice;
    }

    private void closeQuietly(KafkaConsumer<byte[], byte[]> consumer, int workerIdx) {
        consumerRefs.get(workerIdx).set(null);
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(3));
            } catch (Exception e) {
                log.debug("Error closing offsets KafkaConsumer worker {}: {}", workerIdx, e.getMessage());
            }
        }
    }

    private void sleepCancellable(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static String safeMsg(Throwable t) {
        return t.getMessage() == null ? "" : t.getMessage();
    }

    private static final class WorkerState {
        final AtomicBoolean caughtUp = new AtomicBoolean(false);
        final AtomicBoolean healthy = new AtomicBoolean(false);
        final AtomicBoolean waitingForTopic = new AtomicBoolean(false);
        final AtomicLong lastPollSuccessMs = new AtomicLong(0L);
        final AtomicReference<String> lastError = new AtomicReference<>("");
    }

    private record BacklogWatermark(long targetHead, long observedAtMs) {
    }
}
