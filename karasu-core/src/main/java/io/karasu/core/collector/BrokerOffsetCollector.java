/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.KafkaClientConfigBuilder;
import io.karasu.core.storage.ClusterStore;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Periodically refreshes Kafka topic metadata (including Kafka 3.9+/4.x TopicId UUIDs),
 * fetches partition log-end offsets in bounded batches, detects deleted/recreated topics,
 * and tracks per-cycle completeness health.
 */
public final class BrokerOffsetCollector {

    private static final Logger log = LoggerFactory.getLogger(BrokerOffsetCollector.class);
    private static final int DESCRIBE_BATCH_SIZE = 200;
    private static final int LIST_OFFSETS_BATCH_SIZE = 2000;

    private final ClusterSpec initialSpec;
    private final long generation;
    private final ClusterStore store;
    private final Runnable onTopicDeletedOrReaped;

    private volatile ClusterSpec activeSpec;
    private volatile Pattern topicAllowlist;
    private volatile Pattern topicDenylist;
    private volatile Admin adminClient;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean forceMetadataRefresh = new AtomicBoolean(true);
    private final AtomicBoolean initialMetadataLoaded = new AtomicBoolean(false);
    private final AtomicBoolean lastCollectionSuccess = new AtomicBoolean(false);
    private final AtomicLong lastCollectionSuccessEpochSec = new AtomicLong(0L);
    private final AtomicBoolean reachable = new AtomicBoolean(true);
    private final AtomicReference<String> lastError = new AtomicReference<>("");
    private final AtomicLong lastMetadataRefreshMs = new AtomicLong(0L);
    private final AtomicLong lastReaperRunMs = new AtomicLong(0L);
    private final AtomicLong lastImmediateRefreshTriggerMs = new AtomicLong(0L);
    private volatile java.util.concurrent.ScheduledExecutorService asyncScheduler;

    // Current known online partitions per topic (updated only on single-threaded poll ticks)
    private volatile Map<String, List<Integer>> knownOnlinePartitions = Map.of();
    private volatile Map<String, Integer> knownPartitionCounts = Map.of();

    public BrokerOffsetCollector(ClusterSpec spec,
                                 long generation,
                                 ClusterStore store,
                                 Runnable onTopicDeletedOrReaped) {
        this.initialSpec = spec;
        this.activeSpec = spec;
        this.generation = generation;
        this.store = store;
        this.onTopicDeletedOrReaped = onTopicDeletedOrReaped;
        applyPolicy(spec);
    }

    public void attachScheduler(java.util.concurrent.ScheduledExecutorService scheduler) {
        this.asyncScheduler = scheduler;
    }

    /**
     * Requests a debounced immediate metadata + head-offset refresh when a commit arrives
     * for a newly created topic not yet present in the metadata snapshot (#01).
     */
    public void requestImmediateMetadataRefresh() {
        forceMetadataRefresh.set(true);
        if (closed.get() || asyncScheduler == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long prev = lastImmediateRefreshTriggerMs.get();
        if (now - prev >= 2_000L && lastImmediateRefreshTriggerMs.compareAndSet(prev, now)) {
            asyncScheduler.execute(this::collectOnce);
        }
    }

    public void applyPolicy(ClusterSpec spec) {
        this.activeSpec = spec;
        this.topicAllowlist = compileOrNull(spec.filters().topicAllowlist());
        this.topicDenylist = compileOrNull(spec.filters().topicDenylist());
    }

    public boolean isInitialMetadataLoaded() {
        return initialMetadataLoaded.get();
    }

    public boolean isLastCollectionSuccess() {
        return lastCollectionSuccess.get();
    }

    public long lastCollectionSuccessEpochSec() {
        return lastCollectionSuccessEpochSec.get();
    }

    public boolean isReachable() {
        return reachable.get();
    }

    public String lastError() {
        return lastError.get();
    }

    /**
     * Executes one bounded head-offset collection cycle (refreshing topic metadata if due).
     * Never throws; all errors are isolated and reflected in per-cluster health metrics.
     */
    public synchronized void collectOnce() {
        if (closed.get()) {
            return;
        }
        ClusterSpec spec = this.activeSpec;
        try (MDC.MDCCloseable c1 = MDC.putCloseable("k8s_cluster", spec.targetKey().k8sCluster());
             MDC.MDCCloseable c2 = MDC.putCloseable("kafka_cluster", spec.targetKey().kafkaCluster());
             MDC.MDCCloseable c3 = MDC.putCloseable("gen", Long.toString(generation))) {

            Admin admin = ensureAdminClient(spec);
            if (admin == null) {
                lastCollectionSuccess.set(false);
                reachable.set(false);
                return;
            }

            long nowMs = System.currentTimeMillis();
            long metadataIntervalMs = spec.collection().topicRefreshSeconds() * 1000L;
            if (forceMetadataRefresh.get() || (nowMs - lastMetadataRefreshMs.get()) >= metadataIntervalMs) {
                boolean metaOk = refreshMetadata(admin, spec, nowMs);
                if (!metaOk) {
                    lastCollectionSuccess.set(false);
                    return;
                }
            }

            fetchLatestOffsets(admin, spec);

            int reaperSec = spec.collection().groupsReaperSeconds();
            if (reaperSec > 0 && (nowMs - lastReaperRunMs.get()) >= (reaperSec * 1000L)) {
                reapDeletedGroups(admin, spec, nowMs);
            }
        } catch (Throwable t) {
            log.error("Unexpected error in BrokerOffsetCollector cycle: {}", t.getMessage(), t);
            lastCollectionSuccess.set(false);
            lastError.set(t.getClass().getSimpleName() + ": " + safeMsg(t));
        }
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Admin admin = this.adminClient;
        this.adminClient = null;
        if (admin != null) {
            try {
                admin.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("Error closing AdminClient for {}: {}", initialSpec.targetKey().canonicalId(), e.getMessage());
            }
        }
    }

    private Admin ensureAdminClient(ClusterSpec spec) {
        if (adminClient != null) {
            return adminClient;
        }
        try {
            Properties props = KafkaClientConfigBuilder.buildAdminProperties(spec, generation);
            adminClient = Admin.create(props);
            return adminClient;
        } catch (Exception e) {
            log.error("Failed to create AdminClient: {}", e.getMessage());
            lastError.set("admin_init_failed: " + safeMsg(e));
            return null;
        }
    }

    private boolean refreshMetadata(Admin admin, ClusterSpec spec, long nowMs) {
        int timeoutMs = spec.collection().requestTimeoutMs();
        String offsetsTopic = spec.collection().offsetsTopic();
        try {
            Set<String> allTopics = admin.listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(timeoutMs))
                    .names()
                    .get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);

            List<String> filtered = new ArrayList<>();
            for (String topic : allTopics) {
                if (offsetsTopic.equals(topic) || acceptsTopic(topic)) {
                    filtered.add(topic);
                }
            }

            Map<String, List<Integer>> nextOnline = new HashMap<>();
            Map<String, Integer> nextCounts = new HashMap<>();

            boolean recreatedAny = false;
            for (int i = 0; i < filtered.size(); i += DESCRIBE_BATCH_SIZE) {
                List<String> batch = filtered.subList(i, Math.min(filtered.size(), i + DESCRIBE_BATCH_SIZE));
                Map<String, TopicDescription> described = admin.describeTopics(
                                batch, new DescribeTopicsOptions().timeoutMs(timeoutMs))
                        .allTopicNames()
                        .get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);

                for (Map.Entry<String, TopicDescription> entry : described.entrySet()) {
                    String topic = entry.getKey();
                    TopicDescription td = entry.getValue();
                    UUID javaUuid = toJavaUuid(td.topicId());
                    int partCount = td.partitions().size();
                    if (store.updateTopicMetadata(topic, javaUuid, partCount)) {
                        recreatedAny = true;
                    }

                    List<Integer> onlineParts = new ArrayList<>(partCount);
                    for (TopicPartitionInfo pInfo : td.partitions()) {
                        if (pInfo.leader() != null && !pInfo.leader().isEmpty()) {
                            onlineParts.add(pInfo.partition());
                        }
                    }
                    nextOnline.put(topic, onlineParts);
                    nextCounts.put(topic, partCount);
                }
            }

            // Detect deleted topics
            Set<String> previousTopics = new HashSet<>(knownPartitionCounts.keySet());
            boolean deletedAny = false;
            for (String oldTopic : previousTopics) {
                if (!nextCounts.containsKey(oldTopic)) {
                    store.deleteTopic(oldTopic);
                    deletedAny = true;
                }
            }
            if ((deletedAny || recreatedAny) && onTopicDeletedOrReaped != null) {
                onTopicDeletedOrReaped.run();
            }

            this.knownOnlinePartitions = nextOnline;
            this.knownPartitionCounts = nextCounts;
            this.lastMetadataRefreshMs.set(nowMs);
            this.forceMetadataRefresh.set(false);
            this.initialMetadataLoaded.set(true);
            this.reachable.set(true);
            return true;
        } catch (Exception e) {
            Throwable root = unwrap(e);
            if (root instanceof AuthenticationException || root instanceof AuthorizationException) {
                reachable.set(false);
            } else {
                reachable.set(false);
            }
            lastError.set("metadata_refresh_failed: " + root.getClass().getSimpleName() + ": " + safeMsg(root));
            log.warn("Metadata refresh failed: {}", lastError.get());
            forceMetadataRefresh.set(true);
            return false;
        }
    }

    private void fetchLatestOffsets(Admin admin, ClusterSpec spec) {
        Map<String, List<Integer>> online = this.knownOnlinePartitions;
        Map<String, Integer> counts = this.knownPartitionCounts;
        if (online.isEmpty()) {
            lastCollectionSuccess.set(false);
            return;
        }

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : online.entrySet()) {
            for (int p : entry.getValue()) {
                allPartitions.add(new TopicPartition(entry.getKey(), p));
            }
        }
        int expected = allPartitions.size();
        if (expected == 0) {
            lastCollectionSuccess.set(false);
            return;
        }

        int timeoutMs = spec.collection().requestTimeoutMs();
        IsolationLevel isolation = spec.collection().isolationLevel() == ClusterSpec.IsolationLevelConfig.READ_COMMITTED
                ? IsolationLevel.READ_COMMITTED
                : IsolationLevel.READ_UNCOMMITTED;
        ListOffsetsOptions options = new ListOffsetsOptions(isolation).timeoutMs(timeoutMs);

        int succeeded = 0;
        boolean hadError = false;
        long sampleTs = System.currentTimeMillis();

        for (int i = 0; i < allPartitions.size(); i += LIST_OFFSETS_BATCH_SIZE) {
            List<TopicPartition> batch = allPartitions.subList(i, Math.min(allPartitions.size(), i + LIST_OFFSETS_BATCH_SIZE));
            Map<TopicPartition, OffsetSpec> request = new HashMap<>(batch.size() * 2);
            for (TopicPartition tp : batch) {
                request.put(tp, OffsetSpec.latest());
            }
            try {
                Map<TopicPartition, ListOffsetsResultInfo> offsets = admin.listOffsets(request, options)
                        .all()
                        .get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);

                for (Map.Entry<TopicPartition, ListOffsetsResultInfo> entry : offsets.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ListOffsetsResultInfo info = entry.getValue();
                    if (info != null && info.offset() >= 0L) {
                        int partCount = counts.getOrDefault(tp.topic(), tp.partition() + 1);
                        store.recordBrokerOffset(tp.topic(), tp.partition(), partCount, info.offset(), sampleTs);
                        succeeded++;
                    } else {
                        hadError = true;
                    }
                }
            } catch (Exception e) {
                hadError = true;
                Throwable root = unwrap(e);
                lastError.set("list_offsets_failed: " + root.getClass().getSimpleName() + ": " + safeMsg(root));
                log.warn("ListOffsets batch failed: {}", lastError.get());
                forceMetadataRefresh.set(true);
            }
        }

        boolean fullSuccess = !hadError && succeeded == expected;
        lastCollectionSuccess.set(fullSuccess);
        if (fullSuccess) {
            lastCollectionSuccessEpochSec.set(System.currentTimeMillis() / 1000L);
            reachable.set(true);
            lastError.set("");
        } else if (succeeded == 0) {
            reachable.set(false);
        }
    }

    @SuppressWarnings("deprecation")
    private void reapDeletedGroups(Admin admin, ClusterSpec spec, long nowMs) {
        int timeoutMs = spec.collection().requestTimeoutMs();
        try {
            Set<String> kafkaGroups = new HashSet<>();
            try {
                Collection<GroupListing> groups = admin.listGroups(new ListGroupsOptions().timeoutMs(timeoutMs))
                        .all()
                        .get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);
                for (GroupListing gl : groups) {
                    kafkaGroups.add(gl.groupId());
                }
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof UnsupportedVersionException) {
                    var legacy = admin.listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs(timeoutMs))
                            .all()
                            .get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);
                    legacy.forEach(g -> kafkaGroups.add(g.groupId()));
                } else {
                    throw ee;
                }
            }
            List<String> removed = store.retainOnlyGroups(kafkaGroups);
            if (!removed.isEmpty()) {
                log.info("Groups reaper removed {} non-existing groups", removed.size());
                if (onTopicDeletedOrReaped != null) {
                    onTopicDeletedOrReaped.run();
                }
            }
            lastReaperRunMs.set(nowMs);
        } catch (Exception e) {
            log.warn("Groups reaper failed: {}", unwrap(e).getMessage());
        }
    }

    private boolean acceptsTopic(String topic) {
        Pattern allow = this.topicAllowlist;
        if (allow != null && !allow.matcher(topic).find()) {
            return false;
        }
        Pattern deny = this.topicDenylist;
        return deny == null || !deny.matcher(topic).find();
    }

    private static UUID toJavaUuid(Uuid kafkaUuid) {
        if (kafkaUuid == null || Uuid.ZERO_UUID.equals(kafkaUuid)) {
            return null;
        }
        return new UUID(kafkaUuid.getMostSignificantBits(), kafkaUuid.getLeastSignificantBits());
    }

    private static Pattern compileOrNull(String regex) {
        return (regex == null || regex.isEmpty()) ? null : Pattern.compile(regex);
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
    }

    private static String safeMsg(Throwable t) {
        return t.getMessage() == null ? "" : t.getMessage();
    }
}
