/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app.config;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.ClusterSpec.AuthSpec;
import io.karasu.core.config.ClusterSpec.CollectionSpec;
import io.karasu.core.config.ClusterSpec.EvaluationSpec;
import io.karasu.core.config.ClusterSpec.FilterSpec;
import io.karasu.core.config.ClusterSpec.IsolationLevelConfig;
import io.karasu.core.config.ClusterSpec.LimitsSpec;
import io.karasu.core.config.ClusterSpec.StartMode;
import io.karasu.core.config.ClusterSpec.SwapPolicy;
import io.karasu.core.config.ClusterSpec.TlsSpec;
import io.karasu.protocol.model.ClusterTargetKey;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Strongly-typed Spring Boot {@code application.yml} configuration binding for {@code karasu.*}.
 */
@ConfigurationProperties(prefix = "karasu")
public class KarasuProperties {

    private Config config = new Config();
    private Metrics metrics = new Metrics();
    private Defaults defaults = new Defaults();
    private List<ClusterItem> clusters = new ArrayList<>();

    public Config getConfig() { return config; }
    public void setConfig(Config config) { this.config = config; }
    public Metrics getMetrics() { return metrics; }
    public void setMetrics(Metrics metrics) { this.metrics = metrics; }
    public Defaults getDefaults() { return defaults; }
    public void setDefaults(Defaults defaults) { this.defaults = defaults; }
    public List<ClusterItem> getClusters() { return clusters; }
    public void setClusters(List<ClusterItem> clusters) { this.clusters = clusters; }

    public ClusterSpec toClusterSpec(ClusterItem item) {
        ClusterTargetKey key = new ClusterTargetKey(item.getK8sCluster(), item.getKafkaCluster());

        AuthItem a = item.getAuth();
        AuthSpec auth = a == null
                ? AuthSpec.PLAINTEXT_NONE
                : new AuthSpec(a.getSecurityProtocol(), a.getSaslMechanism(),
                a.getUsername(), a.getPassword(), a.getUsernameFile(), a.getPasswordFile());

        TlsItem t = item.getTls();
        TlsSpec tls = t == null
                ? TlsSpec.DISABLED
                : new TlsSpec(t.isEnabled(), t.getCaFile(), t.getCertFile(), t.getKeyFile(), t.getEndpointIdentificationAlgorithm());

        CollectionItem dc = defaults.getCollection();
        CollectionItem c = item.getCollection();
        CollectionSpec collection = new CollectionSpec(
                pick(c != null ? c.getOffsetsTopic() : null, dc.getOffsetsTopic()),
                pick(c != null ? c.getOffsetRefreshSeconds() : null, dc.getOffsetRefreshSeconds()),
                pick(c != null ? c.getTopicRefreshSeconds() : null, dc.getTopicRefreshSeconds()),
                pick(c != null ? c.getSnapshotRefreshSeconds() : null, metrics.getSnapshotRefreshSeconds()),
                pick(c != null ? c.getStartMode() : null, dc.getStartMode()),
                pick(c != null ? c.getIsolationLevel() : null, dc.getIsolationLevel()),
                pick(c != null ? c.getOffsetsConsumerThreads() : null, dc.getOffsetsConsumerThreads()),
                pick(c != null ? c.getGroupsReaperSeconds() : null, dc.getGroupsReaperSeconds()),
                pick(c != null ? c.getSwapPolicy() : null, dc.getSwapPolicy()),
                pick(c != null ? c.getRequestTimeoutMs() : null, dc.getRequestTimeoutMs()),
                pick(c != null ? c.getFetchMaxBytes() : null, dc.getFetchMaxBytes()),
                pick(c != null ? c.getMaxPollRecords() : null, dc.getMaxPollRecords())
        );

        EvaluationItem de = defaults.getEvaluation();
        EvaluationItem e = item.getEvaluation();
        EvaluationSpec evaluation = new EvaluationSpec(
                pick(e != null ? e.getIntervals() : null, de.getIntervals()),
                pick(e != null ? e.getMinDistanceSeconds() : null, de.getMinDistanceSeconds()),
                pick(e != null ? e.getExpireGroupSeconds() : null, de.getExpireGroupSeconds()),
                pick(e != null ? e.getAllowedLag() : null, de.getAllowedLag()),
                pick(e != null ? e.getMinimumComplete() : null, de.getMinimumComplete()),
                pick(e != null ? e.getCacheExpireSeconds() : null, de.getCacheExpireSeconds())
        );

        FilterItem f = item.getFilters();
        FilterSpec filters = f == null
                ? FilterSpec.DEFAULT
                : new FilterSpec(f.getGroupAllowlist(), f.getGroupDenylist(),
                f.getTopicAllowlist(), f.getTopicDenylist(),
                f.getExportTopicPartitionOffset() == null || f.getExportTopicPartitionOffset());

        LimitsItem dl = defaults.getLimits();
        LimitsItem l = item.getLimits();
        LimitsSpec limits = new LimitsSpec(
                pick(l != null ? l.getMaxGroups() : null, dl.getMaxGroups()),
                pick(l != null ? l.getMaxGroupPartitions() : null, dl.getMaxGroupPartitions())
        );

        return new ClusterSpec(
                key,
                item.getBootstrapServers(),
                item.getLabels(),
                auth,
                tls,
                collection,
                evaluation,
                filters,
                limits,
                item.getExtraKafkaProperties()
        );
    }

    private static <T> T pick(T override, T fallback) {
        return override != null ? override : fallback;
    }

    public static class Config {
        private boolean watchEnabled = true;
        private int watchIntervalSeconds = 15;
        private List<String> additionalClusterDirs = new ArrayList<>();

        public boolean isWatchEnabled() { return watchEnabled; }
        public void setWatchEnabled(boolean watchEnabled) { this.watchEnabled = watchEnabled; }
        public int getWatchIntervalSeconds() { return watchIntervalSeconds; }
        public void setWatchIntervalSeconds(int watchIntervalSeconds) { this.watchIntervalSeconds = watchIntervalSeconds; }
        public List<String> getAdditionalClusterDirs() { return additionalClusterDirs; }
        public void setAdditionalClusterDirs(List<String> additionalClusterDirs) { this.additionalClusterDirs = additionalClusterDirs; }
    }

    public static class Metrics {
        private String prefix = "karasu_kafka";
        private int snapshotRefreshSeconds = 10;
        private boolean publishStaleOnDegraded = false;
        private boolean requireCompleteWindowForOffset = true;
        private int maxExtraLabelsPerCluster = 8;
        private Map<String, String> commonLabels = new LinkedHashMap<>();
        private List<String> declaredExtraLabelKeys = new ArrayList<>();

        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }
        public int getSnapshotRefreshSeconds() { return snapshotRefreshSeconds; }
        public void setSnapshotRefreshSeconds(int snapshotRefreshSeconds) { this.snapshotRefreshSeconds = snapshotRefreshSeconds; }
        public boolean isPublishStaleOnDegraded() { return publishStaleOnDegraded; }
        public void setPublishStaleOnDegraded(boolean publishStaleOnDegraded) { this.publishStaleOnDegraded = publishStaleOnDegraded; }
        public boolean isRequireCompleteWindowForOffset() { return requireCompleteWindowForOffset; }
        public void setRequireCompleteWindowForOffset(boolean requireCompleteWindowForOffset) { this.requireCompleteWindowForOffset = requireCompleteWindowForOffset; }
        public int getMaxExtraLabelsPerCluster() { return maxExtraLabelsPerCluster; }
        public void setMaxExtraLabelsPerCluster(int maxExtraLabelsPerCluster) { this.maxExtraLabelsPerCluster = maxExtraLabelsPerCluster; }
        public Map<String, String> getCommonLabels() { return commonLabels; }
        public void setCommonLabels(Map<String, String> commonLabels) { this.commonLabels = commonLabels; }
        public List<String> getDeclaredExtraLabelKeys() { return declaredExtraLabelKeys; }
        public void setDeclaredExtraLabelKeys(List<String> declaredExtraLabelKeys) { this.declaredExtraLabelKeys = declaredExtraLabelKeys; }
    }

    public static class Defaults {
        private CollectionItem collection = CollectionItem.defaultValues();
        private EvaluationItem evaluation = EvaluationItem.defaultValues();
        private LimitsItem limits = LimitsItem.defaultValues();

        public CollectionItem getCollection() { return collection; }
        public void setCollection(CollectionItem collection) { this.collection = collection; }
        public EvaluationItem getEvaluation() { return evaluation; }
        public void setEvaluation(EvaluationItem evaluation) { this.evaluation = evaluation; }
        public LimitsItem getLimits() { return limits; }
        public void setLimits(LimitsItem limits) { this.limits = limits; }
    }

    public static class ClusterItem {
        private String k8sCluster;
        private String kafkaCluster;
        private List<String> bootstrapServers = new ArrayList<>();
        private Map<String, String> labels = new LinkedHashMap<>();
        private AuthItem auth;
        private TlsItem tls;
        private CollectionItem collection;
        private EvaluationItem evaluation;
        private FilterItem filters;
        private LimitsItem limits;
        private Map<String, String> extraKafkaProperties = new LinkedHashMap<>();

        public String getK8sCluster() { return k8sCluster; }
        public void setK8sCluster(String k8sCluster) { this.k8sCluster = k8sCluster; }
        public String getKafkaCluster() { return kafkaCluster; }
        public void setKafkaCluster(String kafkaCluster) { this.kafkaCluster = kafkaCluster; }
        public List<String> getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(List<String> bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public Map<String, String> getLabels() { return labels; }
        public void setLabels(Map<String, String> labels) { this.labels = labels; }
        public AuthItem getAuth() { return auth; }
        public void setAuth(AuthItem auth) { this.auth = auth; }
        public TlsItem getTls() { return tls; }
        public void setTls(TlsItem tls) { this.tls = tls; }
        public CollectionItem getCollection() { return collection; }
        public void setCollection(CollectionItem collection) { this.collection = collection; }
        public EvaluationItem getEvaluation() { return evaluation; }
        public void setEvaluation(EvaluationItem evaluation) { this.evaluation = evaluation; }
        public FilterItem getFilters() { return filters; }
        public void setFilters(FilterItem filters) { this.filters = filters; }
        public LimitsItem getLimits() { return limits; }
        public void setLimits(LimitsItem limits) { this.limits = limits; }
        public Map<String, String> getExtraKafkaProperties() { return extraKafkaProperties; }
        public void setExtraKafkaProperties(Map<String, String> extraKafkaProperties) { this.extraKafkaProperties = extraKafkaProperties; }
    }

    public static class AuthItem {
        private String securityProtocol = "PLAINTEXT";
        private String saslMechanism = "";
        private String username = "";
        private String password = "";
        private String usernameFile = "";
        private String passwordFile = "";

        public String getSecurityProtocol() { return securityProtocol; }
        public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }
        public String getSaslMechanism() { return saslMechanism; }
        public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getUsernameFile() { return usernameFile; }
        public void setUsernameFile(String usernameFile) { this.usernameFile = usernameFile; }
        public String getPasswordFile() { return passwordFile; }
        public void setPasswordFile(String passwordFile) { this.passwordFile = passwordFile; }
    }

    public static class TlsItem {
        private boolean enabled = false;
        private String caFile = "";
        private String certFile = "";
        private String keyFile = "";
        private String endpointIdentificationAlgorithm = "https";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getCaFile() { return caFile; }
        public void setCaFile(String caFile) { this.caFile = caFile; }
        public String getCertFile() { return certFile; }
        public void setCertFile(String certFile) { this.certFile = certFile; }
        public String getKeyFile() { return keyFile; }
        public void setKeyFile(String keyFile) { this.keyFile = keyFile; }
        public String getEndpointIdentificationAlgorithm() { return endpointIdentificationAlgorithm; }
        public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) { this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm; }
    }

    public static class CollectionItem {
        private String offsetsTopic;
        private Integer offsetRefreshSeconds;
        private Integer topicRefreshSeconds;
        private Integer snapshotRefreshSeconds;
        private StartMode startMode;
        private IsolationLevelConfig isolationLevel;
        private Integer offsetsConsumerThreads;
        private Integer groupsReaperSeconds;
        private SwapPolicy swapPolicy;
        private Integer requestTimeoutMs;
        private Integer fetchMaxBytes;
        private Integer maxPollRecords;

        static CollectionItem defaultValues() {
            CollectionItem d = new CollectionItem();
            d.offsetsTopic = "__consumer_offsets";
            d.offsetRefreshSeconds = 15;
            d.topicRefreshSeconds = 60;
            d.snapshotRefreshSeconds = 10;
            d.startMode = StartMode.EARLIEST_THEN_LIVE;
            d.isolationLevel = IsolationLevelConfig.READ_COMMITTED;
            d.offsetsConsumerThreads = 1;
            d.groupsReaperSeconds = 0;
            d.swapPolicy = SwapPolicy.BLUE_GREEN;
            d.requestTimeoutMs = 10_000;
            d.fetchMaxBytes = 4 * 1024 * 1024;
            d.maxPollRecords = 2000;
            return d;
        }

        public String getOffsetsTopic() { return offsetsTopic; }
        public void setOffsetsTopic(String offsetsTopic) { this.offsetsTopic = offsetsTopic; }
        public Integer getOffsetRefreshSeconds() { return offsetRefreshSeconds; }
        public void setOffsetRefreshSeconds(Integer offsetRefreshSeconds) { this.offsetRefreshSeconds = offsetRefreshSeconds; }
        public Integer getTopicRefreshSeconds() { return topicRefreshSeconds; }
        public void setTopicRefreshSeconds(Integer topicRefreshSeconds) { this.topicRefreshSeconds = topicRefreshSeconds; }
        public Integer getSnapshotRefreshSeconds() { return snapshotRefreshSeconds; }
        public void setSnapshotRefreshSeconds(Integer snapshotRefreshSeconds) { this.snapshotRefreshSeconds = snapshotRefreshSeconds; }
        public StartMode getStartMode() { return startMode; }
        public void setStartMode(StartMode startMode) { this.startMode = startMode; }
        public IsolationLevelConfig getIsolationLevel() { return isolationLevel; }
        public void setIsolationLevel(IsolationLevelConfig isolationLevel) { this.isolationLevel = isolationLevel; }
        public Integer getOffsetsConsumerThreads() { return offsetsConsumerThreads; }
        public void setOffsetsConsumerThreads(Integer offsetsConsumerThreads) { this.offsetsConsumerThreads = offsetsConsumerThreads; }
        public Integer getGroupsReaperSeconds() { return groupsReaperSeconds; }
        public void setGroupsReaperSeconds(Integer groupsReaperSeconds) { this.groupsReaperSeconds = groupsReaperSeconds; }
        public SwapPolicy getSwapPolicy() { return swapPolicy; }
        public void setSwapPolicy(SwapPolicy swapPolicy) { this.swapPolicy = swapPolicy; }
        public Integer getRequestTimeoutMs() { return requestTimeoutMs; }
        public void setRequestTimeoutMs(Integer requestTimeoutMs) { this.requestTimeoutMs = requestTimeoutMs; }
        public Integer getFetchMaxBytes() { return fetchMaxBytes; }
        public void setFetchMaxBytes(Integer fetchMaxBytes) { this.fetchMaxBytes = fetchMaxBytes; }
        public Integer getMaxPollRecords() { return maxPollRecords; }
        public void setMaxPollRecords(Integer maxPollRecords) { this.maxPollRecords = maxPollRecords; }
    }

    public static class EvaluationItem {
        private Integer intervals;
        private Long minDistanceSeconds;
        private Long expireGroupSeconds;
        private Long allowedLag;
        private Float minimumComplete;
        private Integer cacheExpireSeconds;

        static EvaluationItem defaultValues() {
            EvaluationItem d = new EvaluationItem();
            d.intervals = 10;
            d.minDistanceSeconds = 1L;
            d.expireGroupSeconds = 604_800L;
            d.allowedLag = 0L;
            d.minimumComplete = 1.0f;
            d.cacheExpireSeconds = 10;
            return d;
        }

        public Integer getIntervals() { return intervals; }
        public void setIntervals(Integer intervals) { this.intervals = intervals; }
        public Long getMinDistanceSeconds() { return minDistanceSeconds; }
        public void setMinDistanceSeconds(Long minDistanceSeconds) { this.minDistanceSeconds = minDistanceSeconds; }
        public Long getExpireGroupSeconds() { return expireGroupSeconds; }
        public void setExpireGroupSeconds(Long expireGroupSeconds) { this.expireGroupSeconds = expireGroupSeconds; }
        public Long getAllowedLag() { return allowedLag; }
        public void setAllowedLag(Long allowedLag) { this.allowedLag = allowedLag; }
        public Float getMinimumComplete() { return minimumComplete; }
        public void setMinimumComplete(Float minimumComplete) { this.minimumComplete = minimumComplete; }
        public Integer getCacheExpireSeconds() { return cacheExpireSeconds; }
        public void setCacheExpireSeconds(Integer cacheExpireSeconds) { this.cacheExpireSeconds = cacheExpireSeconds; }
    }

    public static class FilterItem {
        private String groupAllowlist = "";
        private String groupDenylist = "";
        private String topicAllowlist = "";
        private String topicDenylist = "";
        private Boolean exportTopicPartitionOffset = true;

        public String getGroupAllowlist() { return groupAllowlist; }
        public void setGroupAllowlist(String groupAllowlist) { this.groupAllowlist = groupAllowlist; }
        public String getGroupDenylist() { return groupDenylist; }
        public void setGroupDenylist(String groupDenylist) { this.groupDenylist = groupDenylist; }
        public String getTopicAllowlist() { return topicAllowlist; }
        public void setTopicAllowlist(String topicAllowlist) { this.topicAllowlist = topicAllowlist; }
        public String getTopicDenylist() { return topicDenylist; }
        public void setTopicDenylist(String topicDenylist) { this.topicDenylist = topicDenylist; }
        public Boolean getExportTopicPartitionOffset() { return exportTopicPartitionOffset; }
        public void setExportTopicPartitionOffset(Boolean exportTopicPartitionOffset) { this.exportTopicPartitionOffset = exportTopicPartitionOffset; }
    }

    public static class LimitsItem {
        private Integer maxGroups;
        private Integer maxGroupPartitions;

        static LimitsItem defaultValues() {
            LimitsItem d = new LimitsItem();
            d.maxGroups = 10_000;
            d.maxGroupPartitions = 200_000;
            return d;
        }

        public Integer getMaxGroups() { return maxGroups; }
        public void setMaxGroups(Integer maxGroups) { this.maxGroups = maxGroups; }
        public Integer getMaxGroupPartitions() { return maxGroupPartitions; }
        public void setMaxGroupPartitions(Integer maxGroupPartitions) { this.maxGroupPartitions = maxGroupPartitions; }
    }
}
