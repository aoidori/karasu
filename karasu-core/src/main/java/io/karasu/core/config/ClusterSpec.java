/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.config;

import io.karasu.protocol.model.ClusterTargetKey;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable validated specification for a single monitored Kafka cluster target.
 */
public record ClusterSpec(
        ClusterTargetKey targetKey,
        List<String> bootstrapServers,
        Map<String, String> labels,
        AuthSpec auth,
        TlsSpec tls,
        CollectionSpec collection,
        EvaluationSpec evaluation,
        FilterSpec filters,
        LimitsSpec limits,
        Map<String, String> extraKafkaProperties
) {
    public ClusterSpec {
        Objects.requireNonNull(targetKey, "targetKey must not be null");
        bootstrapServers = bootstrapServers == null ? List.of() : List.copyOf(bootstrapServers);
        labels = labels == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(labels));
        auth = auth == null ? AuthSpec.PLAINTEXT_NONE : auth;
        tls = tls == null ? TlsSpec.DISABLED : tls;
        collection = collection == null ? CollectionSpec.DEFAULT : collection;
        evaluation = evaluation == null ? EvaluationSpec.DEFAULT : evaluation;
        filters = filters == null ? FilterSpec.DEFAULT : filters;
        limits = limits == null ? LimitsSpec.DEFAULT : limits;
        extraKafkaProperties = extraKafkaProperties == null
                ? Map.of()
                : Collections.unmodifiableMap(new TreeMap<>(extraKafkaProperties));
    }

    /**
     * Computes a deterministic SHA-256 fingerprint of connection-affecting fields (brokers, auth, TLS,
     * consumer start/isolation settings, extra Kafka properties). Changes to this fingerprint require
     * rebuilding the ClusterRuntime (Blue-Green swap or Recreate); changes outside it can be hot-patched.
     */
    public String connectionFingerprint(String credentialMaterialDigest) {
        StringBuilder sb = new StringBuilder();
        sb.append("key=").append(targetKey.canonicalId()).append('\n');
        sb.append("servers=").append(String.join(",", bootstrapServers)).append('\n');
        sb.append("auth=").append(auth.securityProtocol()).append('|').append(auth.saslMechanism())
                .append('|').append(auth.usernameFile()).append('|').append(auth.passwordFile()).append('\n');
        sb.append("tls=").append(tls.enabled()).append('|').append(tls.caFile())
                .append('|').append(tls.certFile()).append('|').append(tls.keyFile())
                .append('|').append(tls.endpointIdentificationAlgorithm()).append('\n');
        sb.append("cred=").append(credentialMaterialDigest == null ? "" : credentialMaterialDigest).append('\n');
        sb.append("start=").append(collection.startMode()).append('|')
                .append(collection.isolationLevel()).append('|')
                .append(collection.offsetsTopic()).append('|')
                .append(collection.offsetsConsumerThreads()).append('\n');
        // Window capacity (intervals) is structural to ring buffers; changing it requires Blue-Green rebuild
        sb.append("intervals=").append(evaluation.intervals()).append('\n');
        for (Map.Entry<String, String> e : extraKafkaProperties.entrySet()) {
            sb.append("prop.").append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return sha256Hex(sb.toString());
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public enum StartMode {
        EARLIEST_THEN_LIVE,
        LATEST_ONLY
    }

    public enum SwapPolicy {
        BLUE_GREEN,
        RECREATE
    }

    public enum IsolationLevelConfig {
        READ_COMMITTED,
        READ_UNCOMMITTED
    }

    public record AuthSpec(
            String securityProtocol,
            String saslMechanism,
            String username,
            String password,
            String usernameFile,
            String passwordFile
    ) {
        public static final AuthSpec PLAINTEXT_NONE = new AuthSpec("PLAINTEXT", "", "", "", "", "");

        public AuthSpec {
            securityProtocol = (securityProtocol == null || securityProtocol.isBlank()) ? "PLAINTEXT" : securityProtocol.trim().toUpperCase();
            saslMechanism = saslMechanism == null ? "" : saslMechanism.trim().toUpperCase();
            username = username == null ? "" : username;
            password = password == null ? "" : password;
            usernameFile = usernameFile == null ? "" : usernameFile.trim();
            passwordFile = passwordFile == null ? "" : passwordFile.trim();
        }

        public boolean usesSasl() {
            return securityProtocol.startsWith("SASL_");
        }
    }

    public record TlsSpec(
            boolean enabled,
            String caFile,
            String certFile,
            String keyFile,
            String endpointIdentificationAlgorithm
    ) {
        public static final TlsSpec DISABLED = new TlsSpec(false, "", "", "", "https");

        public TlsSpec {
            caFile = caFile == null ? "" : caFile.trim();
            certFile = certFile == null ? "" : certFile.trim();
            keyFile = keyFile == null ? "" : keyFile.trim();
            endpointIdentificationAlgorithm = endpointIdentificationAlgorithm == null ? "https" : endpointIdentificationAlgorithm.trim();
        }
    }

    public record CollectionSpec(
            String offsetsTopic,
            int offsetRefreshSeconds,
            int topicRefreshSeconds,
            int snapshotRefreshSeconds,
            StartMode startMode,
            IsolationLevelConfig isolationLevel,
            int offsetsConsumerThreads,
            int groupsReaperSeconds,
            SwapPolicy swapPolicy,
            int requestTimeoutMs,
            int fetchMaxBytes,
            int maxPollRecords
    ) {
        public static final CollectionSpec DEFAULT = new CollectionSpec(
                "__consumer_offsets", 15, 60, 10,
                StartMode.EARLIEST_THEN_LIVE, IsolationLevelConfig.READ_COMMITTED,
                1, 0, SwapPolicy.BLUE_GREEN, 10_000, 4 * 1024 * 1024, 2000
        );

        public CollectionSpec {
            offsetsTopic = (offsetsTopic == null || offsetsTopic.isBlank()) ? "__consumer_offsets" : offsetsTopic.trim();
            startMode = startMode == null ? StartMode.EARLIEST_THEN_LIVE : startMode;
            isolationLevel = isolationLevel == null ? IsolationLevelConfig.READ_COMMITTED : isolationLevel;
            swapPolicy = swapPolicy == null ? SwapPolicy.BLUE_GREEN : swapPolicy;
        }
    }

    public record EvaluationSpec(
            int intervals,
            long minDistanceSeconds,
            long expireGroupSeconds,
            long allowedLag,
            float minimumComplete,
            int cacheExpireSeconds
    ) {
        public static final EvaluationSpec DEFAULT = new EvaluationSpec(10, 1L, 604_800L, 0L, 1.0f, 10);
    }

    public record FilterSpec(
            String groupAllowlist,
            String groupDenylist,
            String topicAllowlist,
            String topicDenylist,
            boolean exportTopicPartitionOffset
    ) {
        public static final FilterSpec DEFAULT = new FilterSpec("", "", "", "", true);

        public FilterSpec {
            groupAllowlist = groupAllowlist == null ? "" : groupAllowlist;
            groupDenylist = groupDenylist == null ? "" : groupDenylist;
            topicAllowlist = topicAllowlist == null ? "" : topicAllowlist;
            topicDenylist = topicDenylist == null ? "" : topicDenylist;
        }
    }

    public record LimitsSpec(
            int maxGroups,
            int maxGroupPartitions
    ) {
        public static final LimitsSpec DEFAULT = new LimitsSpec(10_000, 200_000);
    }
}
