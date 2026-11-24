/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.config;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates {@link ClusterSpec} instances before activation or hot-reconfiguration.
 */
public final class ClusterSpecValidator {

    public static final Pattern PROMETHEUS_LABEL_NAME = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    public static final Set<String> RESERVED_LABELS = Set.of(
            "k8s_cluster",
            "kafka_cluster",
            "consumer_group",
            "topic",
            "partition",
            "state",
            "record_type",
            "reason",
            "phase"
    );
    private static final Set<String> SUPPORTED_PROTOCOLS = Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");
    private static final Set<String> SUPPORTED_SASL = Set.of("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN");

    private ClusterSpecValidator() {
    }

    public static void validate(ClusterSpec spec, int maxExtraLabels) {
        if (spec == null) {
            throw new IllegalArgumentException("cluster spec must not be null");
        }
        if (spec.bootstrapServers().isEmpty()) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": bootstrapServers must not be empty");
        }
        for (String server : spec.bootstrapServers()) {
            if (server == null || server.isBlank() || !server.contains(":")) {
                throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": invalid broker address '" + server + "'");
            }
        }
        if (!SUPPORTED_PROTOCOLS.contains(spec.auth().securityProtocol())) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": unsupported securityProtocol '" + spec.auth().securityProtocol() + "'");
        }
        if (spec.auth().usesSasl()) {
            if (!SUPPORTED_SASL.contains(spec.auth().saslMechanism())) {
                throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": unsupported saslMechanism '" + spec.auth().saslMechanism() + "'");
            }
            boolean hasUser = !spec.auth().username().isEmpty() || !spec.auth().usernameFile().isEmpty();
            boolean hasPass = !spec.auth().password().isEmpty() || !spec.auth().passwordFile().isEmpty();
            if (!hasUser || !hasPass) {
                throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": SASL requires username and password (inline or file)");
            }
        }
        boolean hasCert = !spec.tls().certFile().isEmpty();
        boolean hasKey = !spec.tls().keyFile().isEmpty();
        if (hasCert != hasKey) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": tls.certFile and tls.keyFile must be paired");
        }
        if (spec.collection().offsetRefreshSeconds() < 1 || spec.collection().topicRefreshSeconds() < 1) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": refresh intervals must be >= 1s");
        }
        if (spec.collection().offsetsConsumerThreads() < 1 || spec.collection().offsetsConsumerThreads() > 16) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": offsetsConsumerThreads must be between 1 and 16");
        }
        if (spec.evaluation().intervals() < 2) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": evaluation.intervals must be >= 2");
        }
        if (spec.evaluation().expireGroupSeconds() < 1) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": evaluation.expireGroupSeconds must be >= 1");
        }
        if (spec.evaluation().minimumComplete() < 0.0f || spec.evaluation().minimumComplete() > 1.0f) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": evaluation.minimumComplete must be in [0.0, 1.0]");
        }
        if (spec.limits().maxGroups() < 1 || spec.limits().maxGroupPartitions() < 1) {
            throw new IllegalArgumentException("cluster " + spec.targetKey().canonicalId() + ": cardinality limits must be >= 1");
        }

        validateRegex(spec.filters().groupAllowlist(), "groupAllowlist");
        validateRegex(spec.filters().groupDenylist(), "groupDenylist");
        validateRegex(spec.filters().topicAllowlist(), "topicAllowlist");
        validateRegex(spec.filters().topicDenylist(), "topicDenylist");
        validateExtraLabels(spec.labels(), maxExtraLabels, spec.targetKey().canonicalId());
    }

    public static void validateExtraLabels(Map<String, String> labels, int maxExtraLabels, String context) {
        if (labels == null) {
            return;
        }
        if (labels.size() > maxExtraLabels) {
            throw new IllegalArgumentException(context + ": extra labels count " + labels.size() + " exceeds max " + maxExtraLabels);
        }
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            String key = entry.getKey();
            if (key == null || !PROMETHEUS_LABEL_NAME.matcher(key).matches() || key.startsWith("__")) {
                throw new IllegalArgumentException(context + ": invalid Prometheus label name '" + key + "'");
            }
            if (RESERVED_LABELS.contains(key)) {
                throw new IllegalArgumentException(context + ": extra label '" + key + "' collides with reserved built-in label");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(context + ": label '" + key + "' value must not be null");
            }
        }
    }

    private static void validateRegex(String regex, String name) {
        if (regex != null && !regex.isEmpty()) {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("invalid regex for " + name + ": " + e.getMessage(), e);
            }
        }
    }
}
