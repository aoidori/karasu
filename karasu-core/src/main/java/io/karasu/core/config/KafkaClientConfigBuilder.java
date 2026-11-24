/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Builds hardened Kafka AdminClient and KafkaConsumer properties supporting
 * PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL, SCRAM-SHA-256/512, PLAIN, and native PEM TLS/mTLS.
 */
public final class KafkaClientConfigBuilder {

    private KafkaClientConfigBuilder() {
    }

    public static Properties buildAdminProperties(ClusterSpec spec, long generation) throws IOException {
        Properties props = buildCommonProperties(spec, "karasu-admin-" + sanitizeClientId(spec) + "-g" + generation);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, spec.collection().requestTimeoutMs());
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.max(spec.collection().requestTimeoutMs(), 15_000));
        props.put(AdminClientConfig.RETRIES_CONFIG, 1);
        return props;
    }

    public static Properties buildOffsetsConsumerProperties(ClusterSpec spec, long generation, int workerIdx) throws IOException {
        Properties props = buildCommonProperties(spec, "karasu-offsets-" + sanitizeClientId(spec) + "-g" + generation + "-w" + workerIdx);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                spec.collection().isolationLevel() == ClusterSpec.IsolationLevelConfig.READ_UNCOMMITTED
                        ? "read_uncommitted"
                        : "read_committed");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, spec.collection().fetchMaxBytes());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Math.min(spec.collection().fetchMaxBytes(), 1024 * 1024));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, spec.collection().maxPollRecords());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, spec.collection().requestTimeoutMs());
        return props;
    }

    /**
     * Computes a digest of actual credential and TLS file contents so credential rotations
     * (even when file paths stay identical, e.g., Kubernetes Secret volume updates) trigger reload.
     */
    public static String computeMaterialDigest(ClusterSpec spec) {
        StringBuilder sb = new StringBuilder();
        try {
            if (spec.auth().usesSasl()) {
                sb.append("u=").append(resolveSecret(spec.auth().username(), spec.auth().usernameFile())).append('\n');
                sb.append("p=").append(resolveSecret(spec.auth().password(), spec.auth().passwordFile())).append('\n');
            }
            if (spec.tls().enabled() || spec.auth().securityProtocol().endsWith("SSL")) {
                appendFileDigest(sb, "ca", spec.tls().caFile());
                appendFileDigest(sb, "cert", spec.tls().certFile());
                appendFileDigest(sb, "key", spec.tls().keyFile());
            }
        } catch (IOException e) {
            sb.append("err=").append(e.getClass().getSimpleName());
        }
        return ClusterSpec.sha256Hex(sb.toString());
    }

    private static Properties buildCommonProperties(ClusterSpec spec, String clientId) throws IOException {
        Properties props = new Properties();
        // Extra properties first so validated security/isolation settings take precedence where required
        props.putAll(spec.extraKafkaProperties());

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", spec.bootstrapServers()));
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, spec.auth().securityProtocol());
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, 500L);
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10_000L);

        if (spec.auth().usesSasl()) {
            String username = resolveSecret(spec.auth().username(), spec.auth().usernameFile());
            String password = resolveSecret(spec.auth().password(), spec.auth().passwordFile());
            props.put(SaslConfigs.SASL_MECHANISM, spec.auth().saslMechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(spec.auth().saslMechanism(), username, password));
        }

        if (spec.tls().enabled() || spec.auth().securityProtocol().endsWith("SSL")) {
            props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3,TLSv1.2");
            props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, spec.tls().endpointIdentificationAlgorithm());

            if (!spec.tls().caFile().isEmpty()) {
                String caPem = readRequiredPem(spec.tls().caFile(), "tls.caFile");
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
                props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, caPem);
            }
            if (!spec.tls().certFile().isEmpty() && !spec.tls().keyFile().isEmpty()) {
                String certPem = readRequiredPem(spec.tls().certFile(), "tls.certFile");
                String keyPem = readRequiredPem(spec.tls().keyFile(), "tls.keyFile");
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
                props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certPem);
                props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, keyPem);
            }
        }
        return props;
    }

    static String buildJaasConfig(String mechanism, String username, String password) {
        String moduleClass = "PLAIN".equals(mechanism)
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";
        return moduleClass + " required username=\"" + escapeJaas(username) + "\" password=\"" + escapeJaas(password) + "\";";
    }

    private static String escapeJaas(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String resolveSecret(String inline, String filePath) throws IOException {
        if (filePath != null && !filePath.isEmpty()) {
            String content = Files.readString(Path.of(filePath), StandardCharsets.UTF_8);
            return stripTrailingNewlines(content);
        }
        return inline == null ? "" : inline;
    }

    private static String readRequiredPem(String path, String field) throws IOException {
        String content = Files.readString(Path.of(path), StandardCharsets.UTF_8).trim();
        if (content.isEmpty() || !content.contains("-----BEGIN ")) {
            throw new IOException(field + " does not contain valid PEM data");
        }
        return content;
    }

    private static void appendFileDigest(StringBuilder sb, String tag, String path) throws IOException {
        if (path != null && !path.isEmpty()) {
            sb.append(tag).append('=').append(Files.readString(Path.of(path), StandardCharsets.UTF_8)).append('\n');
        }
    }

    private static String stripTrailingNewlines(String s) {
        int end = s.length();
        while (end > 0 && (s.charAt(end - 1) == '\n' || s.charAt(end - 1) == '\r')) {
            end--;
        }
        return s.substring(0, end);
    }

    private static String sanitizeClientId(ClusterSpec spec) {
        String raw = spec.targetKey().k8sCluster() + "-" + spec.targetKey().kafkaCluster();
        return raw.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
