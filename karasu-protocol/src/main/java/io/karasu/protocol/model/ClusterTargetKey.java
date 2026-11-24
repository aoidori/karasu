/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

import java.util.Objects;

/**
 * Globally unique identifier for a monitored Kafka cluster inside a Karasu process.
 * Using (k8sCluster, kafkaCluster) avoids collisions when monitoring identically named
 * Kafka clusters across multiple Kubernetes clusters.
 */
public record ClusterTargetKey(String k8sCluster, String kafkaCluster) implements Comparable<ClusterTargetKey> {

    public ClusterTargetKey {
        Objects.requireNonNull(k8sCluster, "k8sCluster must not be null");
        Objects.requireNonNull(kafkaCluster, "kafkaCluster must not be null");
        if (k8sCluster.isBlank()) {
            throw new IllegalArgumentException("k8sCluster must not be blank");
        }
        if (kafkaCluster.isBlank()) {
            throw new IllegalArgumentException("kafkaCluster must not be blank");
        }
    }

    public String canonicalId() {
        return k8sCluster + "/" + kafkaCluster;
    }

    @Override
    public int compareTo(ClusterTargetKey other) {
        int cmp = this.k8sCluster.compareTo(other.k8sCluster);
        return cmp != 0 ? cmp : this.kafkaCluster.compareTo(other.kafkaCluster);
    }
}
