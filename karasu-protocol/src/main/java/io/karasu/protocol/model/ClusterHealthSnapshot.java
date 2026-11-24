/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Point-in-time health and capacity state of a single monitored Kafka cluster.
 */
public record ClusterHealthSnapshot(
        ClusterPhase phase,
        boolean dataReady,
        boolean headFresh,
        boolean offsetsCaughtUp,
        boolean clusterReachable,
        boolean collectionSuccess,
        long lastCollectionSuccessEpochSec,
        long offsetsLagTotal,
        int trackedGroups,
        int trackedPartitions,
        boolean cardinalityLimitReached,
        long generation,
        String lastErrorMessage
) {
}
