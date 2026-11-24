/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.spi;

import io.karasu.core.config.ClusterSpec;

import java.util.Map;

/**
 * Layer-3 programmatic label enrichment SPI.
 * Register one or more Spring beans implementing this interface to dynamically attach
 * additional Prometheus labels (e.g. from CMDB or naming conventions) without altering core collectors.
 */
public interface MetricLabelEnricher {

    /**
     * Extra labels attached to all metrics (health and business lag) of the given cluster.
     */
    default Map<String, String> enrichClusterLabels(ClusterSpec spec) {
        return Map.of();
    }

    /**
     * Extra labels attached to metrics of a specific consumer group within a cluster.
     */
    default Map<String, String> enrichGroupLabels(ClusterSpec spec, String consumerGroup) {
        return Map.of();
    }
}
