/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.spi;

import io.karasu.core.config.ClusterSpec;

import java.util.List;
import java.util.function.Consumer;

/**
 * Extension point supplying monitored Kafka cluster specifications.
 * Phase 1 uses Spring Boot {@code application.yml} (and optional watched YAML directories);
 * future phases can plug in Kubernetes CRD Informers or external control planes without touching core.
 */
public interface ClusterTargetProvider {

    /**
     * Loads the current complete desired set of Kafka cluster targets.
     */
    List<ClusterSpec> loadTargets();

    /**
     * Registers a listener invoked whenever the desired target list or mounted credentials change.
     */
    default void onTargetsChanged(Consumer<List<ClusterSpec>> listener) {
        // Optional for static providers
    }
}
