/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Lifecycle phase of an isolated per-cluster collector runtime.
 */
public enum ClusterPhase {
    INITIALIZING,
    WAITING_OFFSETS_TOPIC,
    REPLAYING,
    READY,
    DEGRADED,
    STOPPING,
    STOPPED
}
