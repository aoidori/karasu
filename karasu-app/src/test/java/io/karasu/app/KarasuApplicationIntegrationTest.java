/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Spring Boot 3 startup, multi-cluster application.yml binding, startup fault isolation
 * (unreachable brokers never fail application startup), extensible labels, and /metrics + health probes.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@TestPropertySource(properties = {
        "karasu.config.watch-enabled=false",
        "karasu.metrics.common-labels.env=test",
        "karasu.metrics.common-labels.region=cn-east-1",
        "karasu.clusters[0].k8s-cluster=k8s-a",
        "karasu.clusters[0].kafka-cluster=kafka-alpha",
        "karasu.clusters[0].bootstrap-servers[0]=127.0.0.1:19092",
        "karasu.clusters[0].labels.tenant=team-alpha",
        "karasu.clusters[0].collection.request-timeout-ms=1000",
        "karasu.clusters[1].k8s-cluster=k8s-b",
        "karasu.clusters[1].kafka-cluster=kafka-alpha",
        "karasu.clusters[1].bootstrap-servers[0]=127.0.0.1:19093",
        "karasu.clusters[1].labels.tenant=team-beta",
        "karasu.clusters[1].collection.request-timeout-ms=1000"
})
class KarasuApplicationIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void bootsHealthyAndExposesIsolatedMultiClusterHealthMetricsEvenWhenBrokersAreUnreachable() {
        ResponseEntity<String> liveness = restTemplate.getForEntity("/actuator/health/liveness", String.class);
        assertThat(liveness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(liveness.getBody()).contains("UP");

        ResponseEntity<String> readiness = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(readiness.getBody()).contains("UP");

        ResponseEntity<String> metrics = restTemplate.getForEntity("/metrics", String.class);
        assertThat(metrics.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = metrics.getBody();
        assertThat(body).isNotNull();
        // Both identically named Kafka clusters across different K8s clusters are isolated and exposed
        assertThat(body).contains("karasu_cluster_up{");
        assertThat(body).contains("k8s_cluster=\"k8s-a\"");
        assertThat(body).contains("k8s_cluster=\"k8s-b\"");
        assertThat(body).contains("kafka_cluster=\"kafka-alpha\"");
        assertThat(body).contains("env=\"test\"");
        assertThat(body).contains("region=\"cn-east-1\"");
        assertThat(body).contains("tenant=\"team-alpha\"");
        assertThat(body).contains("tenant=\"team-beta\"");

        // Targeted cluster filter query parameter works
        ResponseEntity<String> filtered = restTemplate.getForEntity("/metrics?k8s_cluster=k8s-a", String.class);
        assertThat(filtered.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(filtered.getBody()).contains("k8s_cluster=\"k8s-a\"");
        assertThat(filtered.getBody()).doesNotContain("k8s_cluster=\"k8s-b\"");
    }
}
