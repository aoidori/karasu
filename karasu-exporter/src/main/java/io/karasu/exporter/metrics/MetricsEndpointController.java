/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.exporter.metrics;

import io.prometheus.metrics.expositionformats.ExpositionFormatWriter;
import io.prometheus.metrics.expositionformats.ExpositionFormats;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Strictly read-only Prometheus scrape endpoint ({@code GET /metrics}).
 * Supports standard content negotiation (Prometheus text 0.0.4 and OpenMetrics 1.0)
 * plus optional {@code k8s_cluster} / {@code kafka_cluster} query filtering for targeted diagnostics.
 */
@RestController
public final class MetricsEndpointController {

    private final PrometheusRegistry prometheusRegistry;
    private final KarasuPrometheusCollector karasuCollector;
    private final ExpositionFormats expositionFormats;

    public MetricsEndpointController(PrometheusRegistry prometheusRegistry,
                                     KarasuPrometheusCollector karasuCollector) {
        this.prometheusRegistry = prometheusRegistry;
        this.karasuCollector = karasuCollector;
        this.expositionFormats = ExpositionFormats.init();
    }

    @GetMapping("/metrics")
    public void scrapeMetrics(@RequestParam(name = "k8s_cluster", required = false) String k8sCluster,
                              @RequestParam(name = "kafka_cluster", required = false) String kafkaCluster,
                              HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
        String acceptHeader = request.getHeader("Accept");
        ExpositionFormatWriter writer = expositionFormats.findWriter(acceptHeader);
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(writer.getContentType());

        boolean filtered = (k8sCluster != null && !k8sCluster.isEmpty())
                || (kafkaCluster != null && !kafkaCluster.isEmpty());
        if (filtered) {
            MetricSnapshots snapshots = karasuCollector.collectFiltered(k8sCluster, kafkaCluster);
            writer.write(response.getOutputStream(), snapshots);
        } else {
            MetricSnapshots snapshots = prometheusRegistry.scrape();
            writer.write(response.getOutputStream(), snapshots);
        }
    }
}
