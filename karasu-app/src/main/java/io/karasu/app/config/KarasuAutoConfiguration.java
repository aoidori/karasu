/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app.config;

import io.karasu.app.provider.ApplicationPropertiesClusterProvider;
import io.karasu.core.collector.ClusterManager;
import io.karasu.core.spi.ClusterTargetProvider;
import io.karasu.core.spi.MetricLabelEnricher;
import io.karasu.exporter.labels.LabelNormalizer;
import io.karasu.exporter.metrics.KarasuPrometheusCollector;
import io.karasu.exporter.metrics.KarasuPrometheusCollector.ExporterOptions;
import io.karasu.exporter.metrics.MetricsEndpointController;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.List;

/**
 * Spring Boot wiring for Karasu core engine, multi-cluster providers, extensible label enrichers,
 * and native Prometheus {@code MultiCollector} registration.
 */
@Configuration
@EnableConfigurationProperties(KarasuProperties.class)
public class KarasuAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KarasuAutoConfiguration.class);

    @Bean(destroyMethod = "close")
    public ClusterManager clusterManager(KarasuProperties properties) {
        return new ClusterManager(properties.getMetrics().getMaxExtraLabelsPerCluster());
    }

    @Bean
    public LabelNormalizer labelNormalizer(KarasuProperties properties,
                                           ObjectProvider<MetricLabelEnricher> enricherProvider) {
        List<MetricLabelEnricher> enrichers = enricherProvider.orderedStream().toList();
        return new LabelNormalizer(
                properties.getMetrics().getCommonLabels(),
                properties.getMetrics().getDeclaredExtraLabelKeys(),
                properties.getMetrics().getMaxExtraLabelsPerCluster(),
                enrichers
        );
    }

    @Bean
    public KarasuPrometheusCollector karasuPrometheusCollector(ClusterManager clusterManager,
                                                               LabelNormalizer labelNormalizer,
                                                               KarasuProperties properties,
                                                               PrometheusRegistry prometheusRegistry) {
        ExporterOptions options = new ExporterOptions(
                properties.getMetrics().getPrefix(),
                properties.getMetrics().isPublishStaleOnDegraded(),
                properties.getMetrics().isRequireCompleteWindowForOffset()
        );
        KarasuPrometheusCollector collector = new KarasuPrometheusCollector(clusterManager, labelNormalizer, options);
        prometheusRegistry.register(collector);
        log.info("Registered KarasuPrometheusCollector (prefix={}, publishStaleOnDegraded={})",
                options.metricPrefix(), options.publishStaleOnDegraded());
        return collector;
    }

    @Bean
    public MetricsEndpointController metricsEndpointController(PrometheusRegistry prometheusRegistry,
                                                               KarasuPrometheusCollector collector) {
        return new MetricsEndpointController(prometheusRegistry, collector);
    }

    @Bean(destroyMethod = "close")
    public ApplicationPropertiesClusterProvider applicationPropertiesClusterProvider(KarasuProperties properties) {
        return new ApplicationPropertiesClusterProvider(properties);
    }

    @Bean
    public KarasuBootstrap karasuBootstrap(ClusterManager clusterManager,
                                           List<ClusterTargetProvider> providers,
                                           ApplicationPropertiesClusterProvider defaultProvider) {
        return new KarasuBootstrap(clusterManager, providers, defaultProvider);
    }

    public static final class KarasuBootstrap {
        private final ClusterManager clusterManager;
        private final List<ClusterTargetProvider> providers;
        private final ApplicationPropertiesClusterProvider defaultProvider;

        public KarasuBootstrap(ClusterManager clusterManager,
                               List<ClusterTargetProvider> providers,
                               ApplicationPropertiesClusterProvider defaultProvider) {
            this.clusterManager = clusterManager;
            this.providers = providers;
            this.defaultProvider = defaultProvider;
        }

        @EventListener(ApplicationReadyEvent.class)
        public void onApplicationReady() {
            for (ClusterTargetProvider provider : providers) {
                provider.onTargetsChanged(clusterManager::reconcile);
                clusterManager.reconcile(provider.loadTargets());
            }
            defaultProvider.startWatchingIfEnabled();
            log.info("Karasu initialized with {} active Kafka cluster target(s)", clusterManager.activeTargets().size());
        }
    }
}
