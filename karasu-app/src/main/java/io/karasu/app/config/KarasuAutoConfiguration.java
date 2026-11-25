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
import org.springframework.core.env.Environment;

import java.util.List;

/**
 * Spring Boot wiring for Karasu core engine, multi-provider union reconciliation,
 * merged-label pre-validation, and native Prometheus {@code MultiCollector} registration.
 */
@Configuration
@EnableConfigurationProperties(KarasuProperties.class)
public class KarasuAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KarasuAutoConfiguration.class);

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

    @Bean(destroyMethod = "close")
    public ClusterManager clusterManager(KarasuProperties properties, LabelNormalizer labelNormalizer) {
        ClusterManager manager = new ClusterManager(properties.getMetrics().getMaxExtraLabelsPerCluster());
        // Pre-validate merged L1+L2+L3 labels before activating any cluster target (#12)
        manager.setExtraSpecValidator(labelNormalizer::validateMergedClusterSpec);
        return manager;
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
    public ApplicationPropertiesClusterProvider applicationPropertiesClusterProvider(KarasuProperties properties,
                                                                                     Environment environment) {
        return new ApplicationPropertiesClusterProvider(properties, environment);
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
            // Register each provider under its own providerId so multiple sources merge cleanly (#13)
            for (int i = 0; i < providers.size(); i++) {
                ClusterTargetProvider provider = providers.get(i);
                String providerId = provider.getClass().getName() + "#" + i;
                provider.onTargetsChanged(specs -> clusterManager.reconcileFromProvider(providerId, specs));
                clusterManager.reconcileFromProvider(providerId, provider.loadTargets());
            }
            defaultProvider.startWatchingIfEnabled();
            log.info("Karasu initialized with {} active Kafka cluster target(s) from {} provider(s)",
                    clusterManager.activeTargets().size(), providers.size());
        }
    }
}
