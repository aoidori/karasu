/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app.provider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.karasu.app.config.KarasuProperties;
import io.karasu.app.config.KarasuProperties.ClusterItem;
import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.KafkaClientConfigBuilder;
import io.karasu.core.spi.ClusterTargetProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Default {@link ClusterTargetProvider} loading cluster targets from Spring Boot's {@code application.yml}
 * plus optional drop-in YAML directories ({@code karasu.config.additional-cluster-dirs}).
 *
 * <p>When {@code karasu.config.watch-enabled=true}, periodically polls SHA-256 digests of watched cluster files
 * and mounted secret/TLS files (Kubernetes ConfigMap/Secret symlink-safe) and triggers live hot reconciliation.
 */
public final class ApplicationPropertiesClusterProvider implements ClusterTargetProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ApplicationPropertiesClusterProvider.class);

    private final KarasuProperties properties;
    private final ObjectMapper yamlMapper;
    private final List<Consumer<List<ClusterSpec>>> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService watcher;
    private volatile String lastObservedStateDigest = "";

    public ApplicationPropertiesClusterProvider(KarasuProperties properties) {
        this.properties = properties;
        this.yamlMapper = new ObjectMapper(new YAMLFactory())
                .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.watcher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "karasu-config-watcher");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public List<ClusterSpec> loadTargets() {
        List<ClusterSpec> specs = new ArrayList<>();
        if (properties.getClusters() != null) {
            for (ClusterItem item : properties.getClusters()) {
                addConverted(specs, item, "application.yml");
            }
        }
        for (String dirStr : properties.getConfig().getAdditionalClusterDirs()) {
            if (dirStr == null || dirStr.isBlank()) {
                continue;
            }
            Path dir = Path.of(dirStr.trim());
            if (!Files.isDirectory(dir)) {
                log.debug("Additional cluster directory does not exist yet: {}", dir);
                continue;
            }
            loadDirectory(dir, specs);
        }
        lastObservedStateDigest = computeCombinedDigest(specs);
        return specs;
    }

    @Override
    public void onTargetsChanged(Consumer<List<ClusterSpec>> listener) {
        listeners.add(listener);
    }

    public void startWatchingIfEnabled() {
        if (!properties.getConfig().isWatchEnabled()) {
            return;
        }
        int intervalSec = Math.max(5, properties.getConfig().getWatchIntervalSeconds());
        watcher.scheduleWithFixedDelay(this::checkForChangesSafe, intervalSec, intervalSec, TimeUnit.SECONDS);
        log.info("Config & credential watcher enabled (interval={}s, dirs={})",
                intervalSec, properties.getConfig().getAdditionalClusterDirs());
    }

    @Override
    public void close() {
        watcher.shutdownNow();
    }

    private void checkForChangesSafe() {
        try {
            List<ClusterSpec> latest = new ArrayList<>();
            if (properties.getClusters() != null) {
                for (ClusterItem item : properties.getClusters()) {
                    addConverted(latest, item, "application.yml");
                }
            }
            for (String dirStr : properties.getConfig().getAdditionalClusterDirs()) {
                if (dirStr != null && !dirStr.isBlank()) {
                    Path dir = Path.of(dirStr.trim());
                    if (Files.isDirectory(dir)) {
                        loadDirectory(dir, latest);
                    }
                }
            }
            String digest = computeCombinedDigest(latest);
            if (!digest.equals(lastObservedStateDigest)) {
                lastObservedStateDigest = digest;
                log.info("Detected cluster config or credential file change; triggering hot reconcile ({} targets)", latest.size());
                for (Consumer<List<ClusterSpec>> listener : listeners) {
                    listener.accept(latest);
                }
            }
        } catch (Throwable t) {
            log.warn("Error checking watched cluster configurations: {}", t.getMessage());
        }
    }

    private void loadDirectory(Path dir, List<ClusterSpec> out) {
        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> yamlFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String n = p.getFileName().toString().toLowerCase();
                        return n.endsWith(".yml") || n.endsWith(".yaml");
                    })
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();

            for (Path file : yamlFiles) {
                parseClusterFile(file, out);
            }
        } catch (IOException e) {
            log.warn("Failed to list cluster config directory {}: {}", dir, e.getMessage());
        }
    }

    private void parseClusterFile(Path file, List<ClusterSpec> out) {
        try {
            String raw = Files.readString(file);
            if (raw.isBlank()) {
                return;
            }
            ClusterFileWrapper wrapper = yamlMapper.readValue(raw, ClusterFileWrapper.class);
            if (wrapper.clusters != null && !wrapper.clusters.isEmpty()) {
                for (ClusterItem item : wrapper.clusters) {
                    addConverted(out, item, file.toString());
                }
                return;
            }
            // Fallback: single ClusterItem YAML file
            ClusterItem single = yamlMapper.readValue(raw, ClusterItem.class);
            if (single.getK8sCluster() != null && single.getKafkaCluster() != null) {
                addConverted(out, single, file.toString());
            }
        } catch (Exception e) {
            log.error("Failed to parse cluster config file {}: {}", file, e.getMessage());
        }
    }

    private void addConverted(List<ClusterSpec> out, ClusterItem item, String source) {
        try {
            out.add(properties.toClusterSpec(item));
        } catch (Exception e) {
            log.error("Invalid cluster item in {}: {}", source, e.getMessage());
        }
    }

    private String computeCombinedDigest(List<ClusterSpec> specs) {
        StringBuilder sb = new StringBuilder();
        for (ClusterSpec spec : specs) {
            sb.append(spec.toString()).append('|')
                    .append(KafkaClientConfigBuilder.computeMaterialDigest(spec))
                    .append('\n');
        }
        return ClusterSpec.sha256Hex(sb.toString());
    }

    private static final class ClusterFileWrapper {
        public List<ClusterItem> clusters;
    }
}
