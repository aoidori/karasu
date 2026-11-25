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
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Default {@link ClusterTargetProvider} loading cluster targets from:
 * <ol>
 *   <li>Spring Boot external config files ({@code spring.config.additional-location},
 *       {@code spring.config.location}, and {@code karasu.config.watch-files}), reloaded from disk on poll (#08)</li>
 *   <li>Fallback static {@link KarasuProperties} when no external config file defines clusters</li>
 *   <li>Optional drop-in YAML directories ({@code karasu.config.additional-cluster-dirs})</li>
 * </ol>
 *
 * <p>Per-file last-known-good caching ensures that a transient YAML syntax error in one file retains
 * its previous valid targets instead of deleting live monitoring runtimes (#06).
 */
public final class ApplicationPropertiesClusterProvider implements ClusterTargetProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ApplicationPropertiesClusterProvider.class);

    private final KarasuProperties properties;
    private final Environment environment;
    private final ObjectMapper yamlMapper;
    private final List<Consumer<List<ClusterSpec>>> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService watcher;
    private final Map<Path, List<ClusterSpec>> lastValidSpecsByFile = new ConcurrentHashMap<>();
    private volatile String lastObservedStateDigest = "";

    public ApplicationPropertiesClusterProvider(KarasuProperties properties, Environment environment) {
        this.properties = properties;
        this.environment = environment;
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
        List<ClusterSpec> specs = reloadAllTargets();
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
        int intervalSec = Math.max(2, properties.getConfig().getWatchIntervalSeconds());
        watcher.scheduleWithFixedDelay(this::checkForChangesSafe, intervalSec, intervalSec, TimeUnit.SECONDS);
        log.info("Config & credential watcher enabled (interval={}s, externalFiles={}, dirs={})",
                intervalSec, discoverExternalConfigFiles(), properties.getConfig().getAdditionalClusterDirs());
    }

    /**
     * Triggers an immediate poll check (also useful for deterministic tests).
     */
    public void checkForChangesSafe() {
        try {
            List<ClusterSpec> latest = reloadAllTargets();
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

    @Override
    public void close() {
        watcher.shutdownNow();
    }

    private List<ClusterSpec> reloadAllTargets() {
        List<ClusterSpec> out = new ArrayList<>();
        Set<Path> activeFiles = new HashSet<>();

        // 1. Re-read declared external Spring Boot YAML config files directly from disk (#08)
        List<Path> externalMainFiles = discoverExternalConfigFiles();
        boolean loadedFromExternalMain = false;
        for (Path mainFile : externalMainFiles) {
            if (Files.isRegularFile(mainFile)) {
                activeFiles.add(mainFile);
                List<ClusterSpec> fileSpecs = loadMainConfigFileResilient(mainFile);
                out.addAll(fileSpecs);
                loadedFromExternalMain = true;
            }
        }

        // If no external main file was present, fall back to initial Spring-bound KarasuProperties
        if (!loadedFromExternalMain && properties.getClusters() != null) {
            for (ClusterItem item : properties.getClusters()) {
                addConverted(out, properties, item, "application.yml");
            }
        }

        // 2. Load drop-in cluster YAML directories
        for (String dirStr : properties.getConfig().getAdditionalClusterDirs()) {
            if (dirStr == null || dirStr.isBlank()) {
                continue;
            }
            Path dir = Path.of(dirStr.trim());
            if (!Files.isDirectory(dir)) {
                log.debug("Additional cluster directory does not exist yet: {}", dir);
                continue;
            }
            loadDirectory(dir, activeFiles, out);
        }

        // Prune cached entries only for files explicitly removed from disk
        lastValidSpecsByFile.keySet().retainAll(activeFiles);
        return out;
    }

    List<Path> discoverExternalConfigFiles() {
        Set<Path> result = new LinkedHashSet<>();
        if (environment != null) {
            addLocationSpec(result, environment.getProperty("spring.config.location"));
            addLocationSpec(result, environment.getProperty("spring.config.additional-location"));
        }
        if (properties.getConfig().getWatchFiles() != null) {
            for (String f : properties.getConfig().getWatchFiles()) {
                addLocationSpec(result, f);
            }
        }
        return new ArrayList<>(result);
    }

    private void addLocationSpec(Set<Path> out, String rawLocations) {
        if (rawLocations == null || rawLocations.isBlank()) {
            return;
        }
        for (String token : rawLocations.split("[,;]")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.startsWith("optional:")) {
                trimmed = trimmed.substring("optional:".length()).trim();
            }
            if (trimmed.startsWith("classpath:")) {
                continue;
            }
            if (trimmed.startsWith("file:")) {
                trimmed = trimmed.substring("file:".length()).trim();
            }
            if (trimmed.isEmpty()) {
                continue;
            }
            Path p = Path.of(trimmed);
            if (Files.isDirectory(p)) {
                Path yml = p.resolve("application.yml");
                Path yaml = p.resolve("application.yaml");
                if (Files.isRegularFile(yml)) {
                    out.add(yml.toAbsolutePath().normalize());
                }
                if (Files.isRegularFile(yaml)) {
                    out.add(yaml.toAbsolutePath().normalize());
                }
            } else {
                String lower = p.getFileName().toString().toLowerCase();
                if (lower.endsWith(".yml") || lower.endsWith(".yaml")) {
                    out.add(p.toAbsolutePath().normalize());
                }
            }
        }
    }

    private List<ClusterSpec> loadMainConfigFileResilient(Path file) {
        try {
            String raw = Files.readString(file, StandardCharsets.UTF_8);
            if (environment != null) {
                raw = environment.resolvePlaceholders(raw);
            }
            if (raw.isBlank()) {
                lastValidSpecsByFile.put(file, List.of());
                return List.of();
            }
            RootAppConfigWrapper root = yamlMapper.readValue(raw, RootAppConfigWrapper.class);
            if (root == null || root.karasu == null) {
                lastValidSpecsByFile.put(file, List.of());
                return List.of();
            }
            KarasuProperties effectiveProps = root.karasu.toEffectiveProperties();
            List<ClusterSpec> parsed = new ArrayList<>();
            if (root.karasu.clusters != null) {
                for (ClusterItem item : root.karasu.clusters) {
                    parsed.add(effectiveProps.toClusterSpec(item, properties));
                }
            }
            List<ClusterSpec> immutable = List.copyOf(parsed);
            lastValidSpecsByFile.put(file, immutable);
            return immutable;
        } catch (Exception e) {
            List<ClusterSpec> fallback = lastValidSpecsByFile.getOrDefault(file, List.of());
            log.error("Failed to reload external config file {} (retaining {} last-known-good target(s)): {}",
                    file, fallback.size(), e.getMessage());
            return fallback;
        }
    }

    private void loadDirectory(Path dir, Set<Path> activeFiles, List<ClusterSpec> out) {
        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> yamlFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String n = p.getFileName().toString().toLowerCase();
                        return n.endsWith(".yml") || n.endsWith(".yaml");
                    })
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .map(p -> p.toAbsolutePath().normalize())
                    .toList();

            for (Path file : yamlFiles) {
                activeFiles.add(file);
                out.addAll(parseDropInClusterFileResilient(file));
            }
        } catch (IOException e) {
            log.warn("Failed to list cluster config directory {}: {}", dir, e.getMessage());
            // Retain previously loaded files under this directory on transient I/O error (#06)
            Path normalizedDir = dir.toAbsolutePath().normalize();
            for (Map.Entry<Path, List<ClusterSpec>> entry : lastValidSpecsByFile.entrySet()) {
                if (entry.getKey().startsWith(normalizedDir)) {
                    activeFiles.add(entry.getKey());
                    out.addAll(entry.getValue());
                }
            }
        }
    }

    private List<ClusterSpec> parseDropInClusterFileResilient(Path file) {
        try {
            String raw = Files.readString(file, StandardCharsets.UTF_8);
            if (environment != null) {
                raw = environment.resolvePlaceholders(raw);
            }
            if (raw.isBlank()) {
                lastValidSpecsByFile.put(file, List.of());
                return List.of();
            }
            List<ClusterSpec> parsed = new ArrayList<>();
            ClusterFileWrapper wrapper = yamlMapper.readValue(raw, ClusterFileWrapper.class);
            if (wrapper != null && wrapper.clusters != null) {
                for (ClusterItem item : wrapper.clusters) {
                    parsed.add(properties.toClusterSpec(item));
                }
            } else {
                ClusterItem single = yamlMapper.readValue(raw, ClusterItem.class);
                if (single != null && single.getK8sCluster() != null && single.getKafkaCluster() != null) {
                    parsed.add(properties.toClusterSpec(single));
                }
            }
            List<ClusterSpec> immutable = List.copyOf(parsed);
            lastValidSpecsByFile.put(file, immutable);
            return immutable;
        } catch (Exception e) {
            List<ClusterSpec> fallback = lastValidSpecsByFile.getOrDefault(file, List.of());
            log.error("Failed to parse cluster config file {} (retaining {} last-known-good target(s)): {}",
                    file, fallback.size(), e.getMessage());
            return fallback;
        }
    }

    private void addConverted(List<ClusterSpec> out, KarasuProperties props, ClusterItem item, String source) {
        try {
            out.add(props.toClusterSpec(item));
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

    private static final class RootAppConfigWrapper {
        public RawKarasuSection karasu;
    }

    private static final class RawKarasuSection {
        public RawDefaultsSection defaults;
        public RawMetricsSection metrics;
        public List<ClusterItem> clusters;

        KarasuProperties toEffectiveProperties() {
            KarasuProperties props = new KarasuProperties();
            KarasuProperties.Defaults d = new KarasuProperties.Defaults();
            d.setCollection(defaults != null ? defaults.collection : null);
            d.setEvaluation(defaults != null ? defaults.evaluation : null);
            d.setLimits(defaults != null ? defaults.limits : null);
            props.setDefaults(d);
            if (metrics != null && metrics.snapshotRefreshSeconds != null) {
                props.getMetrics().setSnapshotRefreshSeconds(metrics.snapshotRefreshSeconds);
            } else {
                props.setMetrics(null);
            }
            return props;
        }
    }

    private static final class RawDefaultsSection {
        public KarasuProperties.CollectionItem collection;
        public KarasuProperties.EvaluationItem evaluation;
        public KarasuProperties.LimitsItem limits;
    }

    private static final class RawMetricsSection {
        public Integer snapshotRefreshSeconds;
    }

    private static final class ClusterFileWrapper {
        public List<ClusterItem> clusters;
    }
}
