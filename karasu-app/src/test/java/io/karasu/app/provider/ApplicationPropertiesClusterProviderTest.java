/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app.provider;

import io.karasu.app.config.KarasuProperties;
import io.karasu.core.config.ClusterSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for findings #06 (broken YAML preserves last-known-good targets)
 * and #08 (hot-reloading external main application.yml via spring.config.additional-location).
 */
class ApplicationPropertiesClusterProviderTest {

    @Test
    void reloadsExternalMainConfigFileAndPreservesLastKnownGoodOnBrokenYaml(@TempDir Path tempDir) throws Exception {
        Path externalMain = tempDir.resolve("application-ext.yml");
        Files.writeString(externalMain, """
                karasu:
                  clusters:
                    - k8s-cluster: k8s-ext
                      kafka-cluster: kafka-main
                      bootstrap-servers:
                        - 127.0.0.1:9092
                      labels:
                        stage: initial
                """);

        MockEnvironment env = new MockEnvironment()
                .withProperty("spring.config.additional-location", "file:" + externalMain);
        KarasuProperties props = new KarasuProperties();
        props.getConfig().setWatchEnabled(false);

        try (ApplicationPropertiesClusterProvider provider = new ApplicationPropertiesClusterProvider(props, env)) {
            AtomicReference<List<ClusterSpec>> received = new AtomicReference<>();
            provider.onTargetsChanged(received::set);

            List<ClusterSpec> initial = provider.loadTargets();
            assertThat(initial).hasSize(1);
            assertThat(initial.get(0).labels().get("stage")).isEqualTo("initial");

            // 1. Update external main config file (including partial defaults override) -> hot reload picks up "refreshed" (#08)
            Files.writeString(externalMain, """
                    karasu:
                      defaults:
                        collection:
                          offset-refresh-seconds: 5
                      clusters:
                        - k8s-cluster: k8s-ext
                          kafka-cluster: kafka-main
                          bootstrap-servers:
                            - 127.0.0.1:9092
                          labels:
                            stage: refreshed
                    """);
            provider.checkForChangesSafe();
            assertThat(received.get()).isNotNull().hasSize(1);
            assertThat(received.get().get(0).labels().get("stage")).isEqualTo("refreshed");
            assertThat(received.get().get(0).collection().offsetRefreshSeconds()).isEqualTo(5);
            assertThat(received.get().get(0).collection().offsetsTopic()).isEqualTo("__consumer_offsets");

            // 2. Write malformed YAML -> last-known-good target is retained instead of returning empty list (#06)
            Files.writeString(externalMain, "karasu:\n  clusters: [unclosed");
            received.set(null);
            provider.checkForChangesSafe();
            // Digest matches last-known-good, so no destructive removal triggered; explicit loadTargets also retains it
            assertThat(provider.loadTargets()).hasSize(1);
            assertThat(provider.loadTargets().get(0).labels().get("stage")).isEqualTo("refreshed");
        }
    }

    @Test
    void retainsLastValidTargetsWhenDropInDirectoryYamlIsBroken(@TempDir Path tempDir) throws Exception {
        Path dropIn = tempDir.resolve("cluster-a.yaml");
        Files.writeString(dropIn, """
                k8s-cluster: k8s-drop
                kafka-cluster: kafka-drop
                bootstrap-servers:
                  - 127.0.0.1:9092
                """);

        KarasuProperties props = new KarasuProperties();
        props.getConfig().setAdditionalClusterDirs(List.of(tempDir.toString()));
        MockEnvironment env = new MockEnvironment();

        try (ApplicationPropertiesClusterProvider provider = new ApplicationPropertiesClusterProvider(props, env)) {
            assertThat(provider.loadTargets()).hasSize(1);

            // Break YAML syntax -> must still retain the valid target
            Files.writeString(dropIn, "k8s-cluster: [broken");
            assertThat(provider.loadTargets()).hasSize(1);
            assertThat(provider.loadTargets().get(0).targetKey().kafkaCluster()).isEqualTo("kafka-drop");

            // Explicit file deletion -> target cleanly removed
            Files.delete(dropIn);
            assertThat(provider.loadTargets()).isEmpty();
        }
    }
}
