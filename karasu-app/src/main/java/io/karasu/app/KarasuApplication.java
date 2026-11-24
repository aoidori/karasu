/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for Karasu - Java 17 / Spring Boot 3 multi-cluster Kafka consumer lag monitor.
 */
@SpringBootApplication
public class KarasuApplication {

    public static void main(String[] args) {
        SpringApplication.run(KarasuApplication.class, args);
    }
}
