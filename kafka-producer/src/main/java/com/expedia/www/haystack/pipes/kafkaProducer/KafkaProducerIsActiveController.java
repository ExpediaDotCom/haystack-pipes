/*
 * Copyright 2018 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A very simple Spring Boot application that is intended to support only a single REST endpoint (index.html)
 * to indicate that the JVM is running.
 */
@SpringBootApplication
@Component
public class KafkaProducerIsActiveController extends SpringBootServletInitializer {
    // Singleton, initialized on first constructor call, so that future instances created by Spring during unit tests
    // will not overwrite the initial INSTANCE (with mocks) created by the unit tests.
    @VisibleForTesting
    static final AtomicReference<KafkaProducerIsActiveController> INSTANCE = new AtomicReference<>();
    @VisibleForTesting static final String STARTUP_MSG = "Starting FirehoseIsActiveController";

    private final ProtobufToKafkaProducer protobufToKafkaProducer;
    private final Factory factory;
    private final Logger logger;

    @Autowired
    KafkaProducerIsActiveController(ProtobufToKafkaProducer protobufToKafkaProducer,
                               Factory kafkaProducerIsActiveControllerFactory,
                               Logger kafkaProducerIsActiveControllerLogger) {
        this.protobufToKafkaProducer = protobufToKafkaProducer;
        this.factory = kafkaProducerIsActiveControllerFactory;
        this.logger = kafkaProducerIsActiveControllerLogger;
        INSTANCE.compareAndSet(null, this);
    }

    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(SpringConfig.class);
        INSTANCE.get().logger.info(STARTUP_MSG);
        INSTANCE.get().protobufToKafkaProducer.main();
        INSTANCE.get().factory.createSpringApplication().run(args);
    }

    static class Factory {
        SpringApplication createSpringApplication() {
            return new SpringApplication(KafkaProducerIsActiveController.class);
        }
    }
}
