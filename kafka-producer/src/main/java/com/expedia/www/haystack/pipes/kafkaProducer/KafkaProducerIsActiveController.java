/*
 * Copyright 2017 Expedia, Inc.
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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * A very simple Spring Boot application that is intended to support only a single REST endpoint (index.html)
 * to indicate that the JVM is running.
 */
@SpringBootApplication
public class KafkaProducerIsActiveController extends SpringBootServletInitializer {
    static Factory factory = new Factory();

    public static void main(String[] args) {
        factory.createProtobufToKafkaProducer().main();
        factory.createSpringApplication().run(args);
    }

    static class Factory {
        ProtobufToKafkaProducer createProtobufToKafkaProducer() {
            return new ProtobufToKafkaProducer();
        }

        SpringApplication createSpringApplication() {
            return new SpringApplication(KafkaProducerIsActiveController.class);
        }
    }
}
