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
package com.expedia.www.haystack.pipes.kafka;

/**
 * Configurations for the KafkaProducer that sends data to an Kafka outside of the Haystack system.
 * For details on this configurations, see Kafka documentation, e.g.
 * http://kafka.apache.org/documentation.html#producerconfigs,
 * https://kafka.apache.org/0102/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html, etc.
 * All method names must be lower case and must not include underscores, because the cf4j framework that manages
 * configuration changes configurations specified by environment variables from upper to lower case and replaces
 * underscores by periods.
 */
public interface KafkaProducerConfig {
    String brokers();

    int port();

    String totopic();

    String acks(); // "-1": all replicas; "0": don't wait; "1": leader writes to its local log; "all": same as "-1"

    int batchsize();

    int lingerms();

    int buffermemory();
}
