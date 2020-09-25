/*
 * Copyright 2020 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

/*
This class wraps kafka Producer with default topic defined in base.conf
 */
public class KafkaProducerWrapper {
    private String defaultTopic;
    private String name;
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaProducerMetrics kafkaProducerMetrics;

    public KafkaProducerWrapper(String defaultTopic, String name, KafkaProducer<String, String> kafkaProducer, KafkaProducerMetrics kafkaProducerMetrics) {
        this.defaultTopic = defaultTopic;
        this.name = name;
        this.kafkaProducer = kafkaProducer;
        this.kafkaProducerMetrics = kafkaProducerMetrics;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getName() {
        return name;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public KafkaProducerMetrics getKafkaProducerMetrics() {
        return kafkaProducerMetrics;
    }
}
