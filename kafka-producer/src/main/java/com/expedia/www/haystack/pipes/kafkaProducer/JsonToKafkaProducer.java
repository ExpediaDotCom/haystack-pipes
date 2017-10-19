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

import com.expedia.www.haystack.pipes.commons.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class JsonToKafkaProducer implements KafkaStreamBuilder {
    static final String CLIENT_ID = "haystack-pipes-json-to-kafka-producer";
    static Metrics metrics = new Metrics();

    private static final KafkaConfigurationProvider KAFKA_CONFIGURATION_PROVIDER = new KafkaConfigurationProvider();
    static JsonToKafkaProducer instance = new JsonToKafkaProducer();
    static Factory factory = new Factory();

    final KafkaStreamStarter kafkaStreamStarter;

    JsonToKafkaProducer() {
        this(new KafkaStreamStarter(JsonToKafkaProducer.class, CLIENT_ID));
    }

    JsonToKafkaProducer(KafkaStreamStarter kafkaStreamStarter) {
        this.kafkaStreamStarter = kafkaStreamStarter;
    }

    public static void main(String[] args) {
        metrics.startMetricsPolling();
        instance.kafkaStreamStarter.createAndStartStream(instance);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> stream = kStreamBuilder.stream(
                stringSerde, stringSerde, KAFKA_CONFIGURATION_PROVIDER.fromtopic());
        final ForeachAction<String, String> produceIntoExternalKafkaAction =
                factory.createProduceIntoExternalKafkaAction();
        stream.foreach(produceIntoExternalKafkaAction);
    }

    static class Factory {
        ProduceIntoExternalKafkaAction createProduceIntoExternalKafkaAction() {
            return new ProduceIntoExternalKafkaAction();
        }
    }
}
