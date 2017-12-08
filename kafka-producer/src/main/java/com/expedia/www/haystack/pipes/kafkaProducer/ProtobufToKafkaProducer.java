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

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class ProtobufToKafkaProducer implements KafkaStreamBuilder {
    static final String CLIENT_ID = "haystack-pipes-json-to-kafka-producer";

    private static final KafkaConfigurationProvider KAFKA_CONFIGURATION_PROVIDER = new KafkaConfigurationProvider();
    static ProtobufToKafkaProducer instance = new ProtobufToKafkaProducer();
    static Factory factory = new Factory();

    final KafkaStreamStarter kafkaStreamStarter;
    private final SpanSerdeFactory spanSerdeFactory;

    ProtobufToKafkaProducer() {
        this(new KafkaStreamStarter(ProtobufToKafkaProducer.class, CLIENT_ID), new SpanSerdeFactory());
    }

    ProtobufToKafkaProducer(KafkaStreamStarter kafkaStreamStarter, SpanSerdeFactory spanSerdeFactory) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.spanSerdeFactory = spanSerdeFactory;
    }

    /**
     * main() is an instance method because it is called by the static void IsActiveController.main(String [] args);
     * making it an instance method facilitates unit testing.
     */
    void main() {
        instance.kafkaStreamStarter.createAndStartStream(instance);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(Constants.APPLICATION);
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, Span> stream = kStreamBuilder.stream(
                stringSerde, spanSerde, KAFKA_CONFIGURATION_PROVIDER.fromtopic());
        final ForeachAction<String, Span> produceIntoExternalKafkaAction =
                factory.createProduceIntoExternalKafkaAction();
        stream.foreach(produceIntoExternalKafkaAction);
    }

    static class Factory {
        ProduceIntoExternalKafkaAction createProduceIntoExternalKafkaAction() {
            return new ProduceIntoExternalKafkaAction();
        }
    }
}
