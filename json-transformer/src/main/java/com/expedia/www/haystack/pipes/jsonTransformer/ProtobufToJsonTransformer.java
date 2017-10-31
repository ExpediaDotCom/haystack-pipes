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
package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.Metrics;
import com.expedia.www.haystack.pipes.commons.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class ProtobufToJsonTransformer implements KafkaStreamBuilder {
    static final String CLIENT_ID = "haystack-pipes-protobuf-to-json-transformer";
    static Metrics metrics = new Metrics();

    private static final KafkaConfigurationProvider KAFKA_CONFIGURATION_PROVIDER = new KafkaConfigurationProvider();

    final KafkaStreamStarter kafkaStreamStarter;
    private final SpanSerdeFactory spanSerdeFactory;

    ProtobufToJsonTransformer() {
        this(new KafkaStreamStarter(ProtobufToJsonTransformer.class, CLIENT_ID), new SpanSerdeFactory());
    }

    ProtobufToJsonTransformer(KafkaStreamStarter kafkaStreamStarter, SpanSerdeFactory spanSerdeFactory) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.spanSerdeFactory = spanSerdeFactory;
    }

    /**
     * main() is an instance method because it is called by the static void IsActiveController.main(String [] args);
     * making it an instance method facilitates unit testing.
     */
    void main() {
        metrics.startMetricsPolling();
        kafkaStreamStarter.createAndStartStream(this);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(Constants.APPLICATION);
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, Span> stream = kStreamBuilder.stream(
                stringSerde, spanSerde, KAFKA_CONFIGURATION_PROVIDER.fromtopic());
        stream.mapValues(span -> Span.newBuilder(span).build()).to(
                stringSerde, spanSerde, KAFKA_CONFIGURATION_PROVIDER.totopic());
    }

}
