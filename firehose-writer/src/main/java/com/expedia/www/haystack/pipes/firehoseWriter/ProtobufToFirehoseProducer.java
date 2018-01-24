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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProtobufToFirehoseProducer implements KafkaStreamBuilder {
    private final KafkaStreamStarter kafkaStreamStarter;
    private final SpanSerdeFactory spanSerdeFactory;
    private final FirehoseAction firehoseAction;
    private final KafkaConfigurationProvider kafkaConfigurationProvider;

    @Autowired
    ProtobufToFirehoseProducer(KafkaStreamStarter kafkaStreamStarter,
                               SpanSerdeFactory spanSerdeFactory,
                               FirehoseAction firehoseAction,
                               KafkaConfigurationProvider kafkaConfigurationProvider) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.spanSerdeFactory = spanSerdeFactory;
        this.firehoseAction = firehoseAction;
        this.kafkaConfigurationProvider = kafkaConfigurationProvider;
    }

    /**
     * main() is an instance method because it is called by the static void IsActiveController.main(String [] args);
     * making it an instance method facilitates unit testing.
     */
    void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(Constants.APPLICATION);
        final String fromTopic = kafkaConfigurationProvider.fromtopic();
        final KStream<String, Span> stream = kStreamBuilder.stream(stringSerde, spanSerde, fromTopic);
        stream.foreach(firehoseAction);
    }
}
