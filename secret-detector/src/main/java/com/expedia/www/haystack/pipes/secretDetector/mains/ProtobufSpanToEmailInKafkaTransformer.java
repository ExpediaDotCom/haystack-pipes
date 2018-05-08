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
package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.Detector;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.Main;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

@Component
public class ProtobufSpanToEmailInKafkaTransformer implements KafkaStreamBuilder, Main {
    private final KafkaStreamStarter kafkaStreamStarter;
    private final SpanSerdeFactory spanSerdeFactory;
    private final KafkaConfigurationProvider kafkaConfigurationProvider = new KafkaConfigurationProvider();
    private final Detector detector;

    @Autowired
    public ProtobufSpanToEmailInKafkaTransformer(KafkaStreamStarter kafkaStreamStarter,
                                          SpanSerdeFactory spanSerdeFactory,
                                          Detector detector) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.spanSerdeFactory = spanSerdeFactory;
        this.detector = detector;
    }

    @Override
    public void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(APPLICATION);
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, Span> stream = kStreamBuilder.stream(
                stringSerde, spanSerde, kafkaConfigurationProvider.fromtopic());
        stream.flatMapValues(detector::apply).to(stringSerde, stringSerde, kafkaConfigurationProvider.totopic());
    }
}
