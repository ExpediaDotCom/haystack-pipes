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
package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.Main;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

@Component
public class ProtobufSpanMaskerToKafkaTransformer implements KafkaStreamBuilder, Main {
    private final KafkaStreamStarter kafkaStreamStarter;
    private final SerdeFactory serdeFactory;
    private final KafkaConsumerConfig kafkaConfigurationProvider = ProjectConfiguration.getInstance().getKafkaConsumerConfig();
    private final SpanSecretMasker spanSecretMasker;

    @Autowired
    public ProtobufSpanMaskerToKafkaTransformer(KafkaStreamStarter kafkaStreamStarter,
                                                SerdeFactory serdeFactory,
                                                SpanSecretMasker springWiredSpanSecretMasker) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.serdeFactory = serdeFactory;
        this.spanSecretMasker = springWiredSpanSecretMasker;
    }

    @Override
    public void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final KStream<String, Span> stream = kStreamBuilder.stream(
                serdeFactory.createStringSerde(), serdeFactory.createProtoProtoSpanSerde(APPLICATION),
                kafkaConfigurationProvider.getFromTopic());

        stream.mapValues(spanSecretMasker).to(
                serdeFactory.createStringSerde(), serdeFactory.createProtoProtoSpanSerde(APPLICATION),
                kafkaConfigurationProvider.getToTopic());
    }

}
