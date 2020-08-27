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
import com.expedia.www.haystack.commons.secretDetector.span.SpanDetector;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilder;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.Main;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
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
    private final SerdeFactory serdeFactory;
    private final KafkaConsumerConfig kafkaConsumerConfig = ProjectConfiguration.getInstance().getKafkaConsumerConfig();
    private final SpanDetector spanDetector;

    @Autowired
    public ProtobufSpanToEmailInKafkaTransformer(KafkaStreamStarter kafkaStreamStarter,
                                                 SerdeFactory serdeFactory,
                                                 SpanDetector springWiredDetector) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.serdeFactory = serdeFactory;
        this.spanDetector = springWiredDetector;
    }

    @Override
    public void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<Span> spanSerde = serdeFactory.createJsonProtoSpanSerde(APPLICATION);
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, Span> stream = kStreamBuilder.stream(
                stringSerde, spanSerde, kafkaConsumerConfig.getFromTopic());
        stream.flatMapValues(spanDetector::apply).to(stringSerde, stringSerde, kafkaConsumerConfig.getToTopic());
    }
}
