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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamBuilderBase;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;

public class ProtobufToKafkaProducer extends KafkaStreamBuilderBase {
    public ProtobufToKafkaProducer(KafkaStreamStarter kafkaStreamStarter,
                                   SerdeFactory serdeFactory,
                                   KafkaToKafkaPipeline kafkaToKafkaPipeline,
                                   KafkaConsumerConfig kafkaConsumerConfig) {
        super(kafkaStreamStarter, serdeFactory, Constants.APPLICATION, kafkaConsumerConfig, kafkaToKafkaPipeline);
    }
}
