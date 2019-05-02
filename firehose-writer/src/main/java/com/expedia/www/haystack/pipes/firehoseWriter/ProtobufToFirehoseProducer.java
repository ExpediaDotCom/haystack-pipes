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

import com.expedia.www.haystack.pipes.commons.kafka.KafkaConsumerStarter;
import com.expedia.www.haystack.pipes.commons.kafka.Main;
import com.expedia.www.haystack.pipes.commons.kafka.SpanProcessorSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class ProtobufToFirehoseProducer implements Main {

    private final KafkaConsumerStarter starter;
    private final SpanProcessorSupplier processorSupplier;

    @Autowired
    ProtobufToFirehoseProducer(KafkaConsumerStarter starter,
                               FirehoseProcessorSupplier processorSupplier) {

        this.starter = starter;
        this.processorSupplier = processorSupplier;
    }

    @Override
    public void main() {
        starter.createAndStartConsumer(processorSupplier);
    }
}
