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
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToFirehoseProducerTest {
    private static final String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaConsumerStarter mockKafkaConsumerStarter;

    @Mock
    private FirehoseProcessorSupplier mockFirehoseProcessorSupplier;
    @Mock
    private KafkaConsumerConfig mockKafkaConsumerConfig;

    private ProtobufToFirehoseProducer protobufToFirehoseProducer;

    @Before
    public void setUp() {
        protobufToFirehoseProducer = new ProtobufToFirehoseProducer(mockKafkaConsumerStarter,
                mockFirehoseProcessorSupplier);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaConsumerStarter, mockFirehoseProcessorSupplier,
                mockKafkaConsumerConfig);
    }

    @Test
    public void testMain() {
        protobufToFirehoseProducer.main();

        verify(mockKafkaConsumerStarter).createAndStartConsumer(mockFirehoseProcessorSupplier);
    }
}
