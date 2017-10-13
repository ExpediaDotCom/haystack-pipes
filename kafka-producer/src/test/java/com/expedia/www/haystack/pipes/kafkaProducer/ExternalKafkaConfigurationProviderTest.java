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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExternalKafkaConfigurationProviderTest {
    private ExternalKafkaConfigurationProvider externalKafkaConfigurationProvider;

    @Before
    public void setUp() {
        externalKafkaConfigurationProvider = new ExternalKafkaConfigurationProvider();
    }

    @Test
    public void testBrokers() {
        assertEquals("localhost", externalKafkaConfigurationProvider.brokers());
    }

    @Test
    public void testPort() {
        assertEquals(9093, externalKafkaConfigurationProvider.port());
    }

    @Test
    public void testToTopic() {
        assertEquals("externalKafkaTopic", externalKafkaConfigurationProvider.toTopic());
    }

    @Test
    public void testAcks() {
        assertEquals("0", externalKafkaConfigurationProvider.acks());
    }

    @Test
    public void testBatchSize() {
        assertEquals(8192, externalKafkaConfigurationProvider.batchSize());
    }

    @Test
    public void testLingerMs() {
        assertEquals(4, externalKafkaConfigurationProvider.lingerMs());
    }

    @Test
    public void testBufferMemory() {
        assertEquals(1024, externalKafkaConfigurationProvider.bufferMemory());
    }

}
