/*
 * Copyright 2020 Expedia, Inc.
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

import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.kafkaProducer.config.KafkaProducerConfig;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProjectConfigurationTest {

    private ProjectConfiguration projectConfiguration;

    @Before
    public void setUp() {
        projectConfiguration = ProjectConfiguration.getInstance();
    }

    @Test
    public void testGetInstance() {
        ProjectConfiguration newProjectConfiguration = ProjectConfiguration.getInstance();
        assertEquals(projectConfiguration, newProjectConfiguration);
    }

    @Test
    public void testGetKafkaConsumerConfig() {
        KafkaConsumerConfig kafkaConsumerConfig = projectConfiguration.getKafkaConsumerConfig();
        assertEquals("localhost", kafkaConsumerConfig.brokers());
        assertEquals(9092, kafkaConsumerConfig.port());
        assertEquals("json-spans", kafkaConsumerConfig.fromtopic());
        assertEquals(1, kafkaConsumerConfig.threadcount());
        assertEquals(15000, kafkaConsumerConfig.sessiontimeout());
        assertEquals(10, kafkaConsumerConfig.maxwakeups());
        assertEquals(3000, kafkaConsumerConfig.wakeuptimeoutms());
        assertEquals(250, kafkaConsumerConfig.polltimeoutms());
        assertEquals(3000, kafkaConsumerConfig.commitms());
    }

    @Test
    public void getKafkaProducerConfigList() {
        List<KafkaProducerConfig> kafkaProducerConfigs = projectConfiguration.getKafkaProducerConfigs();
        assertEquals(1, kafkaProducerConfigs.size());
        KafkaProducerConfig kafkaProducerConfig = kafkaProducerConfigs.get(0);
        assertEquals("localhost:9092", kafkaProducerConfig.getBrokers());
        assertEquals(9093, kafkaProducerConfig.getPort());
        assertEquals("externalKafkaTopic", kafkaProducerConfig.getToTopic());
        assertEquals("0", kafkaProducerConfig.getAcks());
        assertEquals(8192, kafkaProducerConfig.getBatchSize());
        assertEquals(4, kafkaProducerConfig.getLingerMs());
        assertEquals(1024, kafkaProducerConfig.getBufferMemory());

    }

    @Test
    public void testGetSpanExtractorConfigs() {
        Map<String,Config> spanKeyExtractorConfigMap = projectConfiguration.getSpanExtractorConfigs();
        assertEquals(1, spanKeyExtractorConfigMap.size());
    }
}