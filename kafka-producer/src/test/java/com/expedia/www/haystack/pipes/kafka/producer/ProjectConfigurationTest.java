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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.kafka.producer.config.KafkaProducerConfig;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProjectConfigurationTest {

    private ProjectConfiguration projectConfiguration;
    @Mock
    private ProjectConfiguration mockProjectConfiguration;

    private static void injectEnvironmentVariable(String key, String value)
            throws Exception {
        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
        Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
        Object unmodifiableMap = unmodifiableMapField.get(null);
        injectIntoUnmodifiableMap(key, value, unmodifiableMap);
        Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
        Map<String, String> map = (Map<String, String>) mapField.get(null);
        map.put(key, value);
    }

    private static void unSetEnvironmentVariable(String key)
            throws Exception {
        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
        Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
        Object unmodifiableMap = unmodifiableMapField.get(null);
        unSetIntoUnmodifiableMap(key, unmodifiableMap);
        Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
        Map<String, String> map = (Map<String, String>) mapField.get(null);
        map.remove(key);
    }

    private static Field getAccessibleField(Class<?> clazz, String fieldName)
            throws NoSuchFieldException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    private static void injectIntoUnmodifiableMap(String key, String value, Object map)
            throws ReflectiveOperationException {
        Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Field field = getAccessibleField(unmodifiableMap, "m");
        Object obj = field.get(map);
        ((Map<String, String>) obj).put(key, value);
    }

    private static void unSetIntoUnmodifiableMap(String key, Object map)
            throws ReflectiveOperationException {
        Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Field field = getAccessibleField(unmodifiableMap, "m");
        Object obj = field.get(map);
        ((Map<String, String>) obj).remove(key);
    }

    @Before
    public void setUp() {
        projectConfiguration = ProjectConfiguration.getInstance();
    }

    @Test
    public void testGetResourceNameEnvSet() throws Exception {
        ProjectConfiguration realProjectConfiguration = ProjectConfiguration.projectConfiguration;
        ProjectConfiguration.projectConfiguration = ProjectConfiguration.getInstance();
        injectEnvironmentVariable("configFilePath", "config/test.conf");
        assertEquals(System.getenv("configFilePath"), "config/test.conf");
        when(System.getenv("configFilePath")).thenReturn("config/test.conf");
        assertEquals("config/test.conf", projectConfiguration.getResourceName());
        unSetEnvironmentVariable("configFilePath");
        ProjectConfiguration.projectConfiguration = null;
    }

    @Test
    public void testGetInstance() {
        ProjectConfiguration newProjectConfiguration = ProjectConfiguration.getInstance();
        assertEquals(projectConfiguration, newProjectConfiguration);
        ProjectConfiguration.projectConfiguration = null;
        assertNotEquals(ProjectConfiguration.getInstance(), null);
    }

    @Test
    public void testWithEnvVariable() throws IOException {
        when(mockProjectConfiguration.getResourceName()).thenReturn("config/test.conf");
        ProjectConfiguration realProjectConfiguration = ProjectConfiguration.projectConfiguration;
        ProjectConfiguration.projectConfiguration = mockProjectConfiguration;
        assertNotEquals("config/base.conf", mockProjectConfiguration.getResourceName());
        assertEquals("config/base.conf", projectConfiguration.getResourceName());
        ProjectConfiguration.projectConfiguration = realProjectConfiguration;
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
        Map<String, Config> spanKeyExtractorConfigMap = projectConfiguration.getSpanExtractorConfigs();
        assertEquals(1, spanKeyExtractorConfigMap.size());
    }

    @Test
    public void testKafkaConsumerConfigIdempotent() {
        assertEquals(projectConfiguration.getKafkaConsumerConfig(), projectConfiguration.getKafkaConsumerConfig());
    }

    @Test
    public void testKafkaProducerConfigsIdempotent() {
        assertEquals(projectConfiguration.getKafkaProducerConfigs(), projectConfiguration.getKafkaProducerConfigs());
    }

    @Test
    public void testSpanKeyExtractorConfigsIdempotent() {
        assertEquals(projectConfiguration.getSpanExtractorConfigs(), projectConfiguration.getSpanExtractorConfigs());
    }
}