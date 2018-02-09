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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.kafkaProducer.KafkaProducerIsActiveController.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;

import java.util.Set;

import static com.expedia.www.haystack.pipes.kafkaProducer.KafkaProducerIsActiveController.STARTUP_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerIsActiveControllerTest {
    @Mock
    private Factory mockFactory;

    @Mock
    private ProtobufToKafkaProducer mockProtobufToKafkaProducer;

    @Mock
    private SpringApplication mockSpringApplication;

    @Mock
    private Logger mockLogger;

    private Factory factory;

    @Before
    public void setUp() {
        storeKafkaProducerIsActiveControllerWithMocksInStaticInstance();
        factory = new Factory();
    }

    private void storeKafkaProducerIsActiveControllerWithMocksInStaticInstance() {
        new KafkaProducerIsActiveController(mockProtobufToKafkaProducer, mockFactory, mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockProtobufToKafkaProducer, mockFactory, mockSpringApplication, mockLogger);
        clearKafkaProducerIsActiveControllerInStaticInstance();
    }

    private void clearKafkaProducerIsActiveControllerInStaticInstance() {
        KafkaProducerIsActiveController.INSTANCE.set(null);
    }

    @Test
    public void testMain() {
        when(mockFactory.createSpringApplication()).thenReturn(mockSpringApplication);

        final String[] args = new String[0];
        KafkaProducerIsActiveController.main(args);

        verify(mockLogger).info(STARTUP_MSG);
        verify(mockFactory).createSpringApplication();
        verify(mockProtobufToKafkaProducer).main();
        verify(mockSpringApplication).run(args);
    }

    @Test
    public void testFactoryCreateSpringApplication() {
        final SpringApplication springApplication = factory.createSpringApplication();

        final Set<Object> sources = springApplication.getSources();
        assertEquals(1, sources.size());
        final Object[] objects = sources.toArray();
        assertSame(KafkaProducerIsActiveController.class, objects[0]);
    }
}