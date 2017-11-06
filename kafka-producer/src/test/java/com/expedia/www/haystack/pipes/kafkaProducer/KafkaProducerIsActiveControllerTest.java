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

import com.expedia.www.haystack.pipes.kafkaProducer.KafkaProducerIsActiveController.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.SpringApplication;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerIsActiveControllerTest {
    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    @Mock
    private ProtobufToKafkaProducer mockProtobufToKafkaProducer;

    @Mock
    private SpringApplication mockSpringApplication;

    @Before
    public void setUp() {
        realFactory = KafkaProducerIsActiveController.factory;
        KafkaProducerIsActiveController.factory = mockFactory;
    }

    @After
    public void tearDown() {
        KafkaProducerIsActiveController.factory = realFactory;
        verifyNoMoreInteractions(mockFactory, mockProtobufToKafkaProducer, mockSpringApplication);
    }

    @Test
    public void testMain() {
        when(mockFactory.createProtobufToKafkaProducer()).thenReturn(mockProtobufToKafkaProducer);
        when(mockFactory.createSpringApplication()).thenReturn(mockSpringApplication);

        final String[] args = new String[0];
        KafkaProducerIsActiveController.main(args);

        verify(mockFactory).createProtobufToKafkaProducer();
        verify(mockFactory).createSpringApplication();
        verify(mockProtobufToKafkaProducer).main();
        verify(mockSpringApplication).run(args);
    }

    @Test
    public void testDefaultConstructor() {
        new KafkaProducerIsActiveController();
    }

    @Test
    public void testFactoryCreateProtobufToKafkaProducer() {
        realFactory.createProtobufToKafkaProducer();
    }

    @Test
    public void testFactoryCreateSpringApplication() {
        final SpringApplication springApplication = realFactory.createSpringApplication();

        final Set<Object> sources = springApplication.getSources();
        assertEquals(Collections.singleton(KafkaProducerIsActiveController.class), sources);
    }
}
