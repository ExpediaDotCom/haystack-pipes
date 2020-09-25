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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerMetricsTest {

    @Mock
    private MetricRegistry mockMetricRegistry;
    @Mock
    private Counter mockSuccessCounter;
    @Mock
    private Counter mockFailureCounter;
    @Mock
    private Counter mockRequestCounter;
    @Mock
    private Timer mockTimer;

    private KafkaProducerMetrics kafkaProducerMetrics;


    @Before
    public void setUp() {
        when(mockMetricRegistry.counter("default_kafka_requests_counter")).thenReturn(mockRequestCounter);
        when(mockMetricRegistry.counter("default_kafka_success_counter")).thenReturn(mockSuccessCounter);
        when(mockMetricRegistry.counter("default_kafka_failure_counter")).thenReturn(mockFailureCounter);
        when(mockMetricRegistry.timer("default_kafka_timer")).thenReturn(mockTimer);
        kafkaProducerMetrics = new KafkaProducerMetrics("default_kafka", mockMetricRegistry);
    }

    @Test
    public void incSuccessCounter() {
        kafkaProducerMetrics.incSuccessCounter();
        verify(mockSuccessCounter).inc();
    }

    @Test
    public void incFailureCounter() {
        kafkaProducerMetrics.incFailureCounter();
        verify(mockFailureCounter).inc();
    }

    @Test
    public void incRequestCounter() {
        kafkaProducerMetrics.incRequestCounter();
        verify(mockRequestCounter).inc();
    }

    @Test
    public void getTimer() {
        assertEquals(mockTimer, kafkaProducerMetrics.getTimer());
    }
}