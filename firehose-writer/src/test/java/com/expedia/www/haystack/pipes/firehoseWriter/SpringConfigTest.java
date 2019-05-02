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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConsumerStarter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.firehoseWriter.SpringConfig.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SpringConfigTest {
    private static final String URL = "https://firehose.us-west-2.amazonaws.com";
    private static final String SIGNING_REGION = "us-west-2";

    @Mock
    private MetricObjects mockMetricObjects;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;
    @Mock
    private Timer mockTimer;
    @Mock
    private Counter mockCounter;
    @Mock
    private HealthController mockHealthController;

    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig(mockMetricObjects);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockMetricObjects, mockFirehoseConfigurationProvider, mockTimer, mockCounter,
                mockHealthController);
    }

    @Test
    public void testSpanCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.spanCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), SPAN_COUNTER_NAME);
    }

    @Test
    public void testSuccessCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.successCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), SUCCESS_COUNTER_NAME);
    }

    @Test
    public void testFailureCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.failureCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), FAILURE_COUNTER_NAME);
    }

    @Test
    public void testExceptionCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.exceptionCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), EXCEPTION_COUNTER_NAME);
    }

    @Test
    public void testSocketTimeoutCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.socketTimeoutCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                S3Sender.class.getName(), SOCKET_TIMEOUT_COUNTER_NAME);
    }

    @Test
    public void testThrottledCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.throttledCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                Batch.class.getName(), THROTTLED_COUNTER_NAME);
    }

    @Test
    public void testPutBatchRequestTimer() {
        when(mockMetricObjects.createAndRegisterBasicTimer(anyString(), anyString(), anyString(),
                anyString(), any(TimeUnit.class))).thenReturn(mockTimer);

        assertNotNull(springConfig.putBatchRequestTimer());

        verify(mockMetricObjects).createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION, FirehoseProcessor.class.getName(),
                PUT_BATCH_REQUEST_TIMER_NAME, MILLISECONDS);
    }

    @Test
    public void testSpanArrivalTimer() {
        when(mockMetricObjects.createAndRegisterBasicTimer(anyString(), anyString(), anyString(),
                anyString(), any(TimeUnit.class))).thenReturn(mockTimer);

        assertNotNull(springConfig.spanArrivalTimer());

        verify(mockMetricObjects).createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION, FirehoseProcessor.class.getName(),
                SPAN_ARRIVAL_TIMER_NAME, MILLISECONDS);
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaConsumerStarter kafkaStreamStarter = springConfig.kafkaConsumerStarter(mockHealthController);
        assertSame(ProtobufToFirehoseProducer.class, kafkaStreamStarter.containingClass);
        assertSame(APPLICATION, kafkaStreamStarter.clientId);
    }

    @Test
    public void testFirehoseProcessorLogger() {
        final Logger logger = springConfig.firehoseProcessorLogger();

        assertEquals(FirehoseProcessor.class.getName(), logger.getName());
    }

    @Test
    public void testS3SenderLogger() {
        final Logger logger = springConfig.s3SenderLogger();

        assertEquals(S3Sender.class.getName(), logger.getName());
    }

    @Test
    public void testProtobufToFirehoseProducerLogger() {
        final Logger logger = springConfig.protobufToFirehoseProducerLogger();

        assertEquals(ProtobufToFirehoseProducer.class.getName(), logger.getName());
    }

    @Test
    public void testFirehoseIsActiveControllerLogger() {
        final Logger logger = springConfig.firehoseIsActiveControllerLogger();

        assertEquals(FirehoseIsActiveController.class.getName(), logger.getName());
    }

    @Test
    public void testBatchLogger() {
        final Logger logger = springConfig.batchLogger();

        assertEquals(Batch.class.getName(), logger.getName());
    }

    @Test
    public void testFailedRecordExtractorLogger() {
        final Logger logger = springConfig.failedRecordExtractorLogger();

        assertEquals(FailedRecordExtractor.class.getName(), logger.getName());
    }

    @Test
    public void testInternalFailureErrorLoggerLogger() {
        final Logger logger = springConfig.internalFailureErrorLoggerLogger();

        assertEquals(InternalFailureErrorLogger.class.getName(), logger.getName());
    }

    @Test
    public void testUnexpectedExceptionLoggerLogger() {
        final Logger logger = springConfig.unexpectedExceptionLoggerLogger();

        assertEquals(UnexpectedExceptionLogger.class.getName(), logger.getName());
    }

    @Test
    public void testEndpointConfiguration() {
        final EndpointConfiguration endpointConfiguration = springConfig.endpointConfiguration(URL, SIGNING_REGION);

        assertEquals(URL, endpointConfiguration.getServiceEndpoint());
        assertEquals(SIGNING_REGION, endpointConfiguration.getSigningRegion());
    }

    @Test
    public void testUrl() {
        when(mockFirehoseConfigurationProvider.url()).thenReturn(URL);

        final String url = springConfig.url(mockFirehoseConfigurationProvider);

        assertEquals(URL, url);
        verify(mockFirehoseConfigurationProvider).url();
    }

    @Test
    public void testSigningRegion() {
        when(mockFirehoseConfigurationProvider.signingregion()).thenReturn(SIGNING_REGION);

        final String signingRegion = springConfig.signingregion(mockFirehoseConfigurationProvider);

        assertEquals(SIGNING_REGION, signingRegion);
        verify(mockFirehoseConfigurationProvider).signingregion();
    }

    @Test
    public void testClientConfiguration() {
        final ClientConfiguration clientConfiguration = springConfig.clientConfiguration();

        assertTrue(clientConfiguration.useGzip());
    }

    // All of the other beans in SpringConfig use default constructors, or use arguments provided by other Spring beans
    // in SpringConfig, so tests on the methods that create those beans have little value. The
    // amazonKinesisFirehoseAsync() @bean method should be tested but is difficult to test well because of how deeply
    // the AWS SDK code hides its instance variables.
}
