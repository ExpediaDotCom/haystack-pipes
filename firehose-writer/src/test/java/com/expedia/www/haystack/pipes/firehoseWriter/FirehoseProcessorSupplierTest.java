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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import com.netflix.servo.monitor.Timer;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.STARTUP_MESSAGE;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseProcessorSupplierTest {
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";

    @Mock
    private Logger mockFirehoseProcessorLogger;
    @Mock
    private FirehoseCountersAndTimer mockFirehoseCountersAndTimer;
    @Mock
    private Timer mockTimer;
    @Mock
    private Batch mockBatch;
    @Mock
    private AmazonKinesisFirehoseAsync mockAmazonKinesisFirehoseAsync;
    @Mock
    private Factory mockFirehoseProcessorFactory;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;
    @Mock
    private S3Sender mockS3Sender;

    private FirehoseProcessorSupplier firehoseProcessorSupplier;

    @Before
    public void setUp() {
        firehoseProcessorSupplier = new FirehoseProcessorSupplier(mockFirehoseProcessorLogger,
                mockFirehoseCountersAndTimer, () -> mockBatch,
                mockFirehoseProcessorFactory, mockFirehoseConfigurationProvider, mockS3Sender);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFirehoseProcessorLogger);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockTimer);
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockAmazonKinesisFirehoseAsync);
        verifyNoMoreInteractions(mockFirehoseProcessorFactory);
        verifyNoMoreInteractions(mockFirehoseConfigurationProvider);
        verifyNoMoreInteractions(mockS3Sender);
    }

    @Test
    public void testGet() {
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);

        assertNotNull(firehoseProcessorSupplier.get());

        verify(mockFirehoseConfigurationProvider).streamname();
        verify(mockFirehoseProcessorLogger).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
    }
}
