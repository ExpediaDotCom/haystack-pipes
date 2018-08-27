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

import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.STARTUP_MESSAGE;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseProcessorSupplierTest {
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final int MAX_PARALLELISM_PER_SHARD = RANDOM.nextInt(Byte.MAX_VALUE);

    @Mock
    private Batch mockBatch;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;
    @Mock
    private FirehoseCountersAndTimer mockFirehoseCountersAndTimer;
    @Mock
    private FirehoseProcessor.Factory mockFirehoseProcessorFactory;
    @Mock
    private Logger mockFirehoseProcessorLogger;
    @Mock
    private S3Sender mockS3Sender;
    @Mock
    private Timer mockTimer;

    private FirehoseProcessorSupplier firehoseProcessorSupplier;

    @Before
    public void setUp() {
        firehoseProcessorSupplier = new FirehoseProcessorSupplier(mockFirehoseProcessorLogger,
                mockFirehoseCountersAndTimer, () -> mockBatch, mockFirehoseProcessorFactory,
                mockFirehoseConfigurationProvider, mockS3Sender);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockFirehoseConfigurationProvider);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockFirehoseProcessorFactory);
        verifyNoMoreInteractions(mockFirehoseProcessorLogger);
        verifyNoMoreInteractions(mockS3Sender);
        verifyNoMoreInteractions(mockTimer);
    }

    @Test
    public void testGet() {
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);
        when(mockFirehoseConfigurationProvider.maxparallelismpershard()).thenReturn(MAX_PARALLELISM_PER_SHARD);

        assertNotNull(firehoseProcessorSupplier.get());

        verify(mockFirehoseConfigurationProvider).streamname();
        verify(mockFirehoseConfigurationProvider).maxparallelismpershard();
        verify(mockFirehoseProcessorLogger).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
        verify(mockFirehoseProcessorFactory).createSemaphore(MAX_PARALLELISM_PER_SHARD);
    }
}
