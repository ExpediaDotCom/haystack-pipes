package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
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
    private Counters mockCounters;
    @Mock
    private Timer mockTimer;
    @Mock
    private Batch mockBatch;
    @Mock
    private AmazonKinesisFirehose mockAmazonKinesisFirehose;
    @Mock
    private FirehoseProcessor.Factory mockFirehoseProcessorFactory;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;

    private FirehoseProcessorSupplier firehoseProcessorSupplier;

    @Before
    public void setUp() {
        firehoseProcessorSupplier = new FirehoseProcessorSupplier(mockFirehoseProcessorLogger, mockCounters, mockTimer,
                mockBatch, mockAmazonKinesisFirehose, mockFirehoseProcessorFactory, mockFirehoseConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFirehoseProcessorLogger, mockCounters, mockTimer, mockBatch,
                mockAmazonKinesisFirehose, mockFirehoseProcessorFactory, mockFirehoseConfigurationProvider);
    }

    @Test
    public void testGet() {
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);

        assertNotNull(firehoseProcessorSupplier.get());

        verify(mockFirehoseConfigurationProvider).streamname();
        verify(mockFirehoseProcessorLogger).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
    }
}
