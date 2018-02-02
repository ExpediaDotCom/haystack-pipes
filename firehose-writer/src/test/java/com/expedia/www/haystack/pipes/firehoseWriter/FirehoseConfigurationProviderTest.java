package com.expedia.www.haystack.pipes.firehoseWriter;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FirehoseConfigurationProviderTest {
    private FirehoseConfigurationProvider firehoseConfigurationProvider;

    @Before
    public void setUp() {
        firehoseConfigurationProvider = new FirehoseConfigurationProvider();
    }

    @Test
    public void testRetryCount() {
        assertEquals(3, firehoseConfigurationProvider.retrycount());
    }

    @Test
    public void testUrl() {
        assertEquals("https://firehose.us-west-2.amazonaws.com", firehoseConfigurationProvider.url());
    }

    @Test
    public void testStreamName() {
        assertEquals("haystack-traces-test", firehoseConfigurationProvider.streamname());
    }
}
