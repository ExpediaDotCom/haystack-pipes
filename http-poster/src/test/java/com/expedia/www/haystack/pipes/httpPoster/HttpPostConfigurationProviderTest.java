package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpPostConfigurationProviderTest {
    private HttpPostConfigurationProvider httpPostConfigurationProvider;

    @Before
    public void setUp() {
        httpPostConfigurationProvider = new HttpPostConfigurationProvider();
    }

    @Test
    public void testMaxBytes() {
        final int maxBytes = httpPostConfigurationProvider.maxbytes();

        assertEquals(1024 * 1024 - 1, maxBytes);
    }
}
