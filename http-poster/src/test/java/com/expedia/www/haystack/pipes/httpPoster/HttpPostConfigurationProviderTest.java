package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpPostConfigurationProviderTest {
    static final int LARGEST_POSSIBLE_MAX_BYTES = (1024 + 512) * 1024; // 1.5 MB
    private HttpPostConfigurationProvider httpPostConfigurationProvider;

    @Before
    public void setUp() {
        httpPostConfigurationProvider = new HttpPostConfigurationProvider();
    }

    @Test
    public void testMaxBytes() {
        final int maxBytes = httpPostConfigurationProvider.maxbytes();

        assertEquals(LARGEST_POSSIBLE_MAX_BYTES, maxBytes);
    }
}
