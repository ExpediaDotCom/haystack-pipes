package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostActionTest.HEADERS;
import static org.junit.Assert.assertEquals;

public class HttpPostConfigurationProviderTest {
    static final String LARGEST_POSSIBLE_MAX_BYTES = Integer.toString((1024 + 512) * 1024); // 1.5 MB
    private HttpPostConfigurationProvider httpPostConfigurationProvider;

    @Before
    public void setUp() {
        httpPostConfigurationProvider = new HttpPostConfigurationProvider();
    }

    @Test
    public void testMaxBytes() {
        final String maxBytes = httpPostConfigurationProvider.maxbytes();

        assertEquals(LARGEST_POSSIBLE_MAX_BYTES, maxBytes);
    }

    @Test
    public void testUrl() {
        final String url = httpPostConfigurationProvider.url();

        assertEquals("http://localhost", url);
    }

    @Test
    public void testBodyPrefix() {
        final String bodyPrefix = httpPostConfigurationProvider.bodyprefix();

        assertEquals("[", bodyPrefix);
    }

    @Test
    public void testBodySuffix() {
        final String bodySuffix = httpPostConfigurationProvider.bodysuffix();

        assertEquals("]", bodySuffix);
    }

    @Test
    public void testSeparator() {
        final String separator = httpPostConfigurationProvider.separator();

        assertEquals(",", separator);
    }

    @Test
    public void testHeaders() {
        final Map<String, String> headers = httpPostConfigurationProvider.headers();

        assertEquals(HEADERS, headers);
    }

    @Test
    public void testPollPercent() {
        final int pollPercent = Integer.parseInt(httpPostConfigurationProvider.pollpercent());

        assertEquals(42, pollPercent);
    }
}
