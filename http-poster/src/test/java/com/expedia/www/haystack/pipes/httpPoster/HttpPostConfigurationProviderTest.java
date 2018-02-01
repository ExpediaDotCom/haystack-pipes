package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostActionTest.HEADERS;
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

    @Test
    public void testUrl() {
        final String url = httpPostConfigurationProvider.url();

        assertEquals("https://collector.test.expedia.com/haystack-spans.json?stream=true&persist=false&multilines=true", url);
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
}
