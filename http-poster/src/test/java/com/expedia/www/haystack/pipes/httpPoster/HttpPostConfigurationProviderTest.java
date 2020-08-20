package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostActionTest.HEADERS;
import static org.junit.Assert.assertEquals;

public class HttpPostConfigurationProviderTest {
    static final String LARGEST_POSSIBLE_MAX_BYTES = Integer.toString((1024 + 512) * 1024); // 1.5 MB
    private HttpPostConfig httpPostConfigurationProvider;

    @Before
    public void setUp() {
        httpPostConfigurationProvider = new ProjectConfiguration().getHttpPostConfig();
    }

    @Test
    public void testMaxBytes() {
        final String maxBytes = httpPostConfigurationProvider.getMaxBytes();

        assertEquals(LARGEST_POSSIBLE_MAX_BYTES, maxBytes);
    }

    @Test
    public void testUrl() {
        final String url = httpPostConfigurationProvider.getUrl();

        assertEquals("http://localhost", url);
    }

    @Test
    public void testBodyPrefix() {
        final String bodyPrefix = httpPostConfigurationProvider.getBodyPrefix();

        assertEquals("[", bodyPrefix);
    }

    @Test
    public void testBodySuffix() {
        final String bodySuffix = httpPostConfigurationProvider.getBodySuffix();

        assertEquals("]", bodySuffix);
    }

    @Test
    public void testSeparator() {
        final String separator = httpPostConfigurationProvider.getSeparator();

        assertEquals(",", separator);
    }

    @Test
    public void testHeaders() {
        final Map<String, String> headers = httpPostConfigurationProvider.getHeaders();

        assertEquals(HEADERS, headers);
    }

    @Test
    public void testPollPercent() {
        final int pollPercent = Integer.parseInt(httpPostConfigurationProvider.getPollPercent());

        assertEquals(42, pollPercent);
    }
}
