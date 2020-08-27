package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostActionTest.HEADERS;
import static org.junit.Assert.assertEquals;

public class HttpPostConfigTest {
    static final String LARGEST_POSSIBLE_MAX_BYTES = Integer.toString((1024 + 512) * 1024); // 1.5 MB
    private HttpPostConfig httpPostConfig;

    @Before
    public void setUp() {
        httpPostConfig = ProjectConfiguration.getInstance().getHttpPostConfig();
    }

    @Test
    public void testMaxBytes() {
        final String maxBytes = httpPostConfig.getMaxBytes();

        assertEquals(LARGEST_POSSIBLE_MAX_BYTES, maxBytes);
    }

    @Test
    public void testUrl() {
        final String url = httpPostConfig.getUrl();

        assertEquals("http://localhost", url);
    }

    @Test
    public void testBodyPrefix() {
        final String bodyPrefix = httpPostConfig.getBodyPrefix();

        assertEquals("[", bodyPrefix);
    }

    @Test
    public void testBodySuffix() {
        final String bodySuffix = httpPostConfig.getBodySuffix();

        assertEquals("]", bodySuffix);
    }

    @Test
    public void testSeparator() {
        final String separator = httpPostConfig.getSeparator();

        assertEquals(",", separator);
    }

    @Test
    public void testHeaders() {
        final Map<String, String> headers = httpPostConfig.getHeaders();

        assertEquals(HEADERS, headers);
    }

    @Test
    public void testPollPercent() {
        final int pollPercent = Integer.parseInt(httpPostConfig.getPollPercent());

        assertEquals(42, pollPercent);
    }
}
