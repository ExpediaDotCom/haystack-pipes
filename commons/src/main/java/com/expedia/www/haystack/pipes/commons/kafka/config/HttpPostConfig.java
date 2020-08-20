package com.expedia.www.haystack.pipes.commons.kafka.config;

import java.util.Map;

public class HttpPostConfig {
    /**
     * The maximum number of bytes to buffer before posting
     * @return maximum bytes
     */
    private String maxBytes;

    /**
     * The URL to which the POST will be sent
     * @return URL
     */
    private String url;

    /**
     * The text that should be emitted first in the POST body before emitting the Span JSON
     * @return prefix
     */
    private String bodyPrefix;

    /**
     * The text that should be emitted last in the POST body after emitting the Span JSON
     * @return suffix
     */
    private String bodySuffix;

    /**
     * The text that should be emitted between Span objects
     * @return separator
     */
    private String separator;

    /**
     * The HTTP headers to send in the POST
     * @return headers
     */
    private Map<String, String> headers;

    /**
     * The percentage of Span objects that will be posted to the HTTP endpoint.
     * @return percentage to use, between 0 and 100 inclusive
     */
    private String pollPercent;

    public HttpPostConfig(final String maxBytes, final String url, final String bodyPrefix, final String bodySuffix, final String separator, final Map<String, String> headers, final String pollPercent) {
        this.maxBytes = maxBytes;
        this.url = url;
        this.bodyPrefix = bodyPrefix;
        this.bodySuffix = bodySuffix;
        this.separator = separator;
        this.headers = headers;
        this.pollPercent = pollPercent;
    }

    public String getMaxBytes() {
        return this.maxBytes;
    }

    public String getUrl() {
        return this.url;
    }

    public String getBodyPrefix() {
        return this.bodyPrefix;
    }

    public String getBodySuffix() {
        return this.bodySuffix;
    }

    public String getSeparator() {
        return this.separator;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public String getPollPercent() {
        return this.pollPercent;
    }
}
