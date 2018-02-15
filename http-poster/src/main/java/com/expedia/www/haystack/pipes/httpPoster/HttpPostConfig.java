package com.expedia.www.haystack.pipes.httpPoster;

import java.util.Map;

public interface HttpPostConfig {
    /**
     * The maximum number of bytes to buffer before posting
     * @return maximum bytes
     */
    int maxbytes();

    /**
     * The URL to which the POST will be sent
     * @return URL
     */
    String url();

    /**
     * The text that should be emitted first in the POST body before emitting the Span JSON
     * @return prefix
     */
    String bodyprefix();

    /**
     * The text that should be emitted last in the POST body after emitting the Span JSON
     * @return suffix
     */
    String bodysuffix();

    /**
     * The text that should be emitted between Span objects
     * @return separator
     */
    String separator();

    /**
     * The HTTP headers to send in the POST
     * @return headers
     */
    Map<String, String> headers();

    /**
     * The percentage of Span objects that will be posted to the HTTP endpoint.
     * @return percentage to use, between 0 and 100 inclusive
     */
    int pollpercent();
}
