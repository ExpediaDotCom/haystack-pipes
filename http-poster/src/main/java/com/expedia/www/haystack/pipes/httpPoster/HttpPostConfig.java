package com.expedia.www.haystack.pipes.httpPoster;

import java.util.Map;

public interface HttpPostConfig {
    int maxbytes();

    String url();

    String bodyprefix();

    String bodysuffix();

    String separator();

    Map<String, String> headers();
}
