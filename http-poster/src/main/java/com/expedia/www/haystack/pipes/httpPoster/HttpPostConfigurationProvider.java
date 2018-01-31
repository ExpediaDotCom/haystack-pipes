package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import java.util.Map;

public class HttpPostConfigurationProvider implements HttpPostConfig {
    private static final String HAYSTACK_HTTPPOST_CONFIG_PREFIX = "haystack.httppost";

    private final HttpPostConfig httpPostConfig;

    HttpPostConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        httpPostConfig = configurationProvider.bind(HAYSTACK_HTTPPOST_CONFIG_PREFIX, HttpPostConfig.class);
    }

    @Override
    public int maxbytes() {
        return httpPostConfig.maxbytes();
    }

    @Override
    public String endpoint() {
        return httpPostConfig.endpoint();
    }

    @Override
    public String url() {
        return httpPostConfig.url();
    }

    @Override
    public String bodyprefix() {
        return httpPostConfig.bodyprefix();
    }

    @Override
    public String bodysuffix() {
        return httpPostConfig.bodysuffix();
    }

    @Override
    public String separator() {
        return httpPostConfig.separator();
    }

    @Override
    public Map<String, String> headers() {
        return httpPostConfig.headers();
    }
}
