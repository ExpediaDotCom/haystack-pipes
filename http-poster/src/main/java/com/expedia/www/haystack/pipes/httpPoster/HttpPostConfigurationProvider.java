package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.commons.config.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import java.util.Map;

/**
 * Configurations that are integers return String because configurations in environment variables can only be Strings.
 */
public class HttpPostConfigurationProvider implements HttpPostConfig {
    private static final String HAYSTACK_HTTPPOST_CONFIG_PREFIX = "haystack.httppost";

    private final HttpPostConfig httpPostConfig;

    HttpPostConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        httpPostConfig = configurationProvider.bind(HAYSTACK_HTTPPOST_CONFIG_PREFIX, HttpPostConfig.class);
    }

    @Override
    public String maxbytes() {
        return httpPostConfig.maxbytes();
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

    @Override
    public String pollpercent() {
        return httpPostConfig.pollpercent();
    }
}
