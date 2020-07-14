package com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config;

import com.expedia.www.haystack.commons.config.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

public class SpanKeyExtractorConfigProvider implements SpanKeyExtractorConfig {

    private final SpanKeyExtractorConfig spanKeyExtractorConfig;

    public SpanKeyExtractorConfigProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        spanKeyExtractorConfig = configurationProvider.bind("haystack.extractor", SpanKeyExtractorConfig.class);
    }

    public SpanKeyExtractorConfig getSpanKeyExtractorConfig() {
        return spanKeyExtractorConfig;
    }

    @Override
    public String directory() {
        return spanKeyExtractorConfig.directory();
    }

    @Override
    public String fileName() {
        return spanKeyExtractorConfig.fileName();
    }
}
