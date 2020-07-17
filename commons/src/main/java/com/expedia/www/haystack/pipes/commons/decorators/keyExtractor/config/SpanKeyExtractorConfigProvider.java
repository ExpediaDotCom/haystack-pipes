package com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config;

import com.expedia.www.haystack.commons.config.Configuration;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.loader.SpanKeyExtractorLoader;
import org.cfg4j.provider.ConfigurationProvider;

public class SpanKeyExtractorConfigProvider implements SpanKeyExtractorConfig {

    private final SpanKeyExtractorConfig spanKeyExtractorConfig;

    public SpanKeyExtractorConfigProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        spanKeyExtractorConfig = configurationProvider.bind("haystack.extractor", SpanKeyExtractorConfig.class);
    }

    public SpanKeyExtractor loadAndGetSpanExtractor(){
        if(spanKeyExtractorConfig == null){
            return null;
        }
        SpanKeyExtractorLoader spanKeyExtractorLoader = SpanKeyExtractorLoader.getInstance(spanKeyExtractorConfig);
        return spanKeyExtractorLoader.getSpanKeyExtractor();
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
