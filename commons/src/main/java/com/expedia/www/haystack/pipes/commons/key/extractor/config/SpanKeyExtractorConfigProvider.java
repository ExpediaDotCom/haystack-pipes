/*
 * Copyright 2020 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.commons.key.extractor.config;

import com.expedia.www.haystack.commons.config.Configuration;
import com.expedia.www.haystack.pipes.commons.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.key.extractor.loader.SpanKeyExtractorLoader;
import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_EXTRACTOR_CONFIG_PREFIX;

public class SpanKeyExtractorConfigProvider implements SpanKeyExtractorConfig {

    private final SpanKeyExtractorConfig spanKeyExtractorConfig;

    public SpanKeyExtractorConfigProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        spanKeyExtractorConfig = configurationProvider.bind(HAYSTACK_EXTRACTOR_CONFIG_PREFIX, SpanKeyExtractorConfig.class);
    }

    public SpanKeyExtractor loadAndGetSpanExtractor() {
        if (spanKeyExtractorConfig == null) {
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
    public String config() {
        return spanKeyExtractorConfig.config();
    }

}
