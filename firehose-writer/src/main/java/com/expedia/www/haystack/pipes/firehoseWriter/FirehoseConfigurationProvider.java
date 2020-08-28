/*
 * Copyright 2018 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.expedia.www.haystack.commons.config.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_FIREHOSE_CONFIG_PREFIX;

public class FirehoseConfigurationProvider implements FirehoseConfig {
    private FirehoseConfig firehoseConfig;

    FirehoseConfigurationProvider() {
        reload();
    }

    @Override
    public String url() {
        return firehoseConfig.url();
    }

    @Override
    public String streamname() {
        return firehoseConfig.streamname();
    }

    @Override
    public String signingregion() {
        return firehoseConfig.signingregion();
    }

    @Override
    public int initialretrysleep() {
        return firehoseConfig.initialretrysleep();
    }

    @Override
    public int maxretrysleep() {
        return firehoseConfig.maxretrysleep();
    }

    @Override
    public boolean usestringbuffering() {
        return firehoseConfig.usestringbuffering();
    }

    @Override
    public int maxbatchinterval() {
        return firehoseConfig.maxbatchinterval();
    }

    @Override
    public int maxparallelismpershard() {
        return firehoseConfig.maxparallelismpershard();
    }

    private void reload() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        firehoseConfig = configurationProvider.bind(HAYSTACK_FIREHOSE_CONFIG_PREFIX, FirehoseConfig.class);
    }
}
