package com.expedia.www.haystack.pipes.firehoseWriter;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_FIREHOSE_CONFIG_PREFIX;

public class FirehoseConfigurationProvider implements FirehoseConfig {
    private FirehoseConfig firehoseConfig;

    FirehoseConfigurationProvider() {
        reload();
    }

    @Override
    public int retrycount() {
        return firehoseConfig.retrycount();
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

    private void reload() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        firehoseConfig = configurationProvider.bind(HAYSTACK_FIREHOSE_CONFIG_PREFIX, FirehoseConfig.class);
    }
}
