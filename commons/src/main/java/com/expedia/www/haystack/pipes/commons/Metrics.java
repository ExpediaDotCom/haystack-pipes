package com.expedia.www.haystack.pipes.commons;

import com.expedia.www.haystack.metrics.GraphiteConfig;
import com.expedia.www.haystack.metrics.MetricPublishing;
import org.cfg4j.provider.ConfigurationProvider;

public class Metrics {
    private final ConfigurationProvider configurationProvider;
    private final MetricPublishing metricPublishing;

    @SuppressWarnings("WeakerAccess") // give unit tests a choice of providing a mock ConfigurationProvider
    public Metrics(ConfigurationProvider configurationProvider, MetricPublishing metricPublishing) {
        this.configurationProvider = configurationProvider;
        this.metricPublishing = metricPublishing;
    }

    public Metrics() {
        this(new Configuration().createMergeConfigurationProvider(), new MetricPublishing());
    }

    public void startMetricsPolling() {
        final GraphiteConfig graphiteConfig = configurationProvider.bind(
                Configuration.HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class);
        metricPublishing.start(graphiteConfig);
    }
}
