/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.commons;

import com.expedia.www.haystack.metrics.GraphiteConfig;
import com.expedia.www.haystack.metrics.MetricPublishing;
import org.cfg4j.provider.ConfigurationProvider;

// TODO is this class still needed? Do non-appender usages of Counters and Timers still publish to InfluxDb?
// TODO Do these non-appender usages shut down properly?
class Metrics {
    private final ConfigurationProvider configurationProvider;
    private final MetricPublishing metricPublishing;

    Metrics(ConfigurationProvider configurationProvider, MetricPublishing metricPublishing) {
        this.configurationProvider = configurationProvider;
        this.metricPublishing = metricPublishing;
    }

    Metrics() {
        this(new Configuration().createMergeConfigurationProvider(), new MetricPublishing());
    }

    void startMetricsPolling() {
        final GraphiteConfig graphiteConfig = configurationProvider.bind(
                Configuration.HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class);
        metricPublishing.start(graphiteConfig);
    }
}
