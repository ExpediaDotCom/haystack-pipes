package com.expedia.www.haystack.pipes.commons;

import com.expedia.www.haystack.metrics.GraphiteConfig;
import com.expedia.www.haystack.metrics.MetricPublishing;
import org.cfg4j.provider.ConfigurationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_GRAPHITE_CONFIG_PREFIX;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetricsTest {
    @Mock
    private ConfigurationProvider mockConfigurationProvider;

    @Mock
    private MetricPublishing mockMetricPublishing;

    @Mock
    private GraphiteConfig mockGraphiteConfig;

    private Metrics metrics;

    @Before
    public void setUp() {
        metrics = new Metrics(mockConfigurationProvider, mockMetricPublishing);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockConfigurationProvider, mockMetricPublishing, mockGraphiteConfig);
    }

    @Test
    public void testStartMetricsPolling() {
        when(mockConfigurationProvider.bind(HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class))
                .thenReturn(mockGraphiteConfig);

        metrics.startMetricsPolling();

        verify(mockConfigurationProvider).bind(HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class);
        verify(mockMetricPublishing).start(mockGraphiteConfig);
    }

    @Test
    public void testDefaultConstructor() {
        new Metrics();
    }
}
