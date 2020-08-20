///*
// * Copyright 2018 Expedia, Inc.
// *
// *       Licensed under the Apache License, Version 2.0 (the "License");
// *       you may not use this file except in compliance with the License.
// *       You may obtain a copy of the License at
// *
// *           http://www.apache.org/licenses/LICENSE-2.0
// *
// *       Unless required by applicable law or agreed to in writing, software
// *       distributed under the License is distributed on an "AS IS" BASIS,
// *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *       See the License for the specific language governing permissions and
// *       limitations under the License.
// *
// */
//package com.expedia.www.haystack.pipes.commons;
//
//import com.expedia.www.haystack.metrics.GraphiteConfig;
//import com.expedia.www.haystack.metrics.MetricPublishing;
//import org.cfg4j.provider.ConfigurationProvider;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_GRAPHITE_CONFIG_PREFIX;
//import static org.mockito.Mockito.*;
//
//@RunWith(MockitoJUnitRunner.class)
//public class MetricsTest {
//    @Mock
//    private ConfigurationProvider mockConfigurationProvider;
//
//    @Mock
//    private MetricPublishing mockMetricPublishing;
//
//    @Mock
//    private GraphiteConfig mockGraphiteConfig;
//
//    private Metrics metrics;
//
//    @Before
//    public void setUp() {
//        metrics = new Metrics(mockConfigurationProvider, mockMetricPublishing);
//    }
//
//    @After
//    public void tearDown() {
//        verifyNoMoreInteractions(mockConfigurationProvider, mockMetricPublishing, mockGraphiteConfig);
//    }
//
//    @Test
//    public void testStartMetricsPolling() {
//        when(mockConfigurationProvider.bind(HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class))
//                .thenReturn(mockGraphiteConfig);
//
//        metrics.startMetricsPolling();
//
//        verify(mockConfigurationProvider).bind(HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class);
//        verify(mockMetricPublishing).start(mockGraphiteConfig);
//    }
//
//    @Test
//    public void testDefaultConstructor() {
//        new Metrics();
//    }
//}
