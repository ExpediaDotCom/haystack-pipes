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


import java.util.function.Supplier;

import com.expedia.www.haystack.metrics.MetricObjects;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SimpleSpringConfigTest {
    @Test
    public void testFirehoseCollectorSupplierWithStringsCollector() {
        final FirehoseConfigurationProvider mockConfigration = Mockito.mock(FirehoseConfigurationProvider.class);
        final MetricObjects mockMetricObjects = Mockito.mock(MetricObjects.class);
        final SpringConfig springConfig = new SpringConfig(mockMetricObjects);
        when(mockConfigration.usestringbuffering()).thenReturn(true);

        final Supplier<FirehoseCollector> supplier = springConfig.firehoseCollector(mockConfigration);

        assertEquals(FirehoseByteArrayCollector.class, supplier.get().getClass());
        verify(mockConfigration).usestringbuffering();
    }

    @Test
    public void testFirehoseCollectorSupplierWithRecordCollector() {
        final FirehoseConfigurationProvider mockConfigration = Mockito.mock(FirehoseConfigurationProvider.class);
        final MetricObjects mockMetricObjects = Mockito.mock(MetricObjects.class);
        final SpringConfig springConfig = new SpringConfig(mockMetricObjects);
        when(mockConfigration.usestringbuffering()).thenReturn(false);

        final Supplier<FirehoseCollector> supplier = springConfig.firehoseCollector(mockConfigration);

        assertEquals(FirehoseRecordBufferCollector.class, supplier.get().getClass());
        verify(mockConfigration).usestringbuffering();
    }
}
