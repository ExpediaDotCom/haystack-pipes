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
package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.Factory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.BYTES_IN_COUNTERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.BYTES_IN_COUNTER_NAME;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.REQUESTS_COUNTERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.REQUEST_COUNTER_NAME;
import static com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufSerializer.PROTOBUF_SERIALIZATION_TIMERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufSerializer.PROTOBUF_SERIALIZATION_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpanProtobufSerializerTest {
    private static final String APPLICATION_NAME = SpanProtobufSerializerTest.class.getSimpleName();
    private static final String CLASS_NAME = SpanProtobufSerializer.class.getSimpleName();

    @Mock
    private Factory mockFactory;
    private Factory realFactory;
    @Mock
    private Logger mockLogger;
    private Logger realLogger;
    @Mock
    private Timer mockTimer;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private Counter mockRequestCounter;
    @Mock
    private Counter mockBytesInCounter;

    private SpanProtobufSerializer spanProtobufSerializer;

    @Before
    public void setUp() {
        realLogger = SpanProtobufSerializer.logger;
        SpanProtobufSerializer.logger = mockLogger;
        realFactory = SerializerDeserializerBase.factory;
        SerializerDeserializerBase.factory = mockFactory;
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME)).thenReturn(mockRequestCounter);
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME)).thenReturn(mockBytesInCounter);
        when(mockFactory.createTimer(APPLICATION_NAME, CLASS_NAME, PROTOBUF_SERIALIZATION_TIMER_NAME)).thenReturn(mockTimer);
        spanProtobufSerializer = new SpanProtobufSerializer(APPLICATION_NAME);
    }

    @After
    public void tearDown() {
        SpanProtobufSerializer.logger = realLogger;
        SerializerDeserializerBase.factory = realFactory;

        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME);
        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME);
        verify(mockFactory).createTimer(APPLICATION_NAME, CLASS_NAME, PROTOBUF_SERIALIZATION_TIMER_NAME);

        verifyNoMoreInteractions(mockFactory, mockLogger, mockTimer, mockStopwatch, mockRequestCounter,
                mockBytesInCounter);
        REQUESTS_COUNTERS.clear();
        BYTES_IN_COUNTERS.clear();
        PROTOBUF_SERIALIZATION_TIMERS.clear();
    }

    @Test
    public void testSerializeFullyPopulated() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final byte[] byteArray = spanProtobufSerializer.serialize(null, FULLY_POPULATED_SPAN);

        assertArrayEquals(FULLY_POPULATED_SPAN.toByteArray(), byteArray);
        verify(mockRequestCounter).increment();
        verify(mockBytesInCounter).increment(byteArray.length);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testConfigure() {
        spanProtobufSerializer.configure(null, true);
    }

    @Test
    public void testClose() {
        spanProtobufSerializer.close();
    }

    private void verifyTimerAndStopwatch() {
        verify(mockTimer).start();
        verify(mockStopwatch).stop();
    }
}
