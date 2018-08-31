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

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
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
import static com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufDeserializer.PROTOBUF_SERIALIZATION_TIMERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufDeserializer.PROTOBUF_SERIALIZATION_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.PROTOBUF_SPAN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpanProtobufDeserializerTest {
    private static final String APPLICATION_NAME = SpanProtobufDeserializerTest.class.getSimpleName();
    private static final String CLASS_NAME = SpanProtobufDeserializer.class.getSimpleName();

    @Mock
    private SerializerDeserializerBase.Factory mockFactory;
    private SerializerDeserializerBase.Factory realFactory;
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

    private SpanProtobufDeserializer spanProtobufDeserializer;

    @Before
    public void setUp() {
        realLogger = SpanProtobufDeserializer.logger;
        SpanProtobufDeserializer.logger = mockLogger;
        realFactory = SerializerDeserializerBase.factory;
        SerializerDeserializerBase.factory = mockFactory;
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME)).thenReturn(mockRequestCounter);
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME)).thenReturn(mockBytesInCounter);
        when(mockFactory.createTimer(APPLICATION_NAME, CLASS_NAME, PROTOBUF_SERIALIZATION_TIMER_NAME)).thenReturn(mockTimer);
        spanProtobufDeserializer = new SpanProtobufDeserializer(APPLICATION_NAME);
    }

    @After
    public void tearDown() {
        SpanProtobufDeserializer.logger = realLogger;
        SerializerDeserializerBase.factory = realFactory;
        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME);
        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME);
        verify(mockFactory).createTimer(APPLICATION_NAME, CLASS_NAME, PROTOBUF_SERIALIZATION_TIMER_NAME);
        verifyNoMoreInteractions(mockLogger, mockTimer, mockStopwatch);
        REQUESTS_COUNTERS.clear();
        BYTES_IN_COUNTERS.clear();
        PROTOBUF_SERIALIZATION_TIMERS.clear();
    }

    @Test
    public void testDeserializeFullyPopulated() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Span actual = spanProtobufDeserializer.deserialize(null, PROTOBUF_SPAN_BYTES);

        assertEquals(FULLY_POPULATED_SPAN, actual);
        verify(mockBytesInCounter).increment(PROTOBUF_SPAN_BYTES.length);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testDeserializeNull() {
        final Span shouldBeNull = spanProtobufDeserializer.deserialize(null, null);

        assertNull(shouldBeNull);
    }

    @Test
    public void testDeserializeExceptionCase() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Span shouldBeNull = spanProtobufDeserializer.deserialize(null, new byte[]{0x00});

        assertNull(shouldBeNull);
        verify(mockLogger).error(eq(SpanProtobufDeserializer.ERROR_MSG), eq("00"), any(InvalidProtocolBufferException.class));
        verifiesForCountersTimerAndStopwatch();
        clearCounters();
    }

    @Test(expected = OutOfMemoryError.class)
    public void testDeserializeErrorCase() {
        when(mockTimer.start()).thenReturn(mockStopwatch);
        final OutOfMemoryError outOfMemoryError = new OutOfMemoryError();
        doThrow(outOfMemoryError).when(mockBytesInCounter).increment(anyLong());

        try {
            spanProtobufDeserializer.deserialize(null, new byte[]{0x00});
        } finally {
            verifiesForCountersTimerAndStopwatch();
            clearCounters();
        }
    }

    private void verifiesForCountersTimerAndStopwatch() {
        verify(mockRequestCounter).increment();
        verify(mockBytesInCounter).increment(1);
        verifyTimerAndStopwatch();
    }

    private void clearCounters() {
        REQUESTS_COUNTERS.clear();
        BYTES_IN_COUNTERS.clear();
        SpanProtobufDeserializer.PROTOBUF_SERIALIZATION_TIMERS.clear();
    }

    @Test
    public void testConfigure() {
        spanProtobufDeserializer.configure(null, true);
    }

    @Test
    public void testClose() {
        spanProtobufDeserializer.close();
    }

    private void verifyTimerAndStopwatch() {
        verify(mockTimer).start();
        verify(mockStopwatch).stop();
    }
}
