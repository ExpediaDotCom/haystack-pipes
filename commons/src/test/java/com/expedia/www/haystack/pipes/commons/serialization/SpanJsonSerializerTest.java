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
package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.www.haystack.pipes.commons.TestConstantsAndCommonCode;
import com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.Factory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Assert;
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
import static com.expedia.www.haystack.pipes.commons.serialization.SpanJsonSerializer.JSON_SERIALIZATION_TIMERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SpanJsonSerializer.JSON_SERIALIZATION_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpanJsonSerializerTest {
    private final static String APPLICATION_NAME = SpanJsonSerializerTest.class.getSimpleName();
    private final static String CLASS_NAME = SpanJsonSerializer.class.getSimpleName();

    @Mock
    private Factory mockFactory;
    private Factory realFactory;
    @Mock
    private Printer mockPrinter;
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

    private SpanJsonSerializer spanJsonSerializer;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        realLogger = SpanJsonSerializer.logger;
        SpanJsonSerializer.logger = mockLogger;
        realFactory = SerializerDeserializerBase.factory;
        SerializerDeserializerBase.factory = mockFactory;
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME)).thenReturn(mockRequestCounter);
        when(mockFactory.createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME)).thenReturn(mockBytesInCounter);
        when(mockFactory.createTimer(APPLICATION_NAME, CLASS_NAME, JSON_SERIALIZATION_TIMER_NAME)).thenReturn(mockTimer);
        spanJsonSerializer = new SpanJsonSerializer(APPLICATION_NAME);
    }

    @After
    public void tearDown() {
        SpanJsonSerializer.logger = realLogger;
        SerializerDeserializerBase.factory = realFactory;

        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, REQUEST_COUNTER_NAME);
        verify(mockFactory).createCounter(APPLICATION_NAME, CLASS_NAME, BYTES_IN_COUNTER_NAME);
        verify(mockFactory).createTimer(APPLICATION_NAME, CLASS_NAME, JSON_SERIALIZATION_TIMER_NAME);

        verifyNoMoreInteractions(mockFactory, mockPrinter, mockLogger, mockTimer, mockStopwatch,
                mockRequestCounter, mockBytesInCounter);
        REQUESTS_COUNTERS.clear();
        BYTES_IN_COUNTERS.clear();
        JSON_SERIALIZATION_TIMERS.clear();
    }

    @Test
    public void testSerializeFullyPopulated() throws Descriptors.DescriptorValidationException {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final byte[] byteArray = spanJsonSerializer.serialize(null, FULLY_POPULATED_SPAN);

        final String string = new String(byteArray);
        Assert.assertEquals(TestConstantsAndCommonCode.JSON_SPAN_STRING, string);
        verify(mockRequestCounter).increment();
        verify(mockBytesInCounter).increment(byteArray.length);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testSerializeExceptionCase() throws InvalidProtocolBufferException {
        when(mockTimer.start()).thenReturn(mockStopwatch);
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException("Test");
        when(mockPrinter.print(FULLY_POPULATED_SPAN)).thenThrow(exception);

        final Printer printer = injectMockPrinter();
        final byte[] shouldBeNull = spanJsonSerializer.serialize(null, FULLY_POPULATED_SPAN);
        SpanJsonSerializer.printer = printer;

        assertNull(shouldBeNull);
        verify(mockPrinter).print(FULLY_POPULATED_SPAN);
        verify(mockLogger).error(SpanJsonSerializer.ERROR_MSG, FULLY_POPULATED_SPAN, exception);
        verify(mockRequestCounter).increment();
        verifyTimerAndStopwatch();
    }

    @Test
    public void testConfigure() throws InvalidProtocolBufferException {
        final Printer printer = injectMockPrinter();
        spanJsonSerializer.configure(null, true);
        SpanJsonSerializer.printer = printer;
    }

    @Test
    public void testClose() throws InvalidProtocolBufferException {
        final Printer printer = injectMockPrinter();
        spanJsonSerializer.close();
        SpanJsonSerializer.printer = printer;
    }

    private Printer injectMockPrinter() {
        final Printer printer = SpanJsonSerializer.printer;
        SpanJsonSerializer.printer = mockPrinter;
        return printer;
    }

    private void verifyTimerAndStopwatch() {
        verify(mockTimer).start();
        verify(mockStopwatch).stop();
    }
}
