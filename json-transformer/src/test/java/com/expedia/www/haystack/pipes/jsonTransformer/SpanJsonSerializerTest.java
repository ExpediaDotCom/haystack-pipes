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
package com.expedia.www.haystack.pipes.jsonTransformer;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.jsonTransformer.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpanJsonSerializerTest {

    @Mock
    private Printer mockPrinter;
    @Mock
    private Logger mockLogger;
    private Logger realLogger;
    @Mock
    private Timer mockTimer;
    private Timer realTimer;
    @Mock
    private Stopwatch mockStopwatch;

    private SpanJsonSerializer spanJsonSerializer;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        spanJsonSerializer = new SpanJsonSerializer();
        realLogger = SpanJsonSerializer.logger;
        SpanJsonSerializer.logger = mockLogger;
        realTimer = SpanJsonSerializer.JSON_SERIALIZATION;
        SpanJsonSerializer.JSON_SERIALIZATION = mockTimer;
    }

    @After
    public void tearDown() {
        SpanJsonSerializer.logger = realLogger;
        SpanJsonSerializer.JSON_SERIALIZATION = realTimer;
        SpanJsonSerializer.ERROR.increment(-((long) SpanJsonSerializer.ERROR.getValue()));
        SpanJsonSerializer.REQUEST.increment(-((long) SpanJsonSerializer.REQUEST.getValue()));
        SpanJsonSerializer.BYTES_IN.increment(-((long) SpanJsonSerializer.BYTES_IN.getValue()));
        verifyNoMoreInteractions(mockPrinter, mockLogger, mockTimer, mockStopwatch);
    }

    @Test
    public void testSerializeFullyPopulated() throws Descriptors.DescriptorValidationException {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final byte[] byteArray = spanJsonSerializer.serialize(null, FULLY_POPULATED_SPAN);

        final String string = new String(byteArray);
        assertEquals(TestConstantsAndCommonCode.JSON_SPAN_STRING, string);
        verifyCounters(0L, 1, byteArray.length);
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
        verifyCounters(1, 1, 0);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testConfigure() throws InvalidProtocolBufferException {
        final Printer printer = injectMockPrinter();
        spanJsonSerializer.configure(null, true);
        SpanJsonSerializer.printer = printer;
        verifyCounters(0, 0, 0);
    }

    @Test
    public void testClose() throws InvalidProtocolBufferException {
        final Printer printer = injectMockPrinter();
        spanJsonSerializer.close();
        SpanJsonSerializer.printer = printer;
        verifyCounters(0, 0, 0);
    }

    private Printer injectMockPrinter() {
        final Printer printer = SpanJsonSerializer.printer;
        SpanJsonSerializer.printer = mockPrinter;
        return printer;
    }

    private void verifyCounters(long errorCount, long requestCount, long bytesIn) {
        assertEquals(errorCount, SpanJsonSerializer.ERROR.getValue());
        assertEquals(requestCount, SpanJsonSerializer.REQUEST.getValue());
        assertEquals(bytesIn, SpanJsonSerializer.BYTES_IN.getValue());
    }

    private void verifyTimerAndStopwatch() {
        verify(mockTimer).start();
        verify(mockStopwatch).stop();
    }
}
