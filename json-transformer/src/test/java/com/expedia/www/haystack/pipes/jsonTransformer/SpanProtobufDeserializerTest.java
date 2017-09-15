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

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.jsonTransformer.SpanProtobufDeserializer.ERROR_MSG;
import static com.expedia.www.haystack.pipes.jsonTransformer.TestConstantsAndCommonCode.PROTOBUF_SPAN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpanProtobufDeserializerTest {
    @Mock
    private Logger mockLogger;
    private Logger realLogger;
    @Mock
    private Timer mockTimer;
    private Timer realTimer;
    @Mock
    private Stopwatch mockStopwatch;

    private SpanProtobufDeserializer spanProtobufDeserializer;

    @Before
    public void setUp() {
        spanProtobufDeserializer = new SpanProtobufDeserializer();
        realLogger = SpanProtobufDeserializer.logger;
        SpanProtobufDeserializer.logger = mockLogger;
        realTimer = SpanProtobufDeserializer.PROTOBUF_DESERIALIZATION;
        SpanProtobufDeserializer.PROTOBUF_DESERIALIZATION = mockTimer;
    }

    @After
    public void tearDown() {
        SpanProtobufDeserializer.logger = realLogger;
        SpanProtobufDeserializer.PROTOBUF_DESERIALIZATION = realTimer;
        SpanProtobufDeserializer.ERROR.increment(-((long) SpanProtobufDeserializer.ERROR.getValue()));
        SpanProtobufDeserializer.REQUEST.increment(-((long) SpanProtobufDeserializer.REQUEST.getValue()));
        SpanProtobufDeserializer.BYTES_IN.increment(-((long) SpanProtobufDeserializer.BYTES_IN.getValue()));
        verifyNoMoreInteractions(mockLogger, mockTimer, mockStopwatch);
    }

    @Test
    public void testDeserializeFullyPopulated() throws InvalidProtocolBufferException {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Span actual = spanProtobufDeserializer.deserialize(null, PROTOBUF_SPAN_BYTES);

        final Span expected = TestConstantsAndCommonCode.createFullyPopulatedSpan();
        assertEquals(expected, actual);
        verifyCounters(0L, PROTOBUF_SPAN_BYTES.length);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testDeserializeNull() {
        final Span shouldBeNull = spanProtobufDeserializer.deserialize(null, null);

        assertNull(shouldBeNull);
        verifyCounters(0L, 0L);
    }

    @Test
    public void testDeserializeExceptionCase() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Span shouldBeNull = spanProtobufDeserializer.deserialize(null, new byte[]{ 0x00 });

        assertNull(shouldBeNull);
        verify(mockLogger).error(eq(ERROR_MSG), eq("00"), any(InvalidProtocolBufferException.class));
        verifyCounters(1, 1);
        verifyTimerAndStopwatch();
    }

    @Test
    public void testConfigure() throws InvalidProtocolBufferException {
        spanProtobufDeserializer.configure(null, true);
    }

    @Test
    public void testClose() throws InvalidProtocolBufferException {
        spanProtobufDeserializer.close();
    }


    private void verifyCounters(long errorCount, long bytesIn) {
        assertEquals(errorCount, SpanProtobufDeserializer.ERROR.getValue());
        assertEquals((long) 1, SpanProtobufDeserializer.REQUEST.getValue());
        assertEquals(bytesIn, SpanProtobufDeserializer.BYTES_IN.getValue());
    }

    private void verifyTimerAndStopwatch() {
        verify(mockTimer).start();
        verify(mockStopwatch).stop();
    }
}
